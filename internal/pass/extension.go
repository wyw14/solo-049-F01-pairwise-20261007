package pass

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

// Extension items are checked and applied in this fixed order, so a conflict
// report always shows exactly which resources were and were not moved.
const (
	ExtensionItemReservation      = "reservation"
	ExtensionItemWindow           = "window"
	ExtensionItemDeadline         = "deadline"
	ExtensionItemPointingQueue    = "pointing_queue"
	ExtensionItemReceiveChainPlan = "receive_chain_plan"
	ExtensionItemRecording        = "recording"
)

type ExtensionItemStatus string

const (
	ExtensionApplied    ExtensionItemStatus = "applied"
	ExtensionSkipped    ExtensionItemStatus = "skipped"
	ExtensionFailed     ExtensionItemStatus = "failed"
	ExtensionNotApplied ExtensionItemStatus = "not_applied"
)

type ExtensionItem struct {
	Name   string              `json:"name"`
	Status ExtensionItemStatus `json:"status"`
	Detail string              `json:"detail,omitempty"`
}

type ExtensionResult struct {
	PassID          string          `json:"pass_id"`
	DurationSeconds float64         `json:"duration_seconds"`
	NewLOS          time.Time       `json:"new_los"`
	Applied         bool            `json:"applied"`
	Replay          bool            `json:"replay,omitempty"`
	Items           []ExtensionItem `json:"items"`
	Pass            Pass            `json:"pass"`
}

// ExtensionConflictError reports an extension that left every resource
// untouched; Result carries the per-item outcome.
type ExtensionConflictError struct {
	PassID string
	Item   string
	Err    error
	Result ExtensionResult
}

func (e *ExtensionConflictError) Error() string {
	return fmt.Sprintf("extension of pass %s not applied at %s: %v", e.PassID, e.Item, e.Err)
}

func (e *ExtensionConflictError) Unwrap() error { return e.Err }

// extensionRecord makes an applied extension replayable: a repeated request
// returns the recorded result instead of extending the window again.
type extensionRecord struct {
	Key       string          `json:"key"`
	PassID    string          `json:"pass_id"`
	Duration  time.Duration   `json:"duration"`
	NewLOS    time.Time       `json:"new_los"`
	Result    ExtensionResult `json:"result"`
	AppliedAt time.Time       `json:"applied_at"`
}

type ExtensionDeps struct {
	Passes    *Service
	Timeline  *scheduler.Timeline
	Fleet     *antenna.Fleet
	Deadlines *DeadlineManager
	Queue     *scheduler.CommandQueue
	RF        *rf.Controller
	Recorder  *recorder.Manager
	Windows   *WindowRecomputer
	Audit     *audit.Trail
}

type ExtensionService struct {
	mu        sync.Mutex
	path      string
	records   map[string]extensionRecord
	passes    *Service
	timeline  *scheduler.Timeline
	fleet     *antenna.Fleet
	deadlines *DeadlineManager
	queue     *scheduler.CommandQueue
	rf        *rf.Controller
	recorder  *recorder.Manager
	windows   *WindowRecomputer
	audit     *audit.Trail
}

func NewExtensionService(paths store.Paths, deps ExtensionDeps) (*ExtensionService, error) {
	service := &ExtensionService{
		path: paths.Snapshot("pass-extensions"), records: make(map[string]extensionRecord),
		passes: deps.Passes, timeline: deps.Timeline, fleet: deps.Fleet, deadlines: deps.Deadlines,
		queue: deps.Queue, rf: deps.RF, recorder: deps.Recorder, windows: deps.Windows, audit: deps.Audit,
	}
	var records []extensionRecord
	found, err := store.ReadJSON(service.path, &records)
	if err != nil {
		return nil, err
	}
	if found {
		for _, record := range records {
			service.records[record.Key] = record
		}
	}
	return service, nil
}

// Extend grows a pass window and synchronizes every resource that depends on
// it: reservation, window, deadline, pointing queue, receive chain plan and
// recording expectation. All resources are preflighted first; when any of
// them conflicts, nothing is applied and the result lists each item. Repeating
// a request (same idempotency key, or the same keyless request while the
// window is unchanged) replays the recorded result instead of extending twice.
func (s *ExtensionService) Extend(passID string, duration time.Duration, idempotencyKey string) (ExtensionResult, error) {
	if duration <= 0 || duration > 15*time.Minute {
		return ExtensionResult{}, fmt.Errorf("extension must be between zero and fifteen minutes")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if idempotencyKey != "" {
		if record, found := s.records[explicitExtensionKey(passID, idempotencyKey)]; found {
			if record.Duration != duration {
				return ExtensionResult{}, fmt.Errorf("idempotency key %q already used for a %s extension of pass %s", idempotencyKey, record.Duration, passID)
			}
			return s.replayLocked(record), nil
		}
	}
	value, found := s.passes.Get(passID)
	if !found {
		return ExtensionResult{}, fmt.Errorf("pass %s not found", passID)
	}
	if idempotencyKey == "" {
		if record, found := s.records[implicitExtensionKey(passID)]; found && record.Duration == duration && record.NewLOS.Equal(value.Window.LOS) {
			return s.replayLocked(record), nil
		}
	}
	if value.Phase != Tracking && value.Phase != Acquiring {
		return ExtensionResult{}, fmt.Errorf("pass %s cannot extend from %s", passID, value.Phase)
	}
	items, plan, firstErr := s.preflight(value, duration)
	if firstErr != nil {
		failedItem := ""
		for _, item := range items {
			if item.Status == ExtensionFailed {
				failedItem = item.Name
				break
			}
		}
		result := ExtensionResult{PassID: passID, DurationSeconds: duration.Seconds(), NewLOS: plan.newLOS, Items: items, Pass: value}
		_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.rejected", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"item": failedItem, "error": firstErr.Error()}})
		return result, &ExtensionConflictError{PassID: passID, Item: failedItem, Err: firstErr, Result: result}
	}
	result, err := s.apply(value, duration, items, plan)
	if err != nil {
		_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.aborted", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"error": err.Error()}})
		return result, err
	}
	record := extensionRecord{PassID: passID, Duration: duration, NewLOS: result.NewLOS, Result: result, AppliedAt: time.Now().UTC()}
	if idempotencyKey != "" {
		record.Key = explicitExtensionKey(passID, idempotencyKey)
	} else {
		record.Key = implicitExtensionKey(passID)
	}
	if err := s.putRecordLocked(record); err != nil {
		_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.record.failed", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"error": err.Error()}})
	}
	fields := map[string]any{"duration_seconds": duration.Seconds(), "new_los": result.NewLOS}
	if idempotencyKey != "" {
		fields["idempotency_key"] = idempotencyKey
	}
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extended", Subject: passID, Fields: fields})
	return result, nil
}

type extensionPreflight struct {
	newLOS     time.Time
	commands   []scheduler.PointingCommand
	hasSegment bool
}

// preflight validates every resource the extension would touch and returns one
// item per resource. On any failure the remaining items are marked not_applied
// so the caller can report the full picture without having mutated anything.
func (s *ExtensionService) preflight(value Pass, duration time.Duration) ([]ExtensionItem, extensionPreflight, error) {
	plan := extensionPreflight{newLOS: value.Window.LOS.Add(duration)}
	stamp := plan.newLOS.UTC().Format(time.RFC3339)
	items := make([]ExtensionItem, 0, 6)
	var firstErr error
	fail := func(name string, err error) {
		items = append(items, ExtensionItem{Name: name, Status: ExtensionFailed, Detail: err.Error()})
		if firstErr == nil {
			firstErr = err
		}
	}

	interval, err := s.timeline.CheckExtend(value.ReservationID, plan.newLOS)
	switch {
	case err != nil:
		fail(ExtensionItemReservation, err)
	default:
		owner, owned := s.fleet.State(value.AntennaID)
		if !owned || owner.OwnerPassID != value.ID || owner.OwnerGeneration != interval.Generation {
			fail(ExtensionItemReservation, fmt.Errorf("antenna %s is not owned by pass %s at generation %d", value.AntennaID, value.ID, interval.Generation))
		} else {
			items = append(items, ExtensionItem{Name: ExtensionItemReservation, Status: ExtensionApplied, Detail: fmt.Sprintf("reservation %s end moves to %s", interval.ReservationID, stamp)})
		}
	}

	window := value.Window
	window.LOS = plan.newLOS
	if err := window.Validate(); err != nil {
		fail(ExtensionItemWindow, err)
	} else {
		items = append(items, ExtensionItem{Name: ExtensionItemWindow, Status: ExtensionApplied, Detail: fmt.Sprintf("pass window LOS moves to %s", stamp)})
	}

	if _, found := s.deadlines.Get(value.ID); !found {
		fail(ExtensionItemDeadline, fmt.Errorf("deadline for pass %s not found", value.ID))
	} else {
		items = append(items, ExtensionItem{Name: ExtensionItemDeadline, Status: ExtensionApplied, Detail: fmt.Sprintf("civil and monotonic deadlines move to %s", stamp)})
	}

	points := extensionTrackPoints(s.queue.List(value.ID), value.Window.LOS, plan.newLOS)
	commands, err := antenna.BuildCommands(value.ID, value.AntennaID, value.Window.Revision, points)
	if err != nil {
		fail(ExtensionItemPointingQueue, err)
	} else {
		plan.commands = commands
		items = append(items, ExtensionItem{Name: ExtensionItemPointingQueue, Status: ExtensionApplied, Detail: fmt.Sprintf("%d pointing commands cover the window through %s", len(commands), stamp)})
	}

	if err := s.rf.CheckChains(value.ID, value.RequiredReceiverChains); err != nil {
		fail(ExtensionItemReceiveChainPlan, err)
	} else {
		items = append(items, ExtensionItem{Name: ExtensionItemReceiveChainPlan, Status: ExtensionApplied, Detail: fmt.Sprintf("receive chains %s stay planned for the pass", strings.Join(value.RequiredReceiverChains, ","))})
	}

	if segment, found := s.recorder.ActiveForPass(value.ID); found {
		plan.hasSegment = true
		items = append(items, ExtensionItem{Name: ExtensionItemRecording, Status: ExtensionApplied, Detail: fmt.Sprintf("segment %s expects frames until %s", segment.ID, stamp)})
	} else {
		items = append(items, ExtensionItem{Name: ExtensionItemRecording, Status: ExtensionSkipped, Detail: "no active recording segment"})
	}

	if firstErr != nil {
		for index := range items {
			if items[index].Status != ExtensionFailed {
				items[index].Status = ExtensionNotApplied
				items[index].Detail = "not applied: extension aborted"
			}
		}
	}
	return items, plan, firstErr
}

// apply publishes the preflighted extension item by item. A step that still
// fails aborts the remaining items and reports what was and was not applied.
func (s *ExtensionService) apply(value Pass, duration time.Duration, items []ExtensionItem, plan extensionPreflight) (ExtensionResult, error) {
	abort := func(index int, err error) (ExtensionResult, error) {
		items[index].Status = ExtensionFailed
		items[index].Detail = err.Error()
		for rest := index + 1; rest < len(items); rest++ {
			items[rest].Status = ExtensionNotApplied
			items[rest].Detail = "not applied: extension aborted"
		}
		result := ExtensionResult{PassID: value.ID, DurationSeconds: duration.Seconds(), NewLOS: plan.newLOS, Items: items, Pass: value}
		return result, &ExtensionConflictError{PassID: value.ID, Item: items[index].Name, Err: err, Result: result}
	}
	if _, err := s.timeline.Extend(value.ReservationID, plan.newLOS); err != nil {
		return abort(0, err)
	}
	if err := s.passes.update(value.ID, func(current *Pass) error {
		current.Window.LOS = current.Window.LOS.Add(duration)
		return nil
	}); err != nil {
		return abort(1, err)
	}
	updated, found := s.passes.Get(value.ID)
	if !found {
		return abort(1, fmt.Errorf("pass %s not found", value.ID))
	}
	if err := s.windows.Apply(value.ID, updated.Window); err != nil {
		return abort(1, err)
	}
	if _, err := s.deadlines.Extend(value.ID, updated.Window.LOS, duration); err != nil {
		return abort(2, err)
	}
	if err := s.queue.Append(value.ID, value.Window.Revision, plan.commands); err != nil {
		return abort(3, err)
	}
	if err := s.rf.Renew(value.ID, value.RequiredReceiverChains); err != nil {
		return abort(4, err)
	}
	if plan.hasSegment {
		if _, _, err := s.recorder.ExtendExpectation(value.ID, updated.Window.LOS); err != nil {
			return abort(5, err)
		}
	}
	return ExtensionResult{PassID: value.ID, DurationSeconds: duration.Seconds(), NewLOS: updated.Window.LOS, Applied: true, Items: items, Pass: updated}, nil
}

func (s *ExtensionService) replayLocked(record extensionRecord) ExtensionResult {
	result := record.Result
	result.Replay = true
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.replayed", Subject: record.PassID, Fields: map[string]any{"key": record.Key, "duration_seconds": record.Duration.Seconds()}})
	return result
}

func (s *ExtensionService) putRecordLocked(record extensionRecord) error {
	s.records[record.Key] = record
	values := make([]extensionRecord, 0, len(s.records))
	for _, current := range s.records {
		values = append(values, current)
	}
	return store.WriteJSON(s.path, values)
}

func explicitExtensionKey(passID, key string) string { return "key:" + passID + ":" + key }
func implicitExtensionKey(passID string) string      { return "auto:" + passID }

// extensionTrackPoints continues the pass's current track from the old LOS to
// the new one, so appended commands slew smoothly instead of jumping.
func extensionTrackPoints(existing []scheduler.PointingCommand, oldLOS, newLOS time.Time) []antenna.TrackPoint {
	azimuth, elevation := 250.0, 7.0
	var latest time.Time
	for _, command := range existing {
		if command.Retired || !command.ExecuteAt.After(latest) {
			continue
		}
		latest = command.ExecuteAt
		azimuth = command.Azimuth
		elevation = command.Elevation
	}
	halfway := oldLOS.Add(newLOS.Sub(oldLOS) / 2)
	return []antenna.TrackPoint{
		{At: halfway, Azimuth: math.Mod(azimuth+2.5, 360), Elevation: clampElevation(elevation - 1.5)},
		{At: newLOS, Azimuth: math.Mod(azimuth+5, 360), Elevation: clampElevation(elevation - 3)},
	}
}

func clampElevation(value float64) float64 {
	if value < 1 {
		return 1
	}
	if value > 90 {
		return 90
	}
	return value
}
