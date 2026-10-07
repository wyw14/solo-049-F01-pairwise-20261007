package pass

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

// ErrExtensionConflict reports that one or more station resources rejected a
// pass extension. The ExtensionResult returned alongside carries the
// per-resource outcome; nothing was applied.
var ErrExtensionConflict = errors.New("pass extension conflicts with station resources")

const (
	ExtensionItemReservation  = "reservation"
	ExtensionItemWindow       = "window"
	ExtensionItemDeadline     = "deadline"
	ExtensionItemPointing     = "pointing_queue"
	ExtensionItemReceiverPlan = "receiver_plan"
	ExtensionItemRecording    = "recording"
)

const (
	ExtensionApplied  = "applied"
	ExtensionSkipped  = "skipped"
	ExtensionConflict = "conflict"
)

// extensionReady is the internal check outcome before an item is committed.
const extensionReady = "ready"

type ExtensionItem struct {
	Item   string `json:"item"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type ExtensionResult struct {
	PassID    string          `json:"pass_id"`
	RequestID string          `json:"request_id"`
	NewLOS    time.Time       `json:"new_los"`
	Applied   bool            `json:"applied"`
	Duplicate bool            `json:"duplicate"`
	Items     []ExtensionItem `json:"items"`
	Pass      Pass            `json:"pass"`
}

// extensionRecord is the idempotency ledger entry for one extension request.
// A record is persisted before anything is applied, so a crash mid-apply is
// replayed against the same target LOS instead of extending twice.
type extensionRecord struct {
	RequestID string          `json:"request_id"`
	Duration  time.Duration   `json:"duration"`
	TargetLOS time.Time       `json:"target_los"`
	Applied   bool            `json:"applied"`
	Result    ExtensionResult `json:"result"`
}

type ExtensionService struct {
	passes    *Service
	timeline  *scheduler.Timeline
	fleet     *antenna.Fleet
	deadlines *DeadlineManager
	commands  *scheduler.CommandQueue
	rf        *rf.Controller
	recorder  *recorder.Manager
	audit     *audit.Trail

	mu     sync.Mutex
	path   string
	ledger map[string]map[string]extensionRecord
}

func NewExtensionService(paths store.Paths, passes *Service, timeline *scheduler.Timeline, fleet *antenna.Fleet, deadlines *DeadlineManager, commands *scheduler.CommandQueue, rfCtrl *rf.Controller, manager *recorder.Manager, trail *audit.Trail) (*ExtensionService, error) {
	service := &ExtensionService{
		passes: passes, timeline: timeline, fleet: fleet, deadlines: deadlines,
		commands: commands, rf: rfCtrl, recorder: manager, audit: trail,
		path:   paths.Snapshot("pass-extensions"),
		ledger: make(map[string]map[string]extensionRecord),
	}
	if _, err := store.ReadJSON(service.path, &service.ledger); err != nil {
		return nil, err
	}
	return service, nil
}

// Extend moves the pass LOS out by duration and synchronizes every dependent
// resource: reservation, pass window, deadline, pointing queue, receiver
// chain plans and the recording expectation. It first checks all resources;
// if any of them conflicts, nothing is applied and the result enumerates each
// resource. Replaying the same requestID returns the recorded outcome without
// extending again.
func (s *ExtensionService) Extend(passID, requestID string, duration time.Duration) (ExtensionResult, error) {
	if duration <= 0 || duration > 15*time.Minute {
		return ExtensionResult{}, fmt.Errorf("extension must be between zero and fifteen minutes")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID == "" {
		requestID = uuid.NewString()
	}
	if record, found := s.recordLocked(passID, requestID); found {
		if record.Duration != duration {
			return ExtensionResult{}, fmt.Errorf("request %s was already used for a %s extension", requestID, record.Duration)
		}
		if record.Applied {
			result := record.Result
			result.Duplicate = true
			_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.duplicate", Subject: passID, Fields: map[string]any{"request_id": requestID}})
			return result, nil
		}
	}

	value, found := s.passes.Get(passID)
	if !found {
		return ExtensionResult{}, fmt.Errorf("pass %s not found", passID)
	}
	if value.Phase != Tracking && value.Phase != Acquiring {
		return ExtensionResult{}, fmt.Errorf("pass %s cannot extend from %s", passID, value.Phase)
	}

	targetLOS := value.Window.LOS.Add(duration)
	if record, found := s.recordLocked(passID, requestID); found {
		// A pending record means a previous attempt crashed mid-apply: replay
		// against the original target instead of extending from the moved LOS.
		targetLOS = record.TargetLOS
	}
	result := ExtensionResult{PassID: passID, RequestID: requestID, NewLOS: targetLOS, Pass: value}

	checks, plan := s.checkExtension(value, targetLOS)
	conflicted := false
	for _, check := range checks {
		if check.status == ExtensionConflict {
			conflicted = true
			break
		}
	}
	if conflicted {
		for index, check := range checks {
			if check.status == extensionReady {
				checks[index].status = ExtensionSkipped
				checks[index].detail = "not applied: another resource conflicts"
			}
		}
		result.Items = extensionItems(checks)
		_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extension.rejected", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"request_id": requestID, "duration_seconds": duration.Seconds()}})
		return result, ErrExtensionConflict
	}

	s.rememberLocked(passID, extensionRecord{RequestID: requestID, Duration: duration, TargetLOS: targetLOS})
	if err := s.flushLocked(); err != nil {
		return ExtensionResult{}, err
	}

	items, current, err := s.applyExtension(value, targetLOS, plan, checks)
	result.Items = items
	result.Pass = current
	if err != nil {
		return result, err
	}
	result.Applied = true

	s.rememberLocked(passID, extensionRecord{RequestID: requestID, Duration: duration, TargetLOS: targetLOS, Applied: true, Result: result})
	if err := s.flushLocked(); err != nil {
		return result, err
	}
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "extended", Subject: passID, Fields: map[string]any{"request_id": requestID, "duration_seconds": duration.Seconds(), "new_los": targetLOS}})
	return result, nil
}

type extensionCheck struct {
	item   string
	status string
	detail string
}

// extensionPlan carries the values prepared during checks into the apply
// phase so both phases agree on what gets written.
type extensionPlan struct {
	pointing []scheduler.PointingCommand
	record   bool
}

func (s *ExtensionService) checkExtension(value Pass, targetLOS time.Time) ([]extensionCheck, extensionPlan) {
	plan := extensionPlan{}
	checks := make([]extensionCheck, 0, 6)

	reservation := extensionCheck{item: ExtensionItemReservation, status: extensionReady}
	if value.ReservationID == "" {
		reservation.status, reservation.detail = ExtensionConflict, "pass has no reservation"
	} else if interval, err := s.timeline.CheckExtend(value.ReservationID, targetLOS); err != nil {
		reservation.status, reservation.detail = ExtensionConflict, err.Error()
	} else if owner, ok := s.fleet.State(value.AntennaID); !ok || owner.OwnerPassID != value.ID || owner.OwnerGeneration != interval.Generation {
		reservation.status, reservation.detail = ExtensionConflict, fmt.Sprintf("antenna %s is not owned by this pass reservation", value.AntennaID)
	}
	checks = append(checks, reservation)

	window := extensionCheck{item: ExtensionItemWindow, status: extensionReady}
	if err := (Window{AOS: value.Window.AOS, LOS: targetLOS, Revision: value.Window.Revision}).Validate(); err != nil {
		window.status, window.detail = ExtensionConflict, err.Error()
	}
	checks = append(checks, window)

	deadline := extensionCheck{item: ExtensionItemDeadline, status: extensionReady}
	if _, found := s.deadlines.Get(value.ID); !found {
		deadline.status, deadline.detail = ExtensionConflict, "no deadline is scheduled for the pass"
	}
	checks = append(checks, deadline)

	pointing := extensionCheck{item: ExtensionItemPointing, status: extensionReady}
	if active, found := s.commands.ActiveRevision(value.ID); found && active != value.Window.Revision {
		pointing.status, pointing.detail = ExtensionConflict, fmt.Sprintf("pointing queue is at revision %d, pass window at %d", active, value.Window.Revision)
	} else if commands, err := s.buildExtensionCommands(value, targetLOS); err != nil {
		pointing.status, pointing.detail = ExtensionConflict, err.Error()
	} else {
		plan.pointing = commands
	}
	checks = append(checks, pointing)

	receiverPlan := extensionCheck{item: ExtensionItemReceiverPlan, status: extensionReady}
	var problems []string
	for _, chainID := range value.RequiredReceiverChains {
		state, found := s.rf.State(chainID)
		switch {
		case !found:
			problems = append(problems, fmt.Sprintf("receiver chain %s is not registered", chainID))
		case state.PassID == "":
			problems = append(problems, fmt.Sprintf("receiver chain %s has no plan to extend", chainID))
		case state.PassID != value.ID:
			problems = append(problems, fmt.Sprintf("receiver chain %s is planned for pass %s", chainID, state.PassID))
		}
	}
	if len(problems) > 0 {
		receiverPlan.status, receiverPlan.detail = ExtensionConflict, strings.Join(problems, "; ")
	}
	checks = append(checks, receiverPlan)

	recording := extensionCheck{item: ExtensionItemRecording, status: extensionReady}
	segment, found := s.recorder.ActiveForPass(value.ID)
	switch {
	case !found:
		recording.status, recording.detail = ExtensionSkipped, "no active recording segment"
	case segment.Status != recorder.Open:
		recording.status, recording.detail = ExtensionConflict, fmt.Sprintf("recording segment %s is %s", segment.ID, segment.Status)
	default:
		plan.record = true
	}
	checks = append(checks, recording)

	return checks, plan
}

// buildExtensionCommands continues the pass's track from the last queued
// point through the extended LOS.
func (s *ExtensionService) buildExtensionCommands(value Pass, targetLOS time.Time) ([]scheduler.PointingCommand, error) {
	azimuth, elevation := 240.0, 8.0
	var lastAt time.Time
	for _, command := range s.commands.List(value.ID) {
		if command.Retired || !command.ExecuteAt.After(lastAt) {
			continue
		}
		lastAt, azimuth, elevation = command.ExecuteAt, command.Azimuth, command.Elevation
	}
	span := targetLOS.Sub(value.Window.LOS)
	points := []antenna.TrackPoint{
		{At: value.Window.LOS.Add(span / 2), Azimuth: math.Mod(azimuth+2, 360), Elevation: math.Max(0, elevation-1)},
		{At: targetLOS, Azimuth: math.Mod(azimuth+4, 360), Elevation: math.Max(0, elevation-2)},
	}
	return antenna.BuildCommands(value.ID, value.AntennaID, value.Window.Revision, points)
}

// applyExtension commits every checked item in a fixed order. Each step sets
// an absolute target derived from targetLOS, so replaying after a crash
// converges to the same state instead of extending twice.
func (s *ExtensionService) applyExtension(value Pass, targetLOS time.Time, plan extensionPlan, checks []extensionCheck) ([]ExtensionItem, Pass, error) {
	items := make([]ExtensionItem, 0, len(checks))
	mark := func(item string) {
		items = append(items, ExtensionItem{Item: item, Status: ExtensionApplied})
	}
	current := value

	interval, err := s.timeline.Extend(value.ReservationID, targetLOS)
	if err != nil {
		return items, current, err
	}
	if owner, ok := s.fleet.State(value.AntennaID); !ok || owner.OwnerPassID != value.ID || owner.OwnerGeneration != interval.Generation {
		return items, current, fmt.Errorf("antenna ownership changed during extension")
	}
	mark(ExtensionItemReservation)

	if err := s.passes.update(value.ID, func(pass *Pass) error {
		pass.Window.LOS = targetLOS
		return nil
	}); err != nil {
		return items, current, err
	}
	current, _ = s.passes.Get(value.ID)
	mark(ExtensionItemWindow)

	if err := s.deadlines.Extend(value.ID, targetLOS); err != nil {
		return items, current, err
	}
	mark(ExtensionItemDeadline)

	if err := s.commands.Append(value.ID, value.Window.Revision, plan.pointing); err != nil {
		return items, current, err
	}
	mark(ExtensionItemPointing)

	for _, chainID := range value.RequiredReceiverChains {
		if _, err := s.rf.ExtendPlan(chainID, value.ID, targetLOS); err != nil {
			return items, current, err
		}
	}
	mark(ExtensionItemReceiverPlan)

	if plan.record {
		if _, _, err := s.recorder.ExtendExpectation(value.ID, targetLOS); err != nil {
			return items, current, err
		}
		mark(ExtensionItemRecording)
	} else {
		items = append(items, ExtensionItem{Item: ExtensionItemRecording, Status: ExtensionSkipped, Detail: "no active recording segment"})
	}
	return items, current, nil
}

func extensionItems(checks []extensionCheck) []ExtensionItem {
	items := make([]ExtensionItem, 0, len(checks))
	for _, check := range checks {
		items = append(items, ExtensionItem{Item: check.item, Status: check.status, Detail: check.detail})
	}
	return items
}

func (s *ExtensionService) recordLocked(passID, requestID string) (extensionRecord, bool) {
	record, found := s.ledger[passID][requestID]
	return record, found
}

func (s *ExtensionService) rememberLocked(passID string, record extensionRecord) {
	requests, found := s.ledger[passID]
	if !found {
		requests = make(map[string]extensionRecord)
		s.ledger[passID] = requests
	}
	requests[record.RequestID] = record
}

func (s *ExtensionService) flushLocked() error {
	return store.WriteJSON(s.path, s.ledger)
}
