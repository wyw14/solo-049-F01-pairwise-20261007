package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

// Timeline owns the check-and-publish boundary for every antenna mutation.
type Timeline struct {
	mu        sync.RWMutex
	path      string
	byAntenna map[string][]Interval
	nextGen   map[string]int64
	audit     *audit.Trail
}

func NewTimeline(paths store.Paths, trail *audit.Trail) (*Timeline, error) {
	timeline := &Timeline{
		path: paths.Snapshot("antenna-timeline"), byAntenna: make(map[string][]Interval),
		nextGen: make(map[string]int64), audit: trail,
	}
	var values []Interval
	found, err := store.ReadJSON(timeline.path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			timeline.byAntenna[value.AntennaID] = append(timeline.byAntenna[value.AntennaID], value)
			if value.Generation > timeline.nextGen[value.AntennaID] {
				timeline.nextGen[value.AntennaID] = value.Generation
			}
		}
	}
	return timeline, nil
}

func (t *Timeline) Reserve(passID, antennaID string, start, end time.Time) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextGen[antennaID]++
	value := Interval{
		ReservationID: uuid.NewString(), PassID: passID, AntennaID: antennaID,
		Start: start, End: end, Generation: t.nextGen[antennaID], Status: Reserved,
	}
	if err := value.Validate(); err != nil {
		return Interval{}, err
	}
	if conflict := t.conflictLocked(value, ""); conflict != nil {
		return Interval{}, conflict
	}
	t.byAntenna[antennaID] = append(t.byAntenna[antennaID], value)
	if err := t.flushLocked(); err != nil {
		t.byAntenna[antennaID] = t.byAntenna[antennaID][:len(t.byAntenna[antennaID])-1]
		return Interval{}, err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: "reserved", Subject: value.ReservationID, Fields: map[string]any{"pass_id": passID, "antenna_id": antennaID, "generation": value.Generation}})
	return value, nil
}

func (t *Timeline) Extend(reservationID string, newEnd time.Time) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	antennaID, index, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if current.Status == Released {
		return Interval{}, fmt.Errorf("reservation %s is already released", reservationID)
	}
	updated := current
	updated.End = newEnd
	if err := updated.Validate(); err != nil {
		return Interval{}, err
	}
	if conflict := t.conflictLocked(updated, reservationID); conflict != nil {
		return Interval{}, conflict
	}
	t.byAntenna[antennaID][index] = updated
	if err := t.flushLocked(); err != nil {
		t.byAntenna[antennaID][index] = current
		return Interval{}, err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: "extended", Subject: reservationID, Fields: map[string]any{"end": newEnd}})
	return updated, nil
}

// CheckExtend validates an extension without publishing it, so callers can
// preflight every resource before mutating any of them.
func (t *Timeline) CheckExtend(reservationID string, newEnd time.Time) (Interval, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, _, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if current.Status == Released {
		return Interval{}, fmt.Errorf("reservation %s is already released", reservationID)
	}
	updated := current
	updated.End = newEnd
	if err := updated.Validate(); err != nil {
		return Interval{}, err
	}
	if conflict := t.conflictLocked(updated, reservationID); conflict != nil {
		return Interval{}, conflict
	}
	return updated, nil
}

func (t *Timeline) Activate(reservationID string) (Interval, error) {
	return t.setStatus(reservationID, Active)
}

func (t *Timeline) Release(reservationID string) (Interval, error) {
	return t.setStatus(reservationID, Released)
}

func (t *Timeline) setStatus(reservationID string, status ReservationStatus) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	antennaID, index, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	updated := current
	updated.Status = status
	t.byAntenna[antennaID][index] = updated
	if err := t.flushLocked(); err != nil {
		t.byAntenna[antennaID][index] = current
		return Interval{}, err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: string(status), Subject: reservationID, Fields: map[string]any{"pass_id": current.PassID}})
	return updated, nil
}

func (t *Timeline) List(antennaID string) []Interval {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var values []Interval
	if antennaID != "" {
		values = append(values, t.byAntenna[antennaID]...)
	} else {
		for _, intervals := range t.byAntenna {
			values = append(values, intervals...)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Start.Before(values[j].Start) })
	return values
}

func (t *Timeline) CurrentOwner(antennaID string, at time.Time) (Interval, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, value := range t.byAntenna[antennaID] {
		if value.Status != Released && !at.Before(value.Start) && at.Before(value.End) {
			return value, true
		}
	}
	return Interval{}, false
}

func (t *Timeline) conflictLocked(candidate Interval, excluded string) error {
	for _, existing := range t.byAntenna[candidate.AntennaID] {
		if existing.ReservationID == excluded || existing.Status == Released {
			continue
		}
		if candidate.Overlaps(existing) {
			return &ConflictError{AntennaID: candidate.AntennaID, Existing: existing}
		}
	}
	return nil
}

func (t *Timeline) findLocked(id string) (string, int, Interval, bool) {
	for antennaID, intervals := range t.byAntenna {
		for index, value := range intervals {
			if value.ReservationID == id {
				return antennaID, index, value, true
			}
		}
	}
	return "", 0, Interval{}, false
}

func (t *Timeline) flushLocked() error {
	var values []Interval
	for _, intervals := range t.byAntenna {
		values = append(values, intervals...)
	}
	return store.WriteJSON(t.path, values)
}
