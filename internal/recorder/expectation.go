package recorder

import (
	"fmt"
	"time"

	"orbitlink/internal/audit"
)

// ExtendExpectation moves the recording expectation of the pass's active
// segment out to until. The horizon is monotonic so replays are no-ops.
// found=false reports that the pass has no active recorder to extend.
func (m *Manager) ExtendExpectation(passID string, until time.Time) (segment Segment, found bool, err error) {
	if passID == "" || until.IsZero() {
		return Segment{}, false, fmt.Errorf("pass and expectation horizon are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, current := range m.sessions {
		if current.segment.PassID != passID || current.segment.Status == Sealed || current.segment.Status == Aborted {
			continue
		}
		if current.segment.Status != Open {
			return Segment{}, true, fmt.Errorf("recording segment %s cannot extend expectation from %s", current.segment.ID, current.segment.Status)
		}
		if until.After(current.segment.ExpectedUntil) {
			current.segment.ExpectedUntil = until
			if err := m.flushLocked(); err != nil {
				return Segment{}, true, err
			}
			_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "expectation.extended", Subject: current.segment.ID, Fields: map[string]any{"pass_id": passID, "expected_until": until}})
		}
		return current.segment, true, nil
	}
	return Segment{}, false, nil
}
