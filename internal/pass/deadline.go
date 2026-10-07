package pass

import (
	"fmt"
	"sync"
	"time"

	stationclock "orbitlink/internal/clock"
)

type Deadline struct {
	PassID       string
	Phase        Phase
	CivilLOS     time.Time
	MonotonicLOS time.Time
	StartedAt    time.Time
}

type DeadlineManager struct {
	mu        sync.RWMutex
	deadlines map[string]Deadline
	clock     *stationclock.Service
}

func NewDeadlineManager(clock *stationclock.Service) *DeadlineManager {
	manager := &DeadlineManager{deadlines: make(map[string]Deadline), clock: clock}
	clock.Subscribe(manager.ApplyCorrection)
	return manager
}

func (m *DeadlineManager) Schedule(value Pass) error {
	if err := value.Window.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	m.deadlines[value.ID] = Deadline{PassID: value.ID, Phase: value.Phase, CivilLOS: value.Window.LOS}
	m.mu.Unlock()
	return nil
}

func (m *DeadlineManager) Activate(passID string, duration time.Duration) (Deadline, error) {
	if duration <= 0 {
		return Deadline{}, fmt.Errorf("active pass duration must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline, found := m.deadlines[passID]
	if !found {
		return Deadline{}, fmt.Errorf("deadline for pass %s not found", passID)
	}
	deadline.Phase = Tracking
	deadline.StartedAt = time.Now()
	deadline.MonotonicLOS = m.clock.MonotonicDeadline(duration)
	m.deadlines[passID] = deadline
	return deadline, nil
}

// Extend re-anchors the deadline to newCivilLOS. When the pass has an active
// monotonic deadline it is recomputed from the same horizon, so replaying an
// extension with the same target is a no-op.
func (m *DeadlineManager) Extend(passID string, newCivilLOS time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline, found := m.deadlines[passID]
	if !found {
		return fmt.Errorf("deadline for pass %s not found", passID)
	}
	deadline.CivilLOS = newCivilLOS
	if !deadline.MonotonicLOS.IsZero() {
		deadline.MonotonicLOS = m.clock.MonotonicDeadline(time.Until(newCivilLOS))
	}
	m.deadlines[passID] = deadline
	return nil
}

func (m *DeadlineManager) ApplyCorrection(correction stationclock.Correction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, deadline := range m.deadlines {
		if deadline.Phase == Tracking || deadline.Phase == Draining {
			continue
		}
		deadline.CivilLOS = deadline.CivilLOS.Add(correction.Delta)
		m.deadlines[id] = deadline
	}
}

func (m *DeadlineManager) Get(passID string) (Deadline, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	deadline, found := m.deadlines[passID]
	return deadline, found
}

func (m *DeadlineManager) Remaining(passID string) (time.Duration, error) {
	deadline, found := m.Get(passID)
	if !found || deadline.MonotonicLOS.IsZero() {
		return 0, fmt.Errorf("pass %s has no active monotonic deadline", passID)
	}
	return time.Until(deadline.MonotonicLOS), nil
}
