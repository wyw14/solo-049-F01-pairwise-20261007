package rf

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type Controller struct {
	mu     sync.RWMutex
	path   string
	states map[string]State
	audit  *audit.Trail
}

func NewController(paths store.Paths, trail *audit.Trail, chainIDs []string) (*Controller, error) {
	controller := &Controller{path: paths.Snapshot("rf-chains"), states: make(map[string]State), audit: trail}
	var states []State
	found, err := store.ReadJSON(controller.path, &states)
	if err != nil {
		return nil, err
	}
	if found {
		for _, state := range states {
			controller.states[state.ChainID] = state
		}
	}
	for _, id := range chainIDs {
		if _, exists := controller.states[id]; !exists {
			controller.states[id] = State{ChainID: id, Polarization: RightCircular}
		}
	}
	if err := controller.flushLocked(); err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *Controller) Apply(plan Plan) (State, error) {
	if plan.PassID == "" || plan.ChainID == "" || plan.CenterHz <= 0 {
		return State{}, fmt.Errorf("pass, RF chain and center frequency are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.states[plan.ChainID]
	if !found {
		return State{}, fmt.Errorf("RF chain %s not found", plan.ChainID)
	}
	state.PassID = plan.PassID
	state.CenterHz = plan.CenterHz
	state.DopplerHz = plan.DopplerHz
	state.Polarization = plan.Polarization
	state.PlanUntil = time.Time{}
	state.UpdatedAt = time.Now().UTC()
	c.states[plan.ChainID] = state
	if err := c.flushLocked(); err != nil {
		return State{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "rf", Action: "plan.applied", Subject: plan.ChainID, Fields: map[string]any{"pass_id": plan.PassID, "center_hz": plan.CenterHz, "doppler_hz": plan.DopplerHz}})
	return state, nil
}

// ExtendPlan moves the chain's plan validity horizon out to until. The
// horizon is monotonic: an earlier horizon than the current one is ignored,
// which keeps replays of the same extension a no-op.
func (c *Controller) ExtendPlan(chainID, passID string, until time.Time) (State, error) {
	if chainID == "" || passID == "" || until.IsZero() {
		return State{}, fmt.Errorf("chain, pass and plan horizon are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.states[chainID]
	if !found {
		return State{}, fmt.Errorf("RF chain %s not found", chainID)
	}
	if state.PassID == "" {
		return State{}, fmt.Errorf("RF chain %s has no plan to extend", chainID)
	}
	if state.PassID != passID {
		return State{}, fmt.Errorf("RF chain %s is planned for pass %s", chainID, state.PassID)
	}
	if until.After(state.PlanUntil) {
		state.PlanUntil = until
		state.UpdatedAt = time.Now().UTC()
		c.states[chainID] = state
		if err := c.flushLocked(); err != nil {
			return State{}, err
		}
		_, _ = c.audit.Record(audit.Event{Component: "rf", Action: "plan.extended", Subject: chainID, Fields: map[string]any{"pass_id": passID, "plan_until": until}})
	}
	return state, nil
}

func (c *Controller) EnableTransmit(chainID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.states[chainID]
	if !found {
		return fmt.Errorf("RF chain %s not found", chainID)
	}
	state.TransmitEnabled = true
	state.InhibitOperation = ""
	state.UpdatedAt = time.Now().UTC()
	c.states[chainID] = state
	return c.flushLocked()
}

func (c *Controller) State(chainID string) (State, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, found := c.states[chainID]
	return state, found
}

func (c *Controller) List() []State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	values := make([]State, 0, len(c.states))
	for _, state := range c.states {
		values = append(values, state)
	}
	return values
}

func (c *Controller) flushLocked() error {
	values := make([]State, 0, len(c.states))
	for _, state := range c.states {
		values = append(values, state)
	}
	return store.WriteJSON(c.path, values)
}
