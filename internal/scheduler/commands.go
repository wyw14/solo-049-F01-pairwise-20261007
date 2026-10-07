package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/store"
)

type PointingCommand struct {
	ID          string    `json:"id"`
	PassID      string    `json:"pass_id"`
	AntennaID   string    `json:"antenna_id"`
	Revision    int64     `json:"revision"`
	ExecuteAt   time.Time `json:"execute_at"`
	Azimuth     float64   `json:"azimuth"`
	Elevation   float64   `json:"elevation"`
	Retired     bool      `json:"retired"`
	RetireCause string    `json:"retire_cause,omitempty"`
}

type CommandQueue struct {
	mu       sync.RWMutex
	path     string
	commands []PointingCommand
	active   map[string]int64
}

func NewCommandQueue(paths store.Paths) (*CommandQueue, error) {
	queue := &CommandQueue{path: paths.Snapshot("pointing-commands"), active: make(map[string]int64)}
	found, err := store.ReadJSON(queue.path, &queue.commands)
	if err != nil {
		return nil, err
	}
	if found {
		for _, command := range queue.commands {
			if !command.Retired && command.Revision > queue.active[command.PassID] {
				queue.active[command.PassID] = command.Revision
			}
		}
	}
	return queue, nil
}

func (q *CommandQueue) Replace(passID string, revision int64, commands []PointingCommand) error {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return fmt.Errorf("pass, revision and pointing commands are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.commands {
		if q.commands[index].PassID == passID && !q.commands[index].Retired {
			q.commands[index].Retired = true
			q.commands[index].RetireCause = fmt.Sprintf("replaced by revision %d", revision)
		}
	}
	for _, command := range commands {
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		q.commands = append(q.commands, command)
	}
	q.active[passID] = revision
	return store.WriteJSON(q.path, q.commands)
}

// Append adds commands to the active revision without retiring anything.
// Commands whose ExecuteAt already exists for the pass are skipped, so
// replaying the same append is a no-op.
func (q *CommandQueue) Append(passID string, revision int64, commands []PointingCommand) error {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return fmt.Errorf("pass, revision and pointing commands are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if active, ok := q.active[passID]; ok && active != revision {
		return fmt.Errorf("pass %s pointing queue is at revision %d, not %d", passID, active, revision)
	}
	for _, command := range commands {
		duplicate := false
		for _, existing := range q.commands {
			if existing.PassID == passID && !existing.Retired && existing.ExecuteAt.Equal(command.ExecuteAt) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		q.commands = append(q.commands, command)
	}
	q.active[passID] = revision
	return store.WriteJSON(q.path, q.commands)
}

func (q *CommandQueue) ActiveRevision(passID string) (int64, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	revision, found := q.active[passID]
	return revision, found
}

func (q *CommandQueue) Due(now time.Time) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if !command.Retired && command.Revision == q.active[command.PassID] && !command.ExecuteAt.After(now) {
			values = append(values, command)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ExecuteAt.Before(values[j].ExecuteAt) })
	return values
}

func (q *CommandQueue) IsExecutable(command PointingCommand) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !command.Retired && command.Revision == q.active[command.PassID]
}

func (q *CommandQueue) List(passID string) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if passID == "" || command.PassID == passID {
			values = append(values, command)
		}
	}
	return values
}
