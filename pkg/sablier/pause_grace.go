package sablier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sablierapp/sablier/pkg/store"
)

// HardStopProvider exposes an unconditional stop for the delayed second phase
// of Docker's pause strategy. It must not apply the provider's idle strategy.
type HardStopProvider interface {
	InstanceHardStop(context.Context, string) error
}

type keyedInstanceLocks struct {
	mu sync.Mutex
	m map[string]*keyedInstanceLock
}
type keyedInstanceLock struct { mu sync.Mutex; refs int }

func (k *keyedInstanceLocks) lock(name string) func() {
	k.mu.Lock()
	if k.m == nil { k.m = make(map[string]*keyedInstanceLock) }
	entry := k.m[name]
	if entry == nil { entry = &keyedInstanceLock{}; k.m[name] = entry }
	entry.refs++
	k.mu.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		k.mu.Lock()
		entry.refs--
		if entry.refs == 0 { delete(k.m, name) }
		k.mu.Unlock()
	}
}

type pauseGraceManager struct {
	s *Sablier
	file string
	duration time.Duration
	mu sync.Mutex
	deadline map[string]time.Time
	timers map[string]*time.Timer
}
func newPauseGraceManager(s *Sablier, duration time.Duration, file string) *pauseGraceManager {
	return &pauseGraceManager{s:s, duration:duration, file:file, deadline:map[string]time.Time{}, timers:map[string]*time.Timer{}}
}
func (m *pauseGraceManager) restore(ctx context.Context) error {
	data, err := os.ReadFile(m.file)
	if err != nil && !errors.Is(err, os.ErrNotExist) { return fmt.Errorf("read pause grace state: %w", err) }
	if err == nil {
		var deadlines map[string]time.Time
		if err := json.Unmarshal(data, &deadlines); err != nil { return fmt.Errorf("decode pause grace state: %w", err) }
		m.deadline = deadlines
	}
	m.mu.Lock()
	for name, deadline := range m.deadline { m.armLocked(name, deadline, time.Until(deadline)) }
	m.mu.Unlock()
	return nil
}
func (m *pauseGraceManager) persistLocked() error {
	data, err := json.Marshal(m.deadline)
	if err != nil { return err }
	tmp := m.file + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil { return err }
	if err := os.Rename(tmp, m.file); err != nil { _ = os.Remove(tmp); return err }
	return nil
}
func (m *pauseGraceManager) schedule(name string) {
	deadline := time.Now().Add(m.duration)
	m.mu.Lock()
	if timer := m.timers[name]; timer != nil { timer.Stop() }
	m.deadline[name] = deadline
	if err := m.persistLocked(); err != nil { m.s.l.Error("could not persist Docker pause grace deadline", slog.String("instance", name), slog.Any("error", err)) }
	m.armLocked(name, deadline, m.duration)
	m.mu.Unlock()
}
func (m *pauseGraceManager) armLocked(name string, deadline time.Time, delay time.Duration) {
	m.timers[name] = time.AfterFunc(max(delay, 0), func(){ m.expire(name, deadline) })
}
func (m *pauseGraceManager) cancel(name string) {
	m.mu.Lock()
	if timer := m.timers[name]; timer != nil { timer.Stop(); delete(m.timers, name) }
	if _, ok := m.deadline[name]; ok {
		delete(m.deadline, name)
		if err := m.persistLocked(); err != nil { m.s.l.Error("could not persist Docker pause grace cancellation", slog.String("instance", name), slog.Any("error", err)) }
	}
	m.mu.Unlock()
}
func (m *pauseGraceManager) pending(name string) bool {
	m.mu.Lock(); defer m.mu.Unlock()
	_, ok := m.deadline[name]; return ok
}
func (m *pauseGraceManager) expire(name string, deadline time.Time) {
	unlock := m.s.instanceLocks.lock(name); defer unlock()
	m.mu.Lock()
	current, ok := m.deadline[name]
	if !ok || !current.Equal(deadline) { m.mu.Unlock(); return }
	m.mu.Unlock()

	ctx := context.Background()
	if _, err := m.s.sessions.Get(ctx, name); err == nil {
		m.cancel(name); return
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		m.retry(name, deadline, err); return
	}
	if m.s.verifyEnabledOnExpiration {
		info, err := m.s.provider.InstanceInspect(ctx, name)
		if err != nil { m.retry(name, deadline, err); return }
		if !info.IsEnabled() { m.cancel(name); return }
	}
	provider, ok := m.s.provider.(HardStopProvider)
	if !ok { m.retry(name, deadline, errors.New("provider does not support hard stop")); return }
	if err := provider.InstanceHardStop(ctx, name); err != nil { m.retry(name, deadline, err); return }

	m.mu.Lock()
	if current, ok := m.deadline[name]; ok && current.Equal(deadline) {
		delete(m.deadline, name); delete(m.timers, name)
		if err := m.persistLocked(); err != nil { m.s.l.Error("could not persist completed Docker pause grace deadline", slog.String("instance", name), slog.Any("error", err)) }
	}
	m.mu.Unlock()
	m.s.metrics.RecordInstanceStop(name, "expired")
	m.s.metrics.RecordInactiveInstance(name)
	m.s.metrics.DiscardReadyWait(name)
	m.s.l.InfoContext(ctx, "paused container stopped after grace period", slog.String("instance", name))
}
func (m *pauseGraceManager) retry(name string, deadline time.Time, cause error) {
	m.s.l.Error("could not complete delayed Docker stop; retrying", slog.String("instance", name), slog.Any("error", cause))
	m.mu.Lock()
	if current, ok := m.deadline[name]; ok && current.Equal(deadline) { m.armLocked(name, deadline, 30*time.Second) }
	m.mu.Unlock()
}
func (s *Sablier) WithPauseGracePeriod(ctx context.Context, duration time.Duration, stateFile string) error {
	if duration <= 0 { return nil }
	if stateFile == "" { return fmt.Errorf("provider.docker.pause-grace-period requires storage.file") }
	if _, ok := s.provider.(HardStopProvider); !ok { return fmt.Errorf("provider does not support Docker pause grace periods") }
	manager := newPauseGraceManager(s, duration, stateFile+".pause-deadlines.json")
	if err := manager.restore(ctx); err != nil { return err }
	s.pauseGrace = manager
	return nil
}
