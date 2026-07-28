package sysupdate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// blockingBackend holds a check open so a test can change the phase underneath it.
// That interleaving needs no non-default setting: the scheduler runs a check
// every IntervalSeconds, 30 minutes by default, and Update All stays enabled
// while one is in flight.
type blockingBackend struct {
	pkgs    []Package
	err     error
	started chan struct{}
	release chan struct{}
}

func newBlockingBackend(pkgs []Package, err error) *blockingBackend {
	return &blockingBackend{
		pkgs:    pkgs,
		err:     err,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingBackend) ID() string                       { return "fake" }
func (b *blockingBackend) DisplayName() string              { return "Fake" }
func (b *blockingBackend) Repo() RepoKind                   { return RepoSystem }
func (b *blockingBackend) IsAvailable(context.Context) bool { return true }
func (b *blockingBackend) NeedsAuth() bool                  { return false }
func (b *blockingBackend) RunsInTerminal() bool             { return false }

func (b *blockingBackend) CheckUpdates(context.Context) ([]Package, error) {
	close(b.started)
	<-b.release
	return b.pkgs, b.err
}

func (b *blockingBackend) Upgrade(context.Context, UpgradeOptions, func(string)) error { return nil }

func newRefreshManager(b Backend) *Manager {
	return &Manager{
		state:     State{Phase: PhaseIdle, IntervalSeconds: defaultIntervalSeconds},
		stopChan:  make(chan struct{}),
		selection: Selection{System: b},
	}
}

// runRefreshDuringUpgrade runs a check, flips the phase to PhaseUpgrading while
// it is in flight, and returns the state the refresh tail left behind.
func runRefreshDuringUpgrade(t *testing.T, b *blockingBackend, manual bool) State {
	t.Helper()

	m := newRefreshManager(b)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.runRefresh(context.Background(), manual)
	}()

	select {
	case <-b.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the check never started")
	}

	m.mu.Lock()
	m.state.Phase = PhaseUpgrading
	m.mu.Unlock()
	close(b.release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runRefresh did not return")
	}

	return m.GetState()
}

// The upgrade owns the phase from the moment it starts: the UI derives
// isUpgrading from it, and nothing restores it once a refresh has overwritten
// it.
func TestRunRefreshKeepsUpgradingPhase(t *testing.T) {
	pkgs := []Package{{Name: "vim", Backend: "fake", Repo: RepoSystem}}

	tests := []struct {
		name   string
		err    error
		manual bool
	}{
		{name: "successful check", err: nil, manual: true},
		{name: "failed manual check", err: errors.New("boom"), manual: true},
		{name: "failed background check", err: errors.New("boom"), manual: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := runRefreshDuringUpgrade(t, newBlockingBackend(pkgs, tt.err), tt.manual)

			if state.Phase != PhaseUpgrading {
				t.Errorf("Phase = %q after the check finished, want %q", state.Phase, PhaseUpgrading)
			}
			if state.Error != nil {
				t.Errorf("Error = %+v, want none painted over a running upgrade", state.Error)
			}
		})
	}
}

// Suppressing the phase must not suppress the results: the check still ran, and
// its package list, count and timestamps are what the next paint reads.
func TestRunRefreshStillRecordsResultsDuringUpgrade(t *testing.T) {
	pkgs := []Package{{Name: "vim", Backend: "fake", Repo: RepoSystem}}

	state := runRefreshDuringUpgrade(t, newBlockingBackend(pkgs, nil), true)

	if state.Count != len(pkgs) {
		t.Errorf("Count = %d, want %d", state.Count, len(pkgs))
	}
	if len(state.Packages) != len(pkgs) {
		t.Fatalf("Packages = %d entries, want %d", len(state.Packages), len(pkgs))
	}
	if state.Packages[0].Name != pkgs[0].Name {
		t.Errorf("Packages[0].Name = %q, want %q", state.Packages[0].Name, pkgs[0].Name)
	}
	if state.LastCheckUnix == 0 {
		t.Error("LastCheckUnix = 0, want the check recorded")
	}
	if state.LastSuccessUnix == 0 {
		t.Error("LastSuccessUnix = 0, want a successful check recorded")
	}
	if state.NextCheckUnix == 0 {
		t.Error("NextCheckUnix = 0, want the next check scheduled")
	}
}

// The guard is scoped to a concurrent upgrade - with none, the tail still sets
// the phase it always did.
func TestRunRefreshSetsPhaseWithoutAnUpgrade(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		manual    bool
		wantPhase Phase
		wantError bool
	}{
		{name: "successful check", err: nil, manual: true, wantPhase: PhaseIdle},
		{name: "failed manual check", err: errors.New("boom"), manual: true, wantPhase: PhaseError, wantError: true},
		{name: "failed background check", err: errors.New("boom"), manual: false, wantPhase: PhaseIdle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockingBackend(nil, tt.err)
			close(b.release)

			m := newRefreshManager(b)
			m.runRefresh(context.Background(), tt.manual)

			state := m.GetState()
			if state.Phase != tt.wantPhase {
				t.Errorf("Phase = %q, want %q", state.Phase, tt.wantPhase)
			}
			if (state.Error != nil) != tt.wantError {
				t.Errorf("Error = %+v, want error: %v", state.Error, tt.wantError)
			}
		})
	}
}
