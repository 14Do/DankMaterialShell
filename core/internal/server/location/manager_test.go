package location

import (
	"sync"
	"testing"
	"time"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/geolocation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClient returns whatever fix it holds; push delivers a subscriber update.
type stubClient struct {
	mu   sync.Mutex
	loc  geolocation.Location
	subs map[string]chan geolocation.Location
}

func newStubClient() *stubClient {
	return &stubClient{subs: make(map[string]chan geolocation.Location)}
}

func (s *stubClient) GetLocation() (geolocation.Location, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loc, nil
}

func (s *stubClient) Subscribe(id string) chan geolocation.Location {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan geolocation.Location, 8)
	s.subs[id] = ch
	return ch
}

func (s *stubClient) Unsubscribe(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subs[id]; ok {
		delete(s.subs, id)
		close(ch)
	}
}

func (s *stubClient) Close() {}

func (s *stubClient) hasSub(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.subs[id]
	return ok
}

func (s *stubClient) setLocation(loc geolocation.Location) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loc = loc
}

func (s *stubClient) push(loc geolocation.Location) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loc = loc
	for _, ch := range s.subs {
		ch <- loc
	}
}

// An Acquire seeds the client silently, so the manager cache stays 0,0.
// CurrentState must read through to the client; GetState alone must not.
func TestManager_CurrentStateReadsThroughToSeededClient(t *testing.T) {
	stub := newStubClient()
	m, err := NewManager(stub)
	require.NoError(t, err)
	defer m.Close()

	assert.Equal(t, State{}, m.CurrentState(), "no fix anywhere yet")

	// Seed after Acquire: the client has a fix, nothing pushed to subscribers.
	stub.setLocation(geolocation.Location{Latitude: 50.08, Longitude: 14.43})

	assert.Equal(t, State{}, m.GetState(), "cache untouched by a silent seed")
	assert.Equal(t, State{Latitude: 50.08, Longitude: 14.43}, m.CurrentState(),
		"CurrentState reads through to the client's seeded fix")
}

// Once a real update streams in, the cache wins and no read-through happens.
func TestManager_CurrentStatePrefersCacheAfterRealUpdate(t *testing.T) {
	stub := newStubClient()
	m, err := NewManager(stub)
	require.NoError(t, err)
	defer m.Close()

	require.Eventually(t, func() bool { return stub.hasSub("locationManager") },
		time.Second, 5*time.Millisecond, "signal pump subscribes on construction")

	stub.push(geolocation.Location{Latitude: 51.5, Longitude: -0.12})

	require.Eventually(t, func() bool {
		return m.GetState() == State{Latitude: 51.5, Longitude: -0.12}
	}, time.Second, 5*time.Millisecond, "pushed update reaches the cache")

	stub.setLocation(geolocation.Location{Latitude: 1, Longitude: 1})
	assert.Equal(t, State{Latitude: 51.5, Longitude: -0.12}, m.CurrentState(),
		"cache is authoritative once populated")
}
