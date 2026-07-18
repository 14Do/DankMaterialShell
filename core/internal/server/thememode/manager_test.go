package thememode

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/geolocation"
)

// gatedDemandClient stands in for the lazy facade mid-acquisition: Acquire
// blocks until the gate opens, and GetLocation reports the idle 0,0 fix until
// then - exactly what a recompute racing the acquisition window observes.
type gatedDemandClient struct {
	gate     chan struct{}
	acquired chan struct{}
	loc      geolocation.Location
	source   string // written before acquired closes; read only after <-acquired
}

func (c *gatedDemandClient) Acquire(source string) {
	c.source = source
	close(c.acquired)
	<-c.gate
}

func (c *gatedDemandClient) Release(source string) {}

func (c *gatedDemandClient) GetLocation() (geolocation.Location, error) {
	select {
	case <-c.gate:
		return c.loc, nil
	default:
		return geolocation.Location{}, nil
	}
}

func (c *gatedDemandClient) Subscribe(id string) chan geolocation.Location {
	return make(chan geolocation.Location, 1)
}

func (c *gatedDemandClient) Unsubscribe(id string) {}

func (c *gatedDemandClient) Close() {}

func TestSetUseIPLocation_ClearsCachePoisonedDuringAcquire(t *testing.T) {
	fake := &gatedDemandClient{
		gate:     make(chan struct{}),
		acquired: make(chan struct{}),
		loc:      geolocation.Location{Latitude: 50.08, Longitude: 14.43},
	}
	m := &Manager{geoClient: fake}

	m.SetUseIPLocation(true)
	<-fake.acquired // demand goroutine is now blocked inside Acquire

	// The scheduler recomputes in the acquisition window: getLocation reads the
	// idle 0,0 fix through the facade and caches it.
	lat, lon := m.getLocation(m.getConfig())
	require.NotNil(t, lat)
	require.Zero(t, *lat)
	require.NotNil(t, lon)
	require.Zero(t, *lon)

	close(fake.gate) // acquisition completes

	require.Eventually(t, func() bool {
		m.locationMutex.RLock()
		defer m.locationMutex.RUnlock()
		return m.cachedIPLat == nil && m.cachedIPLon == nil
	}, time.Second, 5*time.Millisecond,
		"the post-Acquire clear must drop the 0,0 fix cached during the window")
}

// staticClient implements only Client (no DemandController) so handoff tests
// exercise the geoClient read sites without spawning demand goroutines.
type staticClient struct{}

func (staticClient) GetLocation() (geolocation.Location, error) {
	return geolocation.Location{}, nil
}

func (staticClient) Subscribe(id string) chan geolocation.Location {
	return make(chan geolocation.Location, 1)
}

func (staticClient) Unsubscribe(id string) {}

func (staticClient) Close() {}

// A persisted UseIPLocation=true restored before the client is wired (boot
// ordering) must re-assert demand and drop any stale cache once it lands.
func TestSetGeoClient_ReassertsDemandForPersistedIPLocation(t *testing.T) {
	fake := &gatedDemandClient{
		gate:     make(chan struct{}),
		acquired: make(chan struct{}),
		loc:      geolocation.Location{Latitude: 50.08, Longitude: 14.43},
	}
	m := &Manager{}
	m.config.UseIPLocation = true
	zero := 0.0
	m.cachedIPLat, m.cachedIPLon = &zero, &zero // stale fix from before the client existed

	m.SetGeoClient(fake)

	select {
	case <-fake.acquired:
	case <-time.After(time.Second):
		t.Fatal("SetGeoClient never re-asserted demand for the persisted setting")
	}
	require.Equal(t, "theme", fake.source)

	close(fake.gate)

	require.Eventually(t, func() bool {
		m.locationMutex.RLock()
		defer m.locationMutex.RUnlock()
		return m.cachedIPLat == nil && m.cachedIPLon == nil
	}, time.Second, 5*time.Millisecond,
		"boot re-assert must clear the stale cache after Acquire")
}

func TestSetGeoClient_NoDemandWhenIPLocationDisabled(t *testing.T) {
	fake := &gatedDemandClient{gate: make(chan struct{}), acquired: make(chan struct{})}
	m := &Manager{}

	m.SetGeoClient(fake)

	require.Never(t, func() bool {
		select {
		case <-fake.acquired:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond,
		"wiring the client must not acquire without the persisted setting")
}

// SetGeoClient lands on the boot goroutine while the scheduler recomputes and
// IPC handlers toggle concurrently. Run under -race: an unguarded handoff is a
// torn interface write.
func TestSetGeoClient_ConcurrentWithReaders(t *testing.T) {
	m := &Manager{}
	cfg := Config{UseIPLocation: true} // getLocation must reach the client read

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			m.SetGeoClient(staticClient{})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			m.getLocation(cfg)
			m.SetUseIPLocation(i%2 == 0)
		}
	}()
	close(start)
	wg.Wait()
}
