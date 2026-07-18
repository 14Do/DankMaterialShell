package thememode

import (
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
}

func (c *gatedDemandClient) Acquire(source string) {
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
