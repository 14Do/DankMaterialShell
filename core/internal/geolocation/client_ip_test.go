package geolocation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// getState handlers reach IpClient.GetLocation concurrently through the
// facade's read-through whenever the seed fetch failed. The check-fetch-store
// must be guarded: unguarded it is a data race on currLocation, and every
// caller fires its own 10s HTTP fetch.
func TestIpClient_ConcurrentGetLocationSingleFlights(t *testing.T) {
	orig := fetchIPLocation
	defer func() { fetchIPLocation = orig }()

	var calls int32
	fetchIPLocation = func() (ipLocationResult, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond) // hold the window open like a real round-trip
		return ipLocationResult{Location: Location{Latitude: 50.08, Longitude: 14.43}}, nil
	}

	c := newIpClient()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loc, err := c.GetLocation()
			assert.NoError(t, err)
			assert.Equal(t, Location{Latitude: 50.08, Longitude: 14.43}, loc)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls),
		"concurrent zero-fix reads must single-flight the fetch")
}
