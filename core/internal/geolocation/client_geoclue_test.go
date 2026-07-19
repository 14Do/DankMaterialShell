package geolocation

import (
	"testing"
)

// The pump's notifySubscribers and the forwarder's Unsubscribe run concurrently
// on every demand teardown - without subMu this is a send on a closed channel.
func TestGeoClueClient_UnsubscribeDuringNotifyDoesNotPanic(t *testing.T) {
	c := &GeoClueClient{
		currLocation: &Location{Latitude: 50.08, Longitude: 14.43},
		subscribers:  make(map[string]chan Location),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			c.notifySubscribers()
		}
	}()

	for i := 0; i < 500; i++ {
		ch := c.Subscribe("lazy-forward")
		go func() { //nolint:staticcheck // drain so sends don't only hit the full-channel path
			for range ch {
			}
		}()
		c.Unsubscribe("lazy-forward")
	}

	<-done
}
