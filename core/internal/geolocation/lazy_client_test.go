package geolocation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeInner is a controllable Client used to drive the lazyClient facade without
// touching real D-Bus / GeoClue2 / ip-api.
type fakeInner struct {
	mu     sync.Mutex
	loc    Location
	subs   map[string]chan Location
	closed bool
	log    []string // teardown-order log: "unsubscribe:<id>" and "close" entries
}

func newFakeInner(loc Location) *fakeInner {
	return &fakeInner{loc: loc, subs: make(map[string]chan Location)}
}

func (f *fakeInner) GetLocation() (Location, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loc, nil
}

func (f *fakeInner) Subscribe(id string) chan Location {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan Location, 8)
	f.subs[id] = ch
	return ch
}

func (f *fakeInner) Unsubscribe(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, "unsubscribe:"+id)
	if ch, ok := f.subs[id]; ok {
		delete(f.subs, id)
		close(ch)
	}
}

func (f *fakeInner) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, "close")
	if f.closed {
		return
	}
	f.closed = true
	for id, ch := range f.subs {
		delete(f.subs, id)
		close(ch)
	}
}

func (f *fakeInner) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func (f *fakeInner) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeInner) hasSub(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.subs[id]
	return ok
}

func (f *fakeInner) push(loc Location) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loc = loc
	for _, ch := range f.subs {
		ch <- loc
	}
}

// newTestLazyClient returns a facade whose factory hands out the given inner
// clients in order, counting how many times it was called.
func newTestLazyClient(inners ...*fakeInner) (*lazyClient, *int32) {
	var calls int32
	lc := newLazyClient()
	lc.acquire = func() Client {
		n := atomic.AddInt32(&calls, 1)
		idx := int(n) - 1
		if idx < len(inners) {
			return inners[idx]
		}
		return inners[len(inners)-1]
	}
	return lc, &calls
}

func TestLazyClient_IdleDoesNotAcquire(t *testing.T) {
	lc, calls := newTestLazyClient(newFakeInner(Location{Latitude: 1, Longitude: 2}))

	assert.Equal(t, int32(0), atomic.LoadInt32(calls), "no acquisition before demand")

	loc, err := lc.GetLocation()
	require.NoError(t, err)
	assert.Equal(t, Location{}, loc, "idle facade reports no fix")
	assert.Equal(t, int32(0), atomic.LoadInt32(calls), "GetLocation must not acquire")
}

func TestLazyClient_AcquireCreatesInnerOnce(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 50.08, Longitude: 14.43})
	lc, calls := newTestLazyClient(inner)

	lc.Acquire("weather")
	lc.Acquire("nightlight") // second consumer must reuse, not re-create

	assert.Equal(t, int32(1), atomic.LoadInt32(calls), "one client for multiple consumers")

	loc, err := lc.GetLocation()
	require.NoError(t, err)
	assert.Equal(t, inner.loc, loc, "GetLocation delegates to the inner client")
}

func TestLazyClient_ReleaseIsRefcounted(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 1, Longitude: 1})
	lc, _ := newTestLazyClient(inner)

	lc.Acquire("weather")
	lc.Acquire("nightlight")

	lc.Release("weather")
	assert.False(t, inner.isClosed(), "inner stays up while another consumer holds it")

	lc.Release("nightlight")
	assert.True(t, inner.isClosed(), "inner torn down when the last consumer releases")
}

func TestLazyClient_ReacquireAfterRelease(t *testing.T) {
	first := newFakeInner(Location{Latitude: 1, Longitude: 1})
	second := newFakeInner(Location{Latitude: 2, Longitude: 2})
	lc, calls := newTestLazyClient(first, second)

	lc.Acquire("weather")
	lc.Release("weather")
	require.True(t, first.isClosed())

	lc.Acquire("weather")
	assert.Equal(t, int32(2), atomic.LoadInt32(calls), "re-acquire builds a fresh client")

	loc, err := lc.GetLocation()
	require.NoError(t, err)
	assert.Equal(t, second.loc, loc)
}

func TestLazyClient_DuplicateAcquireAndUnknownReleaseAreNoops(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 1, Longitude: 1})
	lc, calls := newTestLazyClient(inner)

	lc.Acquire("weather")
	lc.Acquire("weather") // idempotent - same source
	assert.Equal(t, int32(1), atomic.LoadInt32(calls))

	lc.Release("theme") // never acquired - must not tear down
	assert.False(t, inner.isClosed())

	lc.Release("weather")
	assert.True(t, inner.isClosed())
}

func TestLazyClient_ForwardsUpdatesToSubscribers(t *testing.T) {
	inner := newFakeInner(Location{})
	lc, _ := newTestLazyClient(inner)

	// Subscribe before any acquisition, like the location manager does at boot.
	sub := lc.Subscribe("locationManager")

	lc.Acquire("weather")

	// Wait until the forwarder has attached to the inner client, then push.
	require.Eventually(t, func() bool { return inner.hasSub("lazy-forward") },
		time.Second, 5*time.Millisecond, "forwarder should subscribe to the inner client")

	want := Location{Latitude: 51.5, Longitude: -0.12}
	inner.push(want)

	select {
	case got := <-sub:
		assert.Equal(t, want, got, "update forwarded to the facade subscriber")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded update")
	}
}

// Reproduces the shutdown ordering: the location manager closes first and its
// signal pump's deferred Unsubscribe closes the facade channel while the
// forwarder may still be delivering an inner update. Without subMu this is a
// send on a closed channel (panic) and a -race report.
func TestLazyClient_UnsubscribeDuringForwardDoesNotPanic(t *testing.T) {
	inner := newFakeInner(Location{})
	lc, _ := newTestLazyClient(inner)

	lc.Acquire("weather")
	require.Eventually(t, func() bool { return inner.hasSub("lazy-forward") },
		time.Second, 5*time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			inner.push(Location{Latitude: float64(i), Longitude: 1})
		}
	}()

	for i := 0; i < 200; i++ {
		sub := lc.Subscribe("locationManager")
		go func() { //nolint:staticcheck // drain so pushes don't hit the full-channel path only
			for range sub {
			}
		}()
		lc.Unsubscribe("locationManager")
	}

	<-done
	lc.Release("weather")
}

// SeedLocation writes silently and a LocationUpdated that fires before the
// forwarder attaches lands on an empty subscriber map, so without the one-shot
// prime a stream subscriber that predates acquisition never sees the initial
// fix on a stationary machine (no DistanceThreshold, nothing re-emits).
func TestLazyClient_PrimesSubscribersWithExistingFix(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 50.08, Longitude: 14.43})
	lc, _ := newTestLazyClient(inner)

	sub := lc.Subscribe("locationManager") // stream subscriber predates acquisition

	lc.Acquire("weather")

	select {
	case got := <-sub:
		assert.Equal(t, Location{Latitude: 50.08, Longitude: 14.43}, got,
			"the inner client's existing fix must reach pre-existing subscribers without a push")
	case <-time.After(time.Second):
		t.Fatal("facade never primed the subscriber with the acquired fix")
	}
}

// Teardown must join the forwarder before closing the inner client: forward's
// deferred Unsubscribe (LoadAndDelete+close) and Close's subscriber sweep
// (load+close) otherwise race to close the same channel - a double close that
// panics the daemon on the last consumer's toggle-off.
func TestLazyClient_TeardownJoinsForwarderBeforeClosingInner(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 1, Longitude: 1})
	lc, _ := newTestLazyClient(inner)

	lc.Acquire("weather")
	require.Eventually(t, func() bool { return inner.hasSub("lazy-forward") },
		time.Second, 5*time.Millisecond)

	lc.Release("weather")

	require.True(t, inner.isClosed())
	assert.Equal(t, []string{"unsubscribe:lazy-forward", "close"}, inner.events(),
		"forwarder's deferred Unsubscribe must complete before inner.Close")
}

func TestLazyClient_CloseTearsDownAndClosesSubscribers(t *testing.T) {
	inner := newFakeInner(Location{Latitude: 1, Longitude: 1})
	lc, _ := newTestLazyClient(inner)

	sub := lc.Subscribe("locationManager")
	lc.Acquire("weather")

	lc.Close()

	assert.True(t, inner.isClosed(), "Close tears down the inner client")

	// The facade subscriber channel must be closed by Close. The prime may have
	// delivered the inner client's fix first, so drain through to the close.
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-sub:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("subscriber channel not closed")
		}
	}
}
