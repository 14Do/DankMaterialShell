package geolocation

import (
	"sync"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/log"
)

// DemandController lets consumers signal whether they currently need location.
// The lazy client acquires the underlying GeoClue2/IP client on the first demand
// and releases it when the last consumer goes away.
//
// The concrete lazyClient returned by NewClient implements this alongside Client,
// so consumers reach it with a type assertion:
//
//	if dc, ok := client.(geolocation.DemandController); ok { dc.Acquire("weather") }
type DemandController interface {
	Acquire(source string)
	Release(source string)
}

// lazyClient is a Client facade that owns the real location client on demand.
//
// It performs NO network egress until a consumer calls Acquire. This replaces the
// previous behaviour where NewClient eagerly started GeoClue2 and seeded a fix from
// http://ip-api.com on every daemon start, regardless of user settings.
//
// The facade holds one stable reference for the life of the process (handed to the
// location manager, the gamma manager and the thememode manager at boot), so those
// consumers never need re-wiring as the underlying client comes and goes.
type lazyClient struct {
	mu      sync.Mutex          // guards demand, inner, fwdStop
	demand  map[string]struct{} // sources that currently want location
	inner   Client              // the real client, nil while idle
	fwdStop chan struct{}       // stops the forwarder for the current inner

	acqMu sync.Mutex     // serializes acquisition so every Acquire caller sees a ready client
	fwdWG sync.WaitGroup // tracks the forwarder goroutine

	acquire func() Client // builds the real client; overridable in tests

	// subMu guards subscribers and excludes forward's sends from Unsubscribe/
	// Close closing a channel mid-send. Shutdown closes the location manager
	// (whose signal pump defer unsubscribes here) before the facade, so without
	// this a LocationUpdated in that window panics on a send to the just-closed
	// channel.
	subMu       sync.RWMutex
	subscribers map[string]chan Location
}

func newLazyClient() *lazyClient {
	return &lazyClient{
		demand:      make(map[string]struct{}),
		subscribers: make(map[string]chan Location),
		acquire:     acquireClient,
	}
}

// Acquire records that source needs location and ensures the underlying client
// exists. It blocks until acquisition has been attempted, so a consumer that pulls
// GetLocation immediately afterwards sees the seeded fix.
func (l *lazyClient) Acquire(source string) {
	l.mu.Lock()
	l.demand[source] = struct{}{}
	l.mu.Unlock()
	l.ensureInner()
}

// Release records that source no longer needs location and tears the client down
// once no consumer wants it.
func (l *lazyClient) Release(source string) {
	l.mu.Lock()
	if _, ok := l.demand[source]; !ok {
		l.mu.Unlock()
		return
	}
	delete(l.demand, source)
	empty := len(l.demand) == 0
	l.mu.Unlock()
	if empty {
		l.teardown()
	}
}

// ensureInner creates the inner client if there is demand and none exists yet.
// acqMu serializes it so concurrent Acquire callers all return with a ready client
// (only the first does the slow work; the rest wait and observe the result).
func (l *lazyClient) ensureInner() {
	l.acqMu.Lock()
	defer l.acqMu.Unlock()

	l.mu.Lock()
	if l.inner != nil || len(l.demand) == 0 {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	inner := l.acquire() // slow: dbus + IP seed. Deliberately not holding l.mu.

	l.mu.Lock()
	if len(l.demand) == 0 { // everyone released while we were acquiring
		l.mu.Unlock()
		inner.Close()
		return
	}
	stop := make(chan struct{})
	l.inner = inner
	l.fwdStop = stop
	l.mu.Unlock()

	l.fwdWG.Add(1)
	go l.forward(inner, stop)
}

// teardown closes the current inner client once nothing wants location.
func (l *lazyClient) teardown() {
	l.acqMu.Lock()
	defer l.acqMu.Unlock()

	l.mu.Lock()
	if len(l.demand) != 0 { // re-acquired while we waited on acqMu
		l.mu.Unlock()
		return
	}
	inner := l.inner
	stop := l.fwdStop
	l.inner = nil
	l.fwdStop = nil
	l.mu.Unlock()

	if stop != nil {
		close(stop) // stops the forwarder even for IpClient, whose channel never closes
	}
	// Join the forwarder before closing the inner client: its deferred Unsubscribe
	// and Close's subscriber sweep would otherwise race to close the same channel.
	l.fwdWG.Wait()
	if inner != nil {
		inner.Close()
	}
}

// forward fans the inner client's updates out to the facade's own subscribers, so a
// consumer that subscribed once (at boot) keeps receiving across acquire cycles.
func (l *lazyClient) forward(inner Client, stop chan struct{}) {
	defer l.fwdWG.Done()
	ch := inner.Subscribe("lazy-forward")
	defer inner.Unsubscribe("lazy-forward")

	// One-shot prime: the IP seed is written silently (SeedLocation emits no
	// event) and an update that fired before the Subscribe above landed on an
	// empty subscriber map, so fan out the fix the inner client already holds -
	// otherwise a stream subscriber that predates acquisition never sees it.
	if loc, err := inner.GetLocation(); err == nil && (loc.Latitude != 0 || loc.Longitude != 0) {
		l.fanOut(loc)
	}

	for {
		select {
		case <-stop:
			return
		case loc, ok := <-ch:
			if !ok {
				return
			}
			l.fanOut(loc)
		}
	}
}

func (l *lazyClient) fanOut(loc Location) {
	l.subMu.RLock()
	defer l.subMu.RUnlock()
	for _, out := range l.subscribers {
		select {
		case out <- loc:
		default:
			log.Warn("Location: facade subscriber channel full, dropping update")
		}
	}
}

// --- Client interface ---

func (l *lazyClient) GetLocation() (Location, error) {
	l.mu.Lock()
	inner := l.inner
	l.mu.Unlock()
	if inner == nil {
		return Location{}, nil // idle: no fix, consumers already guard on 0,0
	}
	return inner.GetLocation()
}

func (l *lazyClient) Subscribe(id string) chan Location {
	ch := make(chan Location, 64)
	l.subMu.Lock()
	// A same-id re-subscribe replaces the stream: close the old channel so its
	// reader unblocks instead of waiting forever on an orphan that nothing fans
	// out to anymore. Safe against a concurrent fanOut send - it holds subMu.
	if old, ok := l.subscribers[id]; ok {
		close(old)
	}
	l.subscribers[id] = ch
	l.subMu.Unlock()
	return ch
}

func (l *lazyClient) Unsubscribe(id string) {
	l.subMu.Lock()
	defer l.subMu.Unlock()
	if ch, ok := l.subscribers[id]; ok {
		delete(l.subscribers, id)
		close(ch)
	}
}

func (l *lazyClient) Close() {
	l.mu.Lock()
	l.demand = make(map[string]struct{})
	l.mu.Unlock()
	l.teardown()

	l.subMu.Lock()
	defer l.subMu.Unlock()
	for id, ch := range l.subscribers {
		delete(l.subscribers, id)
		close(ch)
	}
}
