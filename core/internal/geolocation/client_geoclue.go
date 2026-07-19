package geolocation

import (
	"fmt"
	"sync"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/log"
	"github.com/AvengeMedia/dankgo/dbusutil"
	"github.com/godbus/dbus/v5"
)

const (
	dbusGeoClueService   = "org.freedesktop.GeoClue2"
	dbusGeoCluePath      = "/org/freedesktop/GeoClue2"
	dbusGeoClueInterface = dbusGeoClueService

	dbusGeoClueManagerPath      = dbusGeoCluePath + "/Manager"
	dbusGeoClueManagerInterface = dbusGeoClueInterface + ".Manager"
	dbusGeoClueManagerGetClient = dbusGeoClueManagerInterface + ".GetClient"

	dbusGeoClueClientInterface       = dbusGeoClueInterface + ".Client"
	dbusGeoClueClientDesktopId       = dbusGeoClueClientInterface + ".DesktopId"
	dbusGeoClueClientTimeThreshold   = dbusGeoClueClientInterface + ".TimeThreshold"
	dbusGeoClueClientTimeStart       = dbusGeoClueClientInterface + ".Start"
	dbusGeoClueClientTimeStop        = dbusGeoClueClientInterface + ".Stop"
	dbusGeoClueClientLocationUpdated = dbusGeoClueClientInterface + ".LocationUpdated"

	dbusGeoClueLocationInterface = dbusGeoClueInterface + ".Location"
	dbusGeoClueLocationLatitude  = dbusGeoClueLocationInterface + ".Latitude"
	dbusGeoClueLocationLongitude = dbusGeoClueLocationInterface + ".Longitude"
)

type GeoClueClient struct {
	currLocation  *Location
	locationMutex sync.RWMutex

	dbusConn   *dbus.Conn
	clientPath dbus.ObjectPath
	signals    chan *dbus.Signal

	stopChan chan struct{}
	sigWG    sync.WaitGroup

	// subMu excludes notifySubscribers' sends from Unsubscribe/Close closing a
	// channel mid-send - the forwarder unsubscribes on every demand teardown.
	subMu       sync.RWMutex
	subscribers map[string]chan Location
}

func newGeoClueClient() (*GeoClueClient, error) {
	dbusConn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, fmt.Errorf("system bus connection failed: %w", err)
	}

	c := &GeoClueClient{
		dbusConn:    dbusConn,
		stopChan:    make(chan struct{}),
		signals:     make(chan *dbus.Signal, 256),
		subscribers: make(map[string]chan Location),

		currLocation: &Location{
			Latitude:  0.0,
			Longitude: 0.0,
		},
	}

	if err := c.setupClient(); err != nil {
		dbusConn.Close()
		return nil, err
	}

	if err := c.startSignalPump(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *GeoClueClient) Close() {
	close(c.stopChan)

	c.sigWG.Wait()

	if c.signals != nil {
		c.dbusConn.RemoveSignal(c.signals)
		close(c.signals)
	}

	c.subMu.Lock()
	for id, ch := range c.subscribers {
		delete(c.subscribers, id)
		close(ch)
	}
	c.subMu.Unlock()

	if c.dbusConn != nil {
		c.dbusConn.Close()
	}
}

func (c *GeoClueClient) Subscribe(id string) chan Location {
	ch := make(chan Location, 64)
	c.subMu.Lock()
	c.subscribers[id] = ch
	c.subMu.Unlock()
	return ch
}

func (c *GeoClueClient) Unsubscribe(id string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if ch, ok := c.subscribers[id]; ok {
		delete(c.subscribers, id)
		close(ch)
	}
}

func (c *GeoClueClient) setupClient() error {
	managerObj := c.dbusConn.Object(dbusGeoClueService, dbusGeoClueManagerPath)

	if err := managerObj.Call(dbusGeoClueManagerGetClient, 0).Store(&c.clientPath); err != nil {
		return fmt.Errorf("failed to create GeoClue2 client: %w", err)
	}

	clientObj := c.dbusConn.Object(dbusGeoClueService, c.clientPath)
	if err := clientObj.SetProperty(dbusGeoClueClientDesktopId, "dms"); err != nil {
		return fmt.Errorf("failed to set desktop ID: %w", err)
	}

	if err := clientObj.SetProperty(dbusGeoClueClientTimeThreshold, uint(10)); err != nil {
		return fmt.Errorf("failed to set time threshold: %w", err)
	}

	return nil
}

func (c *GeoClueClient) startSignalPump() error {
	c.dbusConn.Signal(c.signals)

	if err := c.dbusConn.AddMatchSignal(
		dbus.WithMatchObjectPath(c.clientPath),
		dbus.WithMatchInterface(dbusGeoClueClientInterface),
		dbus.WithMatchMember("LocationUpdated"),
	); err != nil {
		return err
	}

	c.sigWG.Add(1)
	go func() {
		defer c.sigWG.Done()

		clientObj := c.dbusConn.Object(dbusGeoClueService, c.clientPath)
		clientObj.Call(dbusGeoClueClientTimeStart, 0)
		defer clientObj.Call(dbusGeoClueClientTimeStop, 0)

		for {
			select {
			case <-c.stopChan:
				return
			case sig, ok := <-c.signals:
				if !ok {
					return
				}
				if sig == nil {
					continue
				}

				c.handleSignal(sig)
			}
		}
	}()

	return nil
}

func (c *GeoClueClient) handleSignal(sig *dbus.Signal) {
	switch sig.Name {
	case dbusGeoClueClientLocationUpdated:
		if len(sig.Body) != 2 {
			return
		}

		newLocationPath, ok := sig.Body[1].(dbus.ObjectPath)
		if !ok {
			return
		}

		if err := c.handleLocationUpdated(newLocationPath); err != nil {
			log.Warn("GeoClue: Failed to handle location update: %v", err)
			return
		}
	}
}

func (c *GeoClueClient) handleLocationUpdated(path dbus.ObjectPath) error {
	locationObj := c.dbusConn.Object(dbusGeoClueService, path)

	lat, err := locationObj.GetProperty(dbusGeoClueLocationLatitude)
	if err != nil {
		return err
	}

	long, err := locationObj.GetProperty(dbusGeoClueLocationLongitude)
	if err != nil {
		return err
	}

	c.locationMutex.Lock()
	c.currLocation.Latitude = dbusutil.AsOr(lat, 0.0)
	c.currLocation.Longitude = dbusutil.AsOr(long, 0.0)
	c.locationMutex.Unlock()

	c.notifySubscribers()
	return nil
}

func (c *GeoClueClient) notifySubscribers() {
	currentLocation, err := c.GetLocation()
	if err != nil {
		return
	}

	c.subMu.RLock()
	defer c.subMu.RUnlock()
	for _, ch := range c.subscribers {
		select {
		case ch <- currentLocation:
		default:
			log.Warn("GeoClue: subscriber channel full, dropping update")
		}
	}
}

func (c *GeoClueClient) SeedLocation(loc Location) {
	c.locationMutex.Lock()
	defer c.locationMutex.Unlock()
	c.currLocation.Latitude = loc.Latitude
	c.currLocation.Longitude = loc.Longitude
}

func (c *GeoClueClient) GetLocation() (Location, error) {
	c.locationMutex.RLock()
	defer c.locationMutex.RUnlock()
	if c.currLocation == nil {
		return Location{
			Latitude:  0.0,
			Longitude: 0.0,
		}, nil
	}
	stateCopy := *c.currLocation
	return stateCopy, nil
}
