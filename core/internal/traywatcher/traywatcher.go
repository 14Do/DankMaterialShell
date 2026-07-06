// Package traywatcher implements a minimal org.kde.StatusNotifierWatcher that
// claims the SNI name early in the session so autostart tray apps can register
// before the shell finishes loading. See docs/TRAY_WATCHER.md.
package traywatcher

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/log"
	"github.com/godbus/dbus/v5"
	"github.com/godbus/dbus/v5/introspect"
	"github.com/godbus/dbus/v5/prop"
)

const (
	busName = "org.kde.StatusNotifierWatcher"
	objPath = dbus.ObjectPath("/StatusNotifierWatcher")
	iface   = "org.kde.StatusNotifierWatcher"
)

const introXML = `<node>
  <interface name="org.kde.StatusNotifierWatcher">
    <method name="RegisterStatusNotifierItem">
      <arg type="s" direction="in" name="service"/>
    </method>
    <method name="RegisterStatusNotifierHost">
      <arg type="s" direction="in" name="service"/>
    </method>
    <property name="RegisteredStatusNotifierItems" type="as" access="read"/>
    <property name="IsStatusNotifierHostRegistered" type="b" access="read"/>
    <property name="ProtocolVersion" type="i" access="read"/>
    <signal name="StatusNotifierItemRegistered">
      <arg type="s" name="service"/>
    </signal>
    <signal name="StatusNotifierItemUnregistered">
      <arg type="s" name="service"/>
    </signal>
    <signal name="StatusNotifierHostRegistered"/>
    <signal name="StatusNotifierHostUnregistered"/>
  </interface>
` + introspect.IntrospectDataString + prop.IntrospectDataString + `</node>`

type watcher struct {
	conn  *dbus.Conn
	props *prop.Properties
	mu    sync.Mutex
	items map[string]string // item id ("name/path") -> bus name to watch
	hosts map[string]bool
}

func (w *watcher) itemListLocked() []string {
	list := make([]string, 0, len(w.items))
	for item := range w.items {
		list = append(list, item)
	}
	sort.Strings(list)
	return list
}

func (w *watcher) emit(signal string, args ...any) {
	if err := w.conn.Emit(objPath, iface+"."+signal, args...); err != nil {
		log.Warnf("failed to emit %s: %v", signal, err)
	}
}

// Clients pass either an object path (item on the caller) or a bus name.
func (w *watcher) RegisterStatusNotifierItem(sender dbus.Sender, service string) *dbus.Error {
	var item, watch string
	if strings.HasPrefix(service, "/") {
		item, watch = string(sender)+service, string(sender)
	} else {
		item, watch = service+"/StatusNotifierItem", service
	}

	w.mu.Lock()
	if _, exists := w.items[item]; exists {
		w.mu.Unlock()
		return nil
	}
	w.items[item] = watch
	list := w.itemListLocked()
	w.mu.Unlock()

	log.Infof("item registered: %s", item)
	w.emit("StatusNotifierItemRegistered", item)
	w.props.SetMust(iface, "RegisteredStatusNotifierItems", list)
	return nil
}

func (w *watcher) RegisterStatusNotifierHost(sender dbus.Sender, service string) *dbus.Error {
	watch := string(sender)
	if service != "" && !strings.HasPrefix(service, "/") {
		watch = service
	}

	w.mu.Lock()
	if w.hosts[watch] {
		w.mu.Unlock()
		return nil
	}
	w.hosts[watch] = true
	w.mu.Unlock()

	log.Infof("host registered: %s", watch)
	w.emit("StatusNotifierHostRegistered")
	return nil
}

func (w *watcher) pruneOwner(name string) {
	w.mu.Lock()
	var removed []string
	for item, watch := range w.items {
		if watch == name {
			delete(w.items, item)
			removed = append(removed, item)
		}
	}
	list := w.itemListLocked()
	hostGone := w.hosts[name]
	delete(w.hosts, name)
	w.mu.Unlock()

	for _, item := range removed {
		log.Infof("item gone: %s", item)
		w.emit("StatusNotifierItemUnregistered", item)
	}
	if len(removed) > 0 {
		w.props.SetMust(iface, "RegisteredStatusNotifierItems", list)
	}
	if hostGone {
		log.Infof("host gone: %s", name)
		w.emit("StatusNotifierHostUnregistered")
	}
}

// Run serves the watcher until the session bus connection closes.
func Run() error {
	conn, err := dbus.ConnectSessionBus()
	if err != nil {
		return fmt.Errorf("connect to session bus: %w", err)
	}
	defer conn.Close()

	w := &watcher{conn: conn, items: map[string]string{}, hosts: map[string]bool{}}

	if err := conn.Export(w, objPath, iface); err != nil {
		return fmt.Errorf("export watcher: %w", err)
	}
	w.props, err = prop.Export(conn, objPath, map[string]map[string]*prop.Prop{
		iface: {
			"RegisteredStatusNotifierItems": {Value: []string{}, Emit: prop.EmitTrue},
			// Always true: libappindicator clients drop the icon if it is false.
			"IsStatusNotifierHostRegistered": {Value: true, Emit: prop.EmitFalse},
			"ProtocolVersion":                {Value: int32(0), Emit: prop.EmitFalse},
		},
	})
	if err != nil {
		return fmt.Errorf("export properties: %w", err)
	}
	if err := conn.Export(introspect.Introspectable(introXML), objPath, "org.freedesktop.DBus.Introspectable"); err != nil {
		return fmt.Errorf("export introspection: %w", err)
	}

	if err := conn.AddMatchSignal(
		dbus.WithMatchSender("org.freedesktop.DBus"),
		dbus.WithMatchInterface("org.freedesktop.DBus"),
		dbus.WithMatchMember("NameOwnerChanged"),
		dbus.WithMatchObjectPath("/org/freedesktop/DBus"),
	); err != nil {
		return fmt.Errorf("match NameOwnerChanged: %w", err)
	}
	sigs := make(chan *dbus.Signal, 128)
	conn.Signal(sigs)

	// No flags: queue for the name and take it when the current owner exits.
	reply, err := conn.RequestName(busName, 0)
	if err != nil {
		return fmt.Errorf("request %s: %w", busName, err)
	}
	if reply == dbus.RequestNameReplyInQueue {
		log.Infof("name busy, queued for %s", busName)
	}

	for sig := range sigs {
		switch sig.Name {
		case "org.freedesktop.DBus.NameAcquired":
			if len(sig.Body) == 1 && sig.Body[0] == busName {
				log.Infof("acquired %s", busName)
			}
		case "org.freedesktop.DBus.NameOwnerChanged":
			if len(sig.Body) != 3 {
				continue
			}
			name, _ := sig.Body[0].(string)
			newOwner, _ := sig.Body[2].(string)
			if name != busName && newOwner == "" {
				w.pruneOwner(name)
			}
		}
	}
	return fmt.Errorf("session bus connection closed")
}
