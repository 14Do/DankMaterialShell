pragma Singleton
pragma ComponentBehavior: Bound

import QtQuick
import Quickshell

Singleton {
    id: root

    readonly property var log: Log.scoped("LocationService")

    readonly property bool locationAvailable: DMSService.isConnected && DMSService.capabilities.includes("location")
    readonly property bool valid: latitude !== 0 || longitude !== 0

    property var latitude: 0.0
    property var longitude: 0.0

    signal locationChanged(var data)

    onLocationAvailableChanged: {
        if (locationAvailable && !valid)
            getState();
    }

    Connections {
        target: DMSService

        function onLocationStateUpdate(data) {
            if (!locationAvailable)
                return;
            handleStateUpdate(data);
        }
    }

    function handleStateUpdate(data) {
        const lat = data.latitude;
        const lon = data.longitude;
        if (lat === 0 && lon === 0)
            return;

        root.latitude = lat;
        root.longitude = lon;
        root.locationChanged(data);
    }

    function getState() {
        if (!locationAvailable)
            return;

        DMSService.sendRequest("location.getState", null, response => {
            if (response.result)
                handleStateUpdate(response.result);
        });
    }

    // Tell the daemon whether a consumer wants location (demand-driven acquisition).
    function setAutoEnabled(enabled) {
        if (!locationAvailable)
            return;

        DMSService.sendRequest("location.setAutoEnabled", {
            "enabled": enabled
        }, response => {
            // Warn, never fail: an old daemon has no location.setAutoEnabled and
            // acquired eagerly anyway - version skew stays tolerated.
            if (response.error)
                log.warn("setAutoEnabled failed:", response.error);
            // The daemon responds only after acquisition, so this re-pull
            // deterministically sees the seeded fix (the initial pull raced ahead).
            if (enabled)
                getState();
        });
    }
}
