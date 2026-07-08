pragma Singleton
pragma ComponentBehavior: Bound

import Quickshell
import QtQuick
import qs.Common
import qs.Services

// Accent color extracted from the current track's album art via ColorQuantizer,
// falling back to Theme.primary when no usable accent is available.
Singleton {
    id: root

    readonly property bool hasAccent: _accent !== null
    readonly property color accent: _accent !== null ? _accent : Theme.primary

    readonly property color onAccent: {
        const c = accent;
        const lum = 0.2126 * c.r + 0.7152 * c.g + 0.0722 * c.b;
        return lum > 0.6 ? Qt.rgba(0, 0, 0, 1) : Qt.rgba(1, 1, 1, 1);
    }

    readonly property color accentHover: Theme.withAlpha(accent, 0.12)
    readonly property color accentPressed: Theme.withAlpha(accent, Theme.transparentBlurLayers ? 0.24 : 0.16)

    readonly property color accentTrack: Theme.withAlpha(accent, 0.28)
    readonly property color accentSubtle: Theme.withAlpha(accent, 0.55)

    readonly property string artUrl: TrackArtService.resolvedArtUrl

    // Hold the last accent across the brief artUrl blank between tracks; never reset to primary.
    property var _accent: null

    ColorQuantizer {
        id: quantizer
        source: root.artUrl
        depth: 4
        rescaleSize: 64
        onColorsChanged: {
            const a = root._pickAccent(colors);
            if (a !== null)
                root._accent = a;
        }
    }

    function _pickAccent(colors) {
        if (!colors || colors.length === 0)
            return null;

        let best = null;
        let bestScore = -1;
        for (let i = 0; i < colors.length; i++) {
            const c = colors[i];
            const s = c.hsvSaturation;
            const v = c.hsvValue;
            if (v < 0.22 || v > 0.96 || s < 0.22)
                continue;
            const score = s * (1 - Math.abs(v - 0.68));
            if (score > bestScore) {
                bestScore = score;
                best = c;
            }
        }

        if (!best)
            return null;
        return _normalize(best);
    }

    function _normalize(c) {
        const hue = c.hsvHue < 0 ? 0 : c.hsvHue;
        const s = Math.min(1, c.hsvSaturation * 1.05);
        const v = Math.max(c.hsvValue, 0.62);
        return Qt.hsva(hue, s, v, 1);
    }
}
