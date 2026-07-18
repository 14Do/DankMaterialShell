package geolocation

import "github.com/AvengeMedia/DankMaterialShell/core/internal/log"

// NewClient returns an idle, demand-driven location client. It performs NO network
// egress until a consumer calls Acquire (see DemandController). This replaces the
// old eager behaviour that started GeoClue2 and seeded a fix from http://ip-api.com
// on every daemon start, regardless of user settings.
func NewClient() Client {
	return newLazyClient()
}

// acquireClient builds a real, active location client on demand: GeoClue2 when
// available (seeded once from IP to get an immediate fix), falling back to an
// IP-only client otherwise. Extracted verbatim from the old eager NewClient - the
// fallback provider is unchanged and tracked separately.
func acquireClient() Client {
	geoclueClient, err := newGeoClueClient()
	if err != nil {
		log.Warnf("GeoClue2 unavailable: %v", err)
		return newSeededIpClient()
	}

	loc, _ := geoclueClient.GetLocation()
	if loc.Latitude != 0 || loc.Longitude != 0 {
		log.Info("Using GeoClue2 location")
		return geoclueClient
	}

	log.Info("GeoClue2 has no fix yet, seeding with IP location")
	ipLoc, err := fetchIPLocation()
	if err != nil {
		log.Warnf("IP location seed failed: %v", err)
		return geoclueClient
	}

	log.Info("Seeded GeoClue2 with IP location")
	geoclueClient.SeedLocation(Location{Latitude: ipLoc.Latitude, Longitude: ipLoc.Longitude})
	return geoclueClient
}

func newSeededIpClient() *IpClient {
	client := newIpClient()
	ipLoc, err := fetchIPLocation()
	if err != nil {
		log.Warnf("IP location also failed: %v", err)
		return client
	}

	log.Info("Using IP location")
	client.currLocation.Latitude = ipLoc.Latitude
	client.currLocation.Longitude = ipLoc.Longitude
	return client
}
