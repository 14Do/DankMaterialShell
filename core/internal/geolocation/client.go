package geolocation

import "github.com/AvengeMedia/DankMaterialShell/core/internal/log"

// NewClient returns an idle facade: no network egress happens until a consumer
// calls Acquire (see DemandController).
func NewClient() Client {
	return newLazyClient()
}

// acquireClient builds the real client: GeoClue2 seeded once from IP for an
// immediate fix, or IP-only when GeoClue2 is unavailable.
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
