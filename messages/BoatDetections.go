package messages

// BoatDetections - boat information after analysis
type BoatDetections struct {
	Boat             Boat `json:"Boat"`
	CollisionWarning bool `json:"CollisionWarning"`
	Obliterated	     bool `json:"Obliterated"`
}
