package messages

// BoatAnalysis - boat information after analysis
type BoatAnalysis struct {
	Boat             Boat `json:"Boat"`
	CollisionWarning bool `json:"CollisionWarning"`
}
