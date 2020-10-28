package messages

// Sailboat data
type Sailboat struct {
	Boat      string   `json:"Boat"`
	Position  Vector2D `json:"Position"`
	Timestamp float32  `json:"Timestamp"`
}
