package messages

// Boat - generalized boat data
type Boat struct {
	Boat      string   `json:"Boat"`
	Type      string   `json:"Type"`
	Position  Vector2D `json:"Position"`
	Velocity  Vector2D `json:"Velocity"`
	Timestamp float32  `json:"Timestamp"`
}
