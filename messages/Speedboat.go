package messages

// Speedboat data
type Speedboat struct {
	Name      string   `json:"Boat"`
	Position  Vector2D `json:"Position"`
	Velocity  Vector2D `json:"Velocity"`
	Timestamp float32  `json:"Timestamp"`
}
