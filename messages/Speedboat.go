package messages

// Speedboat data
type Speedboat struct {
	Name      string   `json:"Boat"`
	Position  Point2D `json:"Position"`
	Velocity  Velocity2D `json:"Velocity"`
	Timestamp float32  `json:"Timestamp"`
}
