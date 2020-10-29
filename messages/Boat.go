package messages

// Boat - generalized boat data
type Boat struct {
	Name      string   `json:"Boat"`
	Type      string   `json:"Type"`
	Position  Point2D  `json:"Position"`
	Velocity  Velocity2D `json:"Velocity"`
	Timestamp float32  `json:"Timestamp"`
}
