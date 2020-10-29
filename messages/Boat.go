package messages

// Boat - generalized boat data
type Boat struct {
	Name      string     `json:"Name"`
	Type      string     `json:"Type"`
	Position  Vector2D   `json:"Position"`
	Velocity  Vector2D   `json:"Velocity"`
	Orientation float64  `json:"Orientation"`
	Timestamp float64    `json:"Timestamp"`
}
