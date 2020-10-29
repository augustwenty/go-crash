package messages

// Sailboat data
type Sailboat struct {
	Name      string   `json:"Name"`
	Position  Vector2D  `json:"Position"`
	Timestamp float32  `json:"Timestamp"`
}
