package messages

// Sailboat data
type Sailboat struct {
	Name      string   `json:"Boat"`
	Position  Point2D  `json:"Position"`
	Timestamp float32  `json:"Timestamp"`
}
