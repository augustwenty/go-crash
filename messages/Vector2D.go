package messages

import (
	"math"
)

// Vector2D ...
type Vector2D struct {
	Vx float64 `json:"dx"`
	Vy float64 `json:"dy"`
	Magnitude float64 `json:"magnitude"`
	Theta float64 `json:"theta"`
}

// CreateVector - creates a vector given starting and ending points and time
func CreateVector(p0 Point2D, p1 Point2D) Vector2D {

	dx := p1.X - p0.X
	dy := p1.Y - p0.Y

	return Vector2D {
		Vx: dx,
		Vy: dy,
		Magnitude: math.Sqrt((dx * dx) + (dy * dy)),
		Theta: math.Atan2(dy, dx),
	}
}