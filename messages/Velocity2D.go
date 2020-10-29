package messages

import (
	"math"
)

// Velocity2D ...
type Velocity2D struct {
	Victor Vector2D `json:"victor"`
}

// CreateVelocity2D - creates a vector given starting and ending points and time
func CreateVelocity2D(p0 Point2D, p1 Point2D, t0 float64, t1 float64) Velocity2D {
	
	velocity := Velocity2D {
		Vector2D{
			Vx: 0,
			Vy: 0,
			Magnitude: 0,
			Theta: 0,		
		},
	}

	dt := t1 - t0
	if dt != 0 {
		dx := (p1.X - p0.X) / dt
		dy := (p1.Y - p0.Y) / dt
	
		velocity = Velocity2D {
			Vector2D{
				Vx: dx,
				Vy: dy,
				Magnitude: math.Sqrt((dx * dx) + (dy * dy)),
				Theta: math.Atan2(dy, dx),
			},
		}	
	}

	return velocity
}