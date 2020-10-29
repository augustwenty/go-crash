package messages

import (
	"math"
)

// Vector2D ...
type Vector2D struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// VectorDifference - determines difference between two vectors
func VectorDifference(v0 Vector2D, v1 Vector2D) Vector2D {

	dx := v1.X - v0.X
	dy := v1.Y - v0.Y

	return Vector2D {
		X: dx,
		Y: dy,
	}
}

// AverageRateOfChange - calculates velocity between two vectors
func AverageRateOfChange(v0 Vector2D, v1 Vector2D, t0 float64, t1 float64) Vector2D {
	
	velocity := Vector2D {
		X: math.NaN(),
		Y: math.NaN(),
	}

	dt := t1 - t0
	if dt != 0 {
		velocity := VectorDifference(v0, v1)
		velocity.X /= dt
		velocity.Y /= dt
	}

	return velocity
}