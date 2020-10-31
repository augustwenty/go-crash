package messages

import (
	"math"
)

// Vector2D ...
type Vector2D struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Subtract - Vector subtraction
func (u Vector2D) Subtract(v Vector2D) Vector2D {
	return Vector2D {
		X: u.X - v.X,
		Y: u.Y - v.Y,
	}
}

// Add - Vector addition
func (u Vector2D) Add(v Vector2D) Vector2D {
	return Vector2D {
		X: u.X + v.X,
		Y: u.Y + v.Y,
	}
}

// Multiply - scalar multiplication
func (u Vector2D) Multiply(c float64) Vector2D {
	return Vector2D {
		X: c*u.X,
		Y: c*u.Y,
	}
}

// Dot - Dot product
func (u Vector2D) Dot(v Vector2D) float64 {
	return u.X*v.X + u.Y*v.Y
}

// Magnitude - Vector magnitude
func (u Vector2D) Magnitude() float64 {
	return math.Sqrt(u.Dot(u))
}

// Unit - Unit vector of a vector
func (u Vector2D) Unit() Vector2D {
	x, y := math.NaN(), math.NaN()
	magnitude := u.Magnitude()
	if magnitude > 0 {
		x, y = u.X/magnitude, u.Y/magnitude
	}
	return Vector2D{X: x, Y: y}
}

// Equals - Are two vectors equal?
func (u Vector2D) Equals(v Vector2D, threshold float64) bool {
	return u.Subtract(v).Magnitude() < threshold
}

// Cross - 2D 'Cross product'
func (u Vector2D) Cross(v Vector2D) float64 {
	return u.X*v.Y - v.X*u.Y
}

// AverageRateOfChange - calculates velocity between two vectors
func AverageRateOfChange(v0 Vector2D, v1 Vector2D, t0 float64, t1 float64) Vector2D {
	
	velocity := Vector2D {
		X: math.NaN(),
		Y: math.NaN(),
	}

	dt := t1 - t0

	if dt != 0 {
		dv := v1.Subtract(v0)
		velocity = dv.Multiply(1/dt)
	}

	return velocity
}

// CalculateOrientation - calculates angle/orientation
func CalculateOrientation(v0 Vector2D) float64 {
	return math.Atan2(v0.Y, v0.X)
}