package kinematics

import (
	"go-crash/messages"
	"math"
	"fmt"
)

// DistanceToLine distance from r0 to line r1+v1*t
func DistanceToLine(r0 messages.Vector2D, r1 messages.Vector2D, v1 messages.Vector2D) float64 {
	hyp := r0.Subtract(r1)
	d := math.Abs(hyp.Cross(v1))*(v1.Magnitude())
	fmt.Println(d)
	return d
}

// LinearPosition - Position after time t elapsed of object traversing linear path with initial position r0/initial velocity v0
func LinearPosition(t float64, r0 messages.Vector2D, v0 messages.Vector2D) messages.Vector2D {
	return r0.Add(v0.Multiply(t))
}

// FindCrossingTimes - Find times where each object reaches interesction point of the two paths
func FindCrossingTimes(r1 messages.Vector2D, v1 messages.Vector2D, r2 messages.Vector2D, v2 messages.Vector2D) (float64, float64) {
	t1, t2 := math.NaN(), math.NaN()
	eps := 1e-10
	if v1.Unit().Equals(v2.Unit(), eps) && (DistanceToLine(r1, r2, v2) > eps) {
		fmt.Println(DistanceToLine(r1, r2, v2))
		return t1, t2
	}

	if (v2.Subtract(v1).Dot(v1) < 0 ) && (DistanceToLine(r1, r2, v2) < eps) {
		t12 := (r1.X-r2.X) / (v2.X-v1.X)
		return t12, t12
	}

	if v1.X == 0 {
		t2 = (r1.X - r2.X)/v2.X
		t1 = (r2.Y + v2.Y*t2)/v1.Y
		return t1, t2
	}

	t2 = ((r1.Y - r2.Y)*v1.X + (r2.X-r1.X)*v1.Y) / (v1.X*v2.Y - v1.Y*v2.X)
	t1 = (r2.X + v2.X*t2 - r1.X) / v1.X
	return t1, t2
}

// ObjectsTooClose - objects are too close
func ObjectsTooClose(t float64, r1 messages.Vector2D, v1 messages.Vector2D, r2 messages.Vector2D, v2 messages.Vector2D, 
	minDist float64, alertDist float64) bool {

	t1, t2 := FindCrossingTimes(r1, v1, r2, v2)
	if t1 == math.NaN() || t2 == math.NaN() {
		return false
	}

	intersection := LinearPosition(t1, r1, v1)
	otherPosition := LinearPosition(t1, r2, v2)
	return intersection.Subtract(otherPosition).Magnitude() < minDist && LinearPosition(t, r1, v1).Subtract(intersection).Magnitude() < alertDist
}