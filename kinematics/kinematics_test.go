package kinematics

import (
	"math"
	"go-crash/messages"
	"testing"
	"fmt"
)

// TestCrossingTimes - simple intersection test
func TestCrossingTimes(t *testing.T) {
	r1 := messages.Vector2D{X:0, Y:0}
	v1 := messages.Vector2D{X:2, Y:2}

	r2 := messages.Vector2D{X:1, Y:0}
	v2 := messages.Vector2D{X:0, Y:1}

	t1, t2 := FindCrossingTimes(r1, v1, r2, v2)
	fmt.Println(t1, t2)

	if t1 != 0.5 || t2 != 1 {
		t.Fail()
	}
}

func TestCollinearProperChaseCase(t *testing.T) {
	r1 := messages.Vector2D{X:0, Y:0}
	v1 := messages.Vector2D{X:2, Y:2}

	r2 := messages.Vector2D{X: 1, Y:1}
	v2 := messages.Vector2D{X: 1, Y:1}

	t1, t2 := FindCrossingTimes(r1, v1, r2, v2)

	if t1 != 1 || t2 != 1 {
		t.Fail()
	}
}

func TestCollinearHeadOnCase(t *testing.T) {
	r1 := messages.Vector2D{X:0, Y:0}
	v1 := messages.Vector2D{X:1, Y:1}

	r2 := messages.Vector2D{X: 1, Y:1}
	v2 := messages.Vector2D{X: -1, Y:-1}

	t1, t2 := FindCrossingTimes(r1, v1, r2, v2)

	if t1 != 0.5 || t2 != 0.5 {
		t.Fail()
	}
}

func TestCollinearCantCatchMeCase(t *testing.T) {
	r1 := messages.Vector2D{X:0, Y:0}
	v1 := messages.Vector2D{X:2, Y:2}

	r2 := messages.Vector2D{X: 1, Y:1}
	v2 := messages.Vector2D{X: 5, Y:5}

	t1, t2 := FindCrossingTimes(r1, v1, r2, v2)

	if !(math.IsNaN(t1) && math.IsNaN(t2))  {
		t.Fail()
	}
}
