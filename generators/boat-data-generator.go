package main

import (
	// "context"
	"fmt"
	"time"
	"math/rand"
	"math"
	"github.com/segmentio/kafka-go"
	"github.com/augustwenty/go-crash/messages"
	"encoding/json"
)
 

func main() {
	fmt.Println("Starting....")

	//sp1 := NewSpeedboatMessage(0, messages.Vector2D{X:1.1, Y:1.5} , messages.Vector2D{X: 1.1, Y:1.2})
	// go SendDummyData()
	go SendBoatData(5, .1)
	bnames := []string{"hi", "bye", "cye"}
	for _, val := range bnames {
		fmt.Println(val);
		fmt.Println(rand.Float32())
	}
	fmt.Println("Boat generator started...")
	time.Sleep(100 * time.Second)
}

// SendBoatData ...
func SendBoatData(numMessages int, dt float32) {
	speedboatNames := []string{"SS Hare", "The Flying Wasp", "Slice of Life", "Dont Crash Me"}
	sailboatNames := []string{"SS Slow", "SS Windbag", "Slow n Steady", "Tow Me"}

	speedboatIC := GenerateRandomBoatInitialConditions(speedboatNames)
	sailboatIC := GenerateRandomBoatInitialConditions(sailboatNames)

	t := float32(0)
	
	speedboatMsgs := make([]kafka.Message, len(speedboatNames))
	sailboatMsgs := make([]kafka.Message, len(sailboatNames))

	for i:=0; i < numMessages; i++ {
		for i, boatIC := range speedboatIC {
			speedboatMsgValue, _ := json.Marshal(NewSpeedboatMessage(boatIC.Name, t, boatIC.Position, boatIC.Velocity))
			speedboatMsgs[i] = kafka.Message{
				Key: []byte(boatIC.Name),
				Value: []byte(string(speedboatMsgValue)),
			}
			fmt.Println(string(speedboatMsgValue))
		}

		for i, boatIC := range sailboatIC {
			sailboatMsgValue, _ := json.Marshal(NewSailboatMessage(boatIC.Name, t, boatIC.Position, boatIC.Velocity))
			sailboatMsgs[i] = kafka.Message{
				Key: []byte(boatIC.Name),
				Value: []byte(string(sailboatMsgValue)),
			}
			fmt.Println(string(sailboatMsgValue))
		}

		t = t+dt
	}
}
// GenerateRandomVector2D ...
func GenerateRandomVector2D(maxMagnitude float32) messages.Vector2D {
	maxFactor := maxMagnitude/float32(math.Sqrt(2))
	return messages.Vector2D{X: maxFactor*rand.Float32(), Y: maxFactor*rand.Float32()}
}

// GenerateRandomBoatInitialConditions ...
func GenerateRandomBoatInitialConditions(boatNames []string) []messages.Boat {
	boatsIC := make([]messages.Boat, len(boatNames))
	for i, name := range boatNames {
		r0 := GenerateRandomVector2D(1.0)
		v0 := GenerateRandomVector2D(1.0)
		boatsIC[i] = messages.Boat{Name: name, Position: r0, Velocity: v0}
	}

	return boatsIC;
}

// NewSpeedboatMessage ...
func NewSpeedboatMessage(name string, t float32, r0 messages.Vector2D, v0 messages.Vector2D) messages.Speedboat {
	position := GetBoatPosition(t, r0, v0)
	return messages.Speedboat{Name: name, Position: position, Velocity: v0, Timestamp: t}
}

// NewSailboatMessage ...
func NewSailboatMessage(name string, t float32, r0 messages.Vector2D, v0 messages.Vector2D) messages.Sailboat {
	position := GetBoatPosition(t, r0, v0)
	return messages.Sailboat{Name: name, Position: position, Timestamp: t}
}

// GetBoatPosition ...
func GetBoatPosition(t float32, r0 messages.Vector2D, v0 messages.Vector2D) messages.Vector2D {
	return messages.Vector2D{X: r0.X + v0.X*t, Y: r0.Y + v0.Y*t}
}

// func SendDummyData() {
// 	wConf := kafka.WriterConfig{
// 		Brokers:  []string{"localhost:9092"},
// 		Topic:    "first_topic",
// 		Balancer: &kafka.Hash{},
// 	}
// 	writer := kafka.NewWriter(wConf)
// 	boats := []string{"a", "b", "c", "d"}
// 	boatIndex := 0
// 	x := 0
// 	y := 0
// 	for i := 0; i < 100; i++ {
// 		boatName := boats[boatIndex]
// 		coordinates := fmt.Sprintf("{\"x\": %v, \"y\": %v }", x, y)
// 		err := writer.WriteMessages(context.Background(),
// 			kafka.Message{
// 				Key:   []byte(boatName),
// 				Value: []byte(coordinates),
// 			},
// 		)
// 		if err != nil {
// 			fmt.Println("Yo major failure!")
// 		}
// 		fmt.Println(fmt.Sprintf("Name: %v, JSON: '%v'", boatName, coordinates))
// 		x++
// 		y++
// 		boatIndex++
// 		boatIndex %= len(boats)
// 	}
// 	fmt.Println("Closing writer")
// 	if err := writer.Close(); err != nil {
// 		fmt.Println("Couldn't closer the writer")
// 	}
// }