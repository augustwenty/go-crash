package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-crash/messages"
	"math"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("Starting....")
	go SendBoatData(5, .1)
	fmt.Println("Boat generator started...")
	time.Sleep(10 * time.Minute)
}

// SendBoatData ...
func SendBoatData(numMessages int, dt float64) {
	speedboatConf := kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_speedboat_data",
		Balancer: &kafka.Hash{},
	}
	speedboatWriter := kafka.NewWriter(speedboatConf)

	sailboatConf := kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_sailboat_data",
		Balancer: &kafka.Hash{},
	}
	sailboatWriter := kafka.NewWriter(sailboatConf)

	speedboatNames := []string{"SS Hare", "The Flying Wasp", "Slice of Life", "Dont Crash Me"}
	sailboatNames := []string{"SS Turtle", "SS Windbag", "Slow n Steady", "Tow Me"}

	speedboatIC := GenerateRandomBoatInitialConditions(speedboatNames)
	sailboatIC := GenerateRandomBoatInitialConditions(sailboatNames)

	t := float64(0)

	speedboatMsgs := make([]kafka.Message, len(speedboatNames))
	sailboatMsgs := make([]kafka.Message, len(sailboatNames))

	for i := 0; i < numMessages; i++ {
		for i, boatIC := range speedboatIC {
			speedboatMsgValue, _ := json.Marshal(NewSpeedboatMessage(boatIC.Name, t, boatIC.Position, boatIC.Velocity))
			speedboatMsgs[i] = kafka.Message{
				Key:   []byte(boatIC.Name),
				Value: []byte(string(speedboatMsgValue)),
			}
			fmt.Println(string(speedboatMsgValue))
		}

		for i, boatIC := range sailboatIC {
			sailboatMsgValue, _ := json.Marshal(NewSailboatMessage(boatIC.Name, t, boatIC.Position, boatIC.Velocity))
			sailboatMsgs[i] = kafka.Message{
				Key:   []byte(boatIC.Name),
				Value: []byte(string(sailboatMsgValue)),
			}
			fmt.Println(string(sailboatMsgValue))
		}

		speedboatWriter.WriteMessages(context.Background(), speedboatMsgs...)
		sailboatWriter.WriteMessages(context.Background(), sailboatMsgs...)

		t = t + dt
	}

	if err := speedboatWriter.Close(); err != nil {
		fmt.Println("Couldn't close speedboat writer")
	}

	if err := sailboatWriter.Close(); err != nil {
		fmt.Println("Couldn't close speedboat writer")
	}
}

// GenerateRandomVector2D ...
func GenerateRandomVector2D(maxMagnitude float64) messages.Vector2D {
	maxFactor := maxMagnitude / float64(math.Sqrt(2))
	return messages.Vector2D{X: maxFactor * rand.Float64(), Y: maxFactor * rand.Float64()}
}

// GenerateRandomBoatInitialConditions ...
func GenerateRandomBoatInitialConditions(boatNames []string) []messages.Boat {
	boatsIC := make([]messages.Boat, len(boatNames))
	for i, name := range boatNames {
		r0 := GenerateRandomVector2D(1.0)
		v0 := GenerateRandomVector2D(1.0)
		boatsIC[i] = messages.Boat{Name: name, Position: r0, Velocity: v0}
	}

	return boatsIC
}

// NewSpeedboatMessage ...
func NewSpeedboatMessage(name string, t float64, r0 messages.Vector2D, v0 messages.Vector2D) messages.Speedboat {
	position := GetBoatPosition(t, r0, v0)
	return messages.Speedboat{Name: name, Position: position, Velocity: v0, Timestamp: t}
}

// NewSailboatMessage ...
func NewSailboatMessage(name string, t float64, r0 messages.Vector2D, v0 messages.Vector2D) messages.Sailboat {
	position := GetBoatPosition(t, r0, v0)
	return messages.Sailboat{Name: name, Position: position, Timestamp: t}
}

// GetBoatPosition ...
func GetBoatPosition(t float64, r0 messages.Vector2D, v0 messages.Vector2D) messages.Vector2D {
	return messages.Vector2D{X: r0.X + v0.X*t, Y: r0.Y + v0.Y*t}
}
