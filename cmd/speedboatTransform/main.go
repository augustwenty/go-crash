package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-crash/common"
	"go-crash/messages"

	"github.com/segmentio/kafka-go"
)

func main() {

	fmt.Println("Starting speedboat transformer....")

	cancellationContext, signalShutdownComplete := common.InitCloseHandler()

	rxQueue := make(chan messages.Speedboat)
	xmtQueue := make(chan messages.Boat)

	go readSpeedboats(cancellationContext, rxQueue)
	go transform(rxQueue, xmtQueue)
	go sendGeneralizedBoatData(xmtQueue, signalShutdownComplete)

	common.WaitForShutdownComplete()
}

func readSpeedboats(cancellationContext context.Context, rxQueue chan messages.Speedboat) {
	speedboatReaderConf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_speedboat_data",
		GroupID:  "g3",
		MaxBytes: 1024,
	}

	speedboatReader := kafka.NewReader(speedboatReaderConf)

	for {
		// fmt.Println("Waiting to receive message...")
		msg, err := speedboatReader.ReadMessage(cancellationContext)

		if err == context.Canceled {
			fmt.Println("Shutting down, reading cancelled...")
			break
		} else if err != nil {
			fmt.Printf("ERROR: an error occurred reading messages -> %v\n", err)
		} else {
			// fmt.Printf("Receiving message %v\n", string(msg.Key))

			var speedBoat messages.Speedboat
			err := json.Unmarshal(msg.Value, &speedBoat)
			if err != nil {
				fmt.Printf("ERROR: an error occured interpreting messsage date -> %v\n", err)
			} else {
				rxQueue <- speedBoat
			}
		}
	}

	fmt.Println("Closing reader and receive queue...")
	if err := speedboatReader.Close(); err != nil {
		fmt.Println("Couldn't close speedboat reader...")
	}

	close(rxQueue)
}

func transform(rxQueue chan messages.Speedboat, xmtQueue chan messages.Boat) {

	for latest := range rxQueue {
		boat := messages.Boat{
			Name:        latest.Name,
			Type:        "speedboat",
			Position:    latest.Position,
			Velocity:    latest.Velocity,
			Orientation: messages.CalculateOrientation(latest.Velocity),
			Timestamp:   latest.Timestamp,
		}

		xmtQueue <- boat
	}

	fmt.Println("Stopping transformations.  Closing transmit queue...")
	close(xmtQueue)
}

func sendGeneralizedBoatData(xmtQueue chan messages.Boat, signalShutdownComplete context.CancelFunc) {
	boatWriterConf := kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "boat_data",
		Balancer: &kafka.Hash{},
		Async:    true,
	}
	boatWriter := kafka.NewWriter(boatWriterConf)
	boatMessages := make([]kafka.Message, 1)

	for boat := range xmtQueue {
		fmt.Printf("Ahoy! From the %v %v\n", boat.Type, boat.Name)
		payload, _ := json.Marshal(boat)
		boatMessages[0] = kafka.Message{
			Key:   []byte(boat.Name),
			Value: []byte(string(payload)),
		}

		boatWriter.WriteMessages(context.Background(), boatMessages...)
		// err := boatWriter.WriteMessages(context.Background(), boatMessages...)
		// if err != nil {
		// 	fmt.Println("ERROR: Sending boat message -> ", err)
		// } else {
		//fmt.Println("Message sent -> ", string(payload))
		// }
	}

	fmt.Println("Stopping transmission...")
	signalShutdownComplete()
}
