package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/augustwenty/go-crash/messages"
	"github.com/augustwenty/go-crash/common"
	"encoding/json"
)

func main() {

	fmt.Println("Starting sailboat transformer....")

	cancellationContext, signalShutdownComplete := common.InitCloseHandler()
	
	rxQueue := make(chan messages.Sailboat)
	xmtQueue := make(chan messages.Boat)
	
	go readSailboats(cancellationContext, rxQueue)
	go transform(rxQueue, xmtQueue)
	go sendGeneralizedBoatData(xmtQueue, signalShutdownComplete)

	common.WaitForShutdownComplete()
}


func readSailboats(cancellationContext context.Context, rxQueue chan messages.Sailboat) {
	sailboatReaderConf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_sailboat_data",
		GroupID:  "g3",
		MaxBytes: 1024,
	}

	sailboatReader := kafka.NewReader(sailboatReaderConf)

	for {
		fmt.Println("Waiting to receive message...")
		msg, err := sailboatReader.ReadMessage(cancellationContext)
		
		if err == context.Canceled {
			fmt.Println("Shutting down, reading cancelled...")
			break;
		} else if err != nil {
			fmt.Printf("ERROR: an error occurred reading messages -> %v\n", err)
		} else {
			fmt.Printf("Receiving message %v\n", string(msg.Key))

			var sailBoat messages.Sailboat
			err := json.Unmarshal(msg.Value, &sailBoat)
			if err != nil {
				fmt.Printf("ERROR: an error occured interpreting messsage date -> %v\n", err)
			} else {
				rxQueue <- sailBoat
			}
		}
	}

	fmt.Println("Closing reader and receive queue...")

	if err := sailboatReader.Close(); err != nil {
		fmt.Println("Couldn't close sailboat reader...")
	}

	close(rxQueue)
}

func transform(rxQueue chan messages.Sailboat, xmtQueue chan messages.Boat) {

	sailboatHistory := make(map[string]messages.Sailboat)

	for latest := range rxQueue {
		fmt.Println("Processing message for ", latest.Name)
		lastKnown, found := sailboatHistory[latest.Name]
		if found {
			fmt.Printf("Transforming message for %v...\n", latest.Name)
			t0 := lastKnown.Timestamp
			t1 := latest.Timestamp
			p0 := lastKnown.Position
			p1 := latest.Position
			velocity := messages.AverageRateOfChange(p0, p1, t0, t1)

			boat := messages.Boat{
				Name: latest.Name,
				Type: "sailboat",
				Position: latest.Position,
				Velocity: velocity,
				Orientation: messages.CalculateOrientation(velocity),
				Timestamp: latest.Timestamp,
			}

			xmtQueue <- boat
		} else {
			fmt.Printf("Adding %v...\n", latest.Name)
			sailboatHistory[latest.Name] = latest
		}
	}

	fmt.Println("Stopping transformations.  Closing transmit queue...")
	close(xmtQueue)
}

func sendGeneralizedBoatData(xmtQueue chan messages.Boat, signalShutdownComplete context.CancelFunc) {
	boatWriterConf := kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "boat_data",
		Balancer: &kafka.Hash{},
	}
	boatWriter := kafka.NewWriter(boatWriterConf)
	boatMessages := make([]kafka.Message, 1)

	for boat := range xmtQueue {
		fmt.Printf("Sending general boat data for %v...\n", boat.Name)
		payload, _ := json.Marshal(boat)
		boatMessages[0] = kafka.Message {
			Key:   []byte(boat.Name),
			Value: []byte(string(payload)),
		}
		err := boatWriter.WriteMessages(context.Background(), boatMessages...)

		if err != nil {
			fmt.Println("ERROR: Sending boat message -> ", err)
		} else {
			fmt.Println("Message sent -> ", string(payload))
		}
	}

	fmt.Println("Stopping transmission...")
	signalShutdownComplete()
}
