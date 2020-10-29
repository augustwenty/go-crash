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

	fmt.Println("Starting velocity transformer....")

	cancellationContext, signalShutdownComplete := common.InitCloseHandler()
	
	rxQueue := make(chan messages.Sailboat)
	xmtQueue := make(chan messages.Boat)
	
	go readSailboats(cancellationContext, rxQueue)
	go transform(rxQueue, xmtQueue)
	go sendGeneralizedBoatData(cancellationContext, xmtQueue, signalShutdownComplete)

	common.WaitForShutdownComplete()
}


func readSailboats(cancellationContext context.Context, rxQueue chan messages.Sailboat) {
	readerConf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_boat_data",
		GroupID:  "g3",
		MaxBytes: 1024,
	}

	reader := kafka.NewReader(readerConf)

	for {
		fmt.Println("Waiting to receive message...")
		msg, err := reader.ReadMessage(cancellationContext)
		
		if err == context.Canceled {
			fmt.Println("Shutting down, reading cancelled...")
			break;
		} else if err != nil {
			fmt.Printf("ERROR: an error occurred reading messages -> %v\n", err)
		} else {
			fmt.Printf("Processing message %v\n", msg.Key)

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
	reader.Close()
	close(rxQueue)
}

func transform(rxQueue chan messages.Sailboat, xmtQueue chan messages.Boat) {

	sailboatHistory := make(map[string]messages.Sailboat)

	// Receive
	for sailboat := range rxQueue {
		// Transform - TODO

		// Lookup last velocity in map for this boat
		// We can only calculate velocity with two points
		// If not found, nothing to send
		// If found, calculate and send
		_, found := sailboatHistory[sailboat.Name]			// TBD use last known position to calc below
		if found {
			boat := messages.Boat{
				Name: sailboat.Name,
				Type: "sailboat",
				Position: sailboat.Position,
				Velocity: messages.Velocity2D{},					// TBD - transform this
				Timestamp: sailboat.Timestamp,
			}

			xmtQueue <- boat
		} else {
			sailboatHistory[sailboat.Name] = sailboat
		}
	}

	fmt.Println("Stopping transformations.  Closing transmit queue...")
	close(xmtQueue)
}

func sendGeneralizedBoatData(cancellationContext context.Context, xmtQueue chan messages.Boat, signalShutdownComplete context.CancelFunc) {

	for msg := range xmtQueue {
		// TBD - send the message
		fmt.Println(msg)
	}

	fmt.Println("Stopping transmissions...")
	signalShutdownComplete()
}
