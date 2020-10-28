package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/augustwenty/go-crash/messages"
)

func main() {

	fmt.Println("Starting velocity transformer....")

	rxQueue := make(chan messages.Sailboat)
	xmtQueue := make(chan messages.Boat)

	go readSailboats(rxQueue)
	go transform(rxQueue, xmtQueue)
	go sendGeneralizedBoatData(xmtQueue)

	time.Sleep(10 * time.Minute)
}

func readSailboats(rxQueue chan messages.Sailboat) {
	readerConf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_boat_data",
		GroupID:  "g3",
		MaxBytes: 1024,
	}

	reader := kafka.NewReader(readerConf)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("ERROR: an error occurred reading messages -> ", err)
			continue
		}

		// Push to receive queue

		// Push to transmit queue
		fmt.Println("Message:", string(msg.Key), string(msg.Value))
		if string(msg.Value) == "STOP" {
			break
		}
	}
	fmt.Println("closing reader")
	reader.Close()

}

func transform(rxQueue chan messages.Sailboat, xmtQueue chan messages.Boat) {

	// Receive

	// Transform

	// Send

}

func sendGeneralizedBoatData(xmtQueue chan messages.Boat) {

}
