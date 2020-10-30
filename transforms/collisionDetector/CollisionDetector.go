package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/augustwenty/go-crash/common"
	"github.com/augustwenty/go-crash/messages"
	"encoding/json"
)

func main() {

	fmt.Println("Starting fake UI receiver....")

	cancellationContext, signalShutdownComplete := common.InitCloseHandler()
	
	rxQueue := make(chan messages.Boat)
	xmtQueue := make(chan messages.BoatDetections)
	
	go readBoats(cancellationContext, rxQueue)
	go transform(rxQueue, xmtQueue)
	go sendCollisionDetections(xmtQueue, signalShutdownComplete)

	common.WaitForShutdownComplete()
}


func readBoats(cancellationContext context.Context, rxQueue chan messages.Boat) {
	boatReaderConf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "boat_data",
		GroupID:  "g3",
		MaxBytes: 1024,
	}

	boatReader := kafka.NewReader(boatReaderConf)

	for {
		// fmt.Println("Waiting to receive message...")
		msg, err := boatReader.ReadMessage(cancellationContext)
		
		if err == context.Canceled {
			fmt.Println("Shutting down, reading cancelled...")
			break;
		} else if err != nil {
			fmt.Printf("ERROR: an error occurred reading messages -> %v\n", err)
		} else {
			var boat messages.Boat
			err := json.Unmarshal(msg.Value, &boat)
			if err != nil {
				fmt.Printf("ERROR: an error occured interpreting messsage date -> %v\n", err)
			} else {
				fmt.Printf("Thar be the %v %v!\n", boat.Type, boat.Name)
				rxQueue <- boat
			}
		}
	}

	fmt.Println("Closing reader and receive queue...")
	if err := boatReader.Close(); err != nil {
		fmt.Println("Couldn't close boat reader...")
	}
	close(rxQueue)
}

func transform(rxQueue chan messages.Boat, xmtQueue chan messages.BoatDetections) {

	for latest := range rxQueue {
		boatDetections := messages.BoatDetections{
			Boat: latest,
			CollisionWarning: false,
			Obliterated: false,
		}

		xmtQueue <- boatDetections
	}

	fmt.Println("Stopping transformations.  Closing transmit queue...")
	close(xmtQueue)
}

func sendCollisionDetections(xmtQueue chan messages.BoatDetections, signalShutdownComplete context.CancelFunc) {
	boatWriterConf := kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "boats_detected",
		Balancer: &kafka.Hash{},
	}
	boatWriter := kafka.NewWriter(boatWriterConf)
	boatMessages := make([]kafka.Message, 1)

	for boatDetection := range xmtQueue {
		fmt.Printf("The %v %v is coming your way!\n", boatDetection.Boat.Type, boatDetection.Boat.Name)
		payload, _ := json.Marshal(boatDetection)
		boatMessages[0] = kafka.Message {
			Key:   []byte(boatDetection.Boat.Name),
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
