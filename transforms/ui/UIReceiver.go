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
	
	go readBoats(cancellationContext, signalShutdownComplete)

	common.WaitForShutdownComplete()
}


func readBoats(cancellationContext context.Context, signalShutdownComplete context.CancelFunc) {
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
			}
		}
	}

	fmt.Println("Closing reader and receive queue...")
	if err := boatReader.Close(); err != nil {
		fmt.Println("Couldn't close boat reader...")
	}

	signalShutdownComplete()
}

