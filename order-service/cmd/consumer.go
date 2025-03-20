package main

import (
	"api"
	"context"
	"log"

	"github.com/lerenn/asyncapi-codegen/pkg/extensions/brokers/kafka"
)

func main() {
	// Create a new Kafka broker
	broker, err := kafka.NewController([]string{"localhost:19092"})
	if err != nil {
		log.Fatalf("Failed to create Kafka controller: %v", err)
	}

	ctrl, _ := api.NewAppController(broker)
	defer ctrl.Close(context.Background())

	// Subscribe to the order created operation
	if err := ctrl.SubscribeToOnOrderCreatedOperation(context.Background(), func(ctx context.Context, order api.OrderMessage) error {
		log.Printf("Received order: %+v", order)
		return nil
	}); err != nil {
		log.Fatalf("Failed to subscribe to order topic: %v", err)
	}

	select {} // Block forever

}
