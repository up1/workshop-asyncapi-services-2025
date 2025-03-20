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

	// Create a new order topic
	ctrl, err := api.NewUserController(broker)
	if err != nil {
		log.Fatalf("Failed to create user controller: %v", err)
	}
	// Create a new order and send it to the order topic
	orderPayload := api.OrderMessagePayload{
		OrderId:    "12345",
		CustomerId: stringPtr("67890"),
		Items: []api.ItemFromItemsPropertyFromOrderMessagePayload{
			{
				ProductId: stringPtr("abc123"),
				Quantity:  int64Ptr(2),
			},
		},
		TotalAmount: float32Ptr(100.50),
	}
	order := api.OrderMessage{
		Payload: orderPayload,
	}
	// Send the order to the order topic
	if err := ctrl.SendToOnOrderCreatedOperation(context.Background(), order); err != nil {
		log.Fatalf("Failed to send order: %v", err)
	}
	log.Println("Order sent successfully")

}

func stringPtr(s string) *string {
	return &s
}

func int64Ptr(i int64) *int64 {
	return &i
}

func float32Ptr(i float32) *float32 {
	return &i
}
