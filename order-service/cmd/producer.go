package main

import (
	"api"
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/lerenn/asyncapi-codegen/pkg/extensions/brokers/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func main() {
	// Initialize OpenTelemetry
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	conn, err := api.InitConn()
	if err != nil {
		log.Fatal(err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// The service name used to display traces in backends
			semconv.ServiceNameKey.String("order-service"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	shutdownTracerProvider, err := api.InitTracerProvider(ctx, res, conn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := shutdownTracerProvider(ctx); err != nil {
			log.Fatalf("failed to shutdown TracerProvider: %s", err)
		}
	}()

	// Create a new Kafka broker
	broker, err := kafka.NewController([]string{"localhost:19092"})
	if err != nil {
		log.Fatalf("Failed to create Kafka controller: %v", err)
	}

	// Create a new user controller
	ctrl, err := api.NewUserController(broker)
	if err != nil {
		log.Fatalf("Failed to create user controller: %v", err)
	}

	// Trace the request
	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("demo").Start(context.Background(), "send-created-order")
	span.SetAttributes(attribute.String("topic_name", "order-created"))

	defer span.End()
	// Get the span context
	traceId := span.SpanContext().TraceID().String()
	spanId := span.SpanContext().SpanID().String()

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

		// Add tracing information
		TraceId: traceId,
		SpanId:  spanId,
	}
	order := api.OrderMessage{
		Payload: orderPayload,
	}
	// Send the order to the order topic with tracing
	if err := ctrl.SendToOnOrderCreatedOperation(ctx, order); err != nil {
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
