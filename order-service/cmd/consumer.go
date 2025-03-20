package main

import (
	"api"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/lerenn/asyncapi-codegen/pkg/extensions/brokers/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
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
			semconv.ServiceNameKey.String("report-service"),
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
	broker, err := kafka.NewController([]string{"localhost:19092"}, kafka.WithGroupID("report-service"))
	if err != nil {
		log.Fatalf("Failed to create Kafka controller: %v", err)
	}

	ctrl, _ := api.NewAppController(broker)
	defer ctrl.Close(context.Background())

	// Subscribe to the order created operation
	if err := ctrl.SubscribeToOnOrderCreatedOperation(context.Background(), func(ctx context.Context, order api.OrderMessage) error {
		log.Printf("Received order: %+v", order)

		tid, _ := trace.TraceIDFromHex(order.Payload.TraceId)
		sid, _ := trace.SpanIDFromHex(order.Payload.SpanId)

		tracer := otel.GetTracerProvider()
		tr := tracer.Tracer("consumer-step01")
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid,
			SpanID:  sid,
		})
		_, span := tr.Start(trace.ContextWithRemoteSpanContext(ctx, sc), "consumer-received-order")
		span.SetAttributes(attribute.String("topic_name", "order-created"))
		defer span.End()

		// Process the order
		fmt.Printf("Processing order: %s\n", order.Payload.OrderId)

		return nil
	}); err != nil {
		log.Fatalf("Failed to subscribe to order topic: %v", err)
	}

	select {
	// Ctrl + C
	case <-ctx.Done():
		log.Println("Received interrupt signal, shutting down...")
		os.Exit(0)
	} // Block forever

}
