# Workshop :: Distributed Tracing
* Go
* OpenTelemetry
* Apache Kafka
* Jaeger

## Step 1 :: Create Otel collector and Jaeger
```
$docker compose up -d otel-collector
$docker compose up -d jaeger
$docker compose ps
```

Access to Jaeger UI
* http://localhost:16686

## Step 2 :: Edit AsycAPI file 
* Add Trace ID and Span ID to message

## Step 3 :: Start Apache Kafka and Kafka UI
```
$docker compose up -d kafka
$docker compose up -d kafka-ui
$docker compose ps
```

## Step 4 :: Run producer => order-service
* Add trace id and span id to message
```
$go run cmd/producer.go
```

## Step 5 :: Run consumer  => report-service
* * Read trace id and span id from message
```
$go run cmd/consumer.go
```

## Step 6 :: See results in to Jaeger UI
* http://localhost:16686