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