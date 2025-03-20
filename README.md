# Workshop with [AsyncAPI](https://www.asyncapi.com/en)
* Design-first
* Working with Apache Kafka
* Generate producer and consumer code from AsyncAPI
* Observability
  * Distributed tracing 


## Step 1 :: Install [AsyncAPI CLI](https://www.asyncapi.com/tools/cli)
* Create file
* Validate

**Install**
```
$npm install -g @asyncapi/cli
```

**Create AsyncAPI file**
```
$cd asyncapi
$asyncapi new file
```

Open in [AsyncAPI Studio](https://studio.asyncapi.com/)

**Validate your file**
```
$asyncapi validate asyncapi.yaml
```

## Step 2 :: Create Apache Kafka
```
$docker compose up -d kafka
$docker compose ps
```

Create Kafka UI
```
$docker compose up -d kafka-ui
$docker compose ps
```

Access to Kafka UI
* http://localhost:8080


## Step 3 :: Generate code of consumer and producer
* Use [asyncapi-codegen](https://github.com/lerenn/asyncapi-codegen)
* [List of tools](https://www.asyncapi.com/tools)

```
$go install github.com/lerenn/asyncapi-codegen/cmd/asyncapi-codegen@latest
$asyncapi-codegen -i ./asyncapi/asyncapi.yaml -p api -o ./order-service/create.order.gen.go
```

Output files in folder `order-service`


## Step 4 :: Run producer
```
$go run cmd/producer.go
```

## Step 5 :: Run consumer
```
$go run cmd/consumer.go
```

## Extra :: Simulate the Kafka Instance with [Mokapi](https://mokapi.io/)

**Install**
```
$npm install -g go-mokapi
```

**Create mock server for Kafka broker at localhost:9092**
```
$mokapi --providers-file-filename asyncapi.yaml
```