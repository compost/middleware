# Kafka Mirror Service

A Quarkus application that mirrors Kafka topics between clusters, extracting partition keys from message content (`brand_id-player_id`).

## Features

- 🔄 **Kafka Topic Mirroring**: Mirror messages between source and target Kafka clusters
- 🔑 **Dynamic Partition Keys**: Extract `brand_id-player_id` from message content as partition key
- 🏥 **Health Checks**: Built-in health probes for container deployment
- 🧪 **Integration Tests**: Comprehensive tests with real Kafka clusters
- 🎯 **Error Handling**: Graceful fallback for invalid messages

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source Kafka  │───▶│  Mirror Service │───▶│  Target Kafka   │
│   (port 9092)   │    │   (Quarkus)     │    │   (port 9093)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        │              ┌─────────────────┐              │
        └─────────────▶│   Kafka UI      │◀─────────────┘
                       │   (port 8080)   │
                       └─────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Java 21+
- Gradle 8.14+

### 1. Start Kafka Infrastructure

```bash
# Start both Kafka clusters and UI
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

This starts:
- **Source Kafka**: `localhost:9092` (Source cluster)
- **Target Kafka**: `localhost:9093` (Target cluster)  
- **Kafka UI**: `http://localhost:8080` (Web interface for both clusters)

### 2. Build and Test

```bash
# Run all tests (including integration tests)
./gradlew test

# Run only integration tests
./gradlew test --tests "KafkaContainerIntegrationTest"

# Run specific integration test
./gradlew test --tests "KafkaContainerIntegrationTest.testRealKafkaIntegrationWithContainers"
```

### 3. Access Kafka UI

Open your browser to [http://localhost:8080](http://localhost:8080) to:

- 📊 Monitor both **Source Cluster** and **Target Cluster**
- 📝 View topics, messages, and consumer groups
- 🔍 Inspect partition distribution and message flow
- 📈 Monitor metrics and performance

## Message Processing

The service processes JSON messages and extracts partition keys:

### Input Message
```json
{
  "brand_id": "betsson_se",
  "player_id": "player_premium_001",
  "event_type": "slot_spin",
  "game": {
    "id": "mega_fortune_dreams",
    "provider": "netent"
  },
  "bet": {
    "amount": 10.0,
    "currency": "SEK"
  },
  "timestamp": "2025-09-21T22:30:15.123Z"
}
```

### Generated Partition Key
```
betsson_se-player_premium_001
```

## Integration Tests

The integration tests demonstrate real cross-cluster mirroring:

### Test Scenario
1. **Send messages** to Source Kafka (`localhost:9092`)
2. **Process through** mirror service (extracts partition keys)
3. **Send mirrored messages** to Target Kafka (`localhost:9093`)
4. **Verify** correct partition key generation and message integrity

### Test Data
- Gaming events (slots, blackjack, sports betting)
- Different brands (betsson_se, leovegas_com, unibet_uk)
- Error handling scenarios (malformed JSON, missing fields)

## Development Commands

```bash
# Start infrastructure
docker compose up -d

# Run application in dev mode
./gradlew quarkusDev

# Run specific test
./gradlew test --tests "KafkaContainerIntegrationTest.testRealKafkaIntegrationWithContainers"

# Stop infrastructure
docker compose down

# Clean up volumes
docker compose down -v
```

## Configuration

### Profiles

The application supports multiple profiles for different environments:

#### Default Profile (Development)
```yaml
# Source: localhost:9092 (topic: source-topic)
# Target: localhost:9093 (topic: target-topic)
# Configuration file: application.yml
```

#### Greenfeather Profile (Production)
```yaml
# Source: 10.0.0.1:9093 (topic: changelog)
# Target: 10.0.1.1:9022 (topic: old-changelog)
# Configuration file: application-greenfeather.yml
```

**Run with Greenfeather profile:**
```bash
# Using system property
java -Dquarkus.profile=greenfeather -jar target/quarkus-app/quarkus-run.jar

# Using environment variable
export QUARKUS_PROFILE=greenfeather
./gradlew quarkusRun

# In development mode
./gradlew quarkusDev -Dquarkus.profile=greenfeather
```

**Build and publish container image:**
```bash
# Build and push to registry (greenfeather profile)
./gradlew build -Dquarkus.profile=greenfeather -Dquarkus.container-image.push=true

# Or use the image build task
./gradlew quarkusBuild -Dquarkus.profile=greenfeather

# The image will be published to: greenfeather.azurecr.io/kafka-mirror/kafka-mirror-service:latest
```

### Docker Compose Services

| Service | Port | Purpose |
|---------|------|---------|
| kafka-source | 9092 | Source Kafka cluster |
| kafka-target | 9093 | Target Kafka cluster |
| kafka-ui | 8080 | Web UI for both clusters |

### Environment Variables

```yaml
# Source Kafka
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka-source:29092,PLAINTEXT_HOST://localhost:9092'

# Target Kafka  
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka-target:29092,PLAINTEXT_HOST://localhost:9093'

# Auto-create topics enabled on both clusters
KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
```

### Container Image Configuration

The greenfeather profile includes container image publishing configuration:

```yaml
quarkus:
  container-image:
    registry: "greenfeather.azurecr.io"
    group: "kafka-mirror"
    name: "kafka-mirror-service"
    tag: "latest"
    push: true
    builder: "jib"
```

**Generated image:** `greenfeather.azurecr.io/kafka-mirror/kafka-mirror-service:latest`

### JSON Logging Configuration

Both profiles include structured JSON logging with service name identification:

```yaml
quarkus:
  log:
    console:
      json: true
      json:
        additional-field:
          serviceName:
            value: "kafka-mirror-default"  # or "kafka-mirror-greenfeather"
```

**Benefits:**
- Structured log parsing for observability platforms
- Service identification in log aggregation
- Profile-specific service naming for environment tracking

## Monitoring

### Kafka UI Dashboard
- **Source Cluster**: View original messages
- **Target Cluster**: View mirrored messages with new partition keys
- **Topics**: Monitor `source-topic` and `target-topic`
- **Consumer Groups**: Track processing progress

### Health Checks
- Application health: Built-in Quarkus health checks
- Kafka health: Container health checks with broker API validation
- UI health: HTTP endpoint validation

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 9092, 9093, 8080 are available
2. **Container startup**: Wait for health checks to pass (may take 30-60 seconds)
3. **Topic creation**: Topics are auto-created, but may need time to initialize

### Debugging Commands

```bash
# Check container status
docker compose ps

# View service logs
docker compose logs kafka-source
docker compose logs kafka-target
docker compose logs kafka-ui

# Test Kafka connectivity
docker exec kafka-source kafka-broker-api-versions --bootstrap-server localhost:9092
docker exec kafka-target kafka-broker-api-versions --bootstrap-server localhost:9093

# Reset environment
docker compose down -v && docker compose up -d
```

## Production Deployment

For production deployment to Azure Container Apps (ACA):

1. Configure external Kafka clusters
2. Update `application.properties` with production endpoints
3. Enable authentication and TLS
4. Configure health probe endpoints
5. Set appropriate resource limits

## License

This project is part of a Kafka mirroring solution for gaming event processing.

```

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create \
  --bootstrap-server 10.21.3.14:9092 \
  --topic marsbet-v1-player-store-changelog \
  --partitions 16 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=604800000
```
