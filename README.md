# MongoDB CDC Pipeline

A Change Data Capture (CDC) pipeline that captures changes from MongoDB collections and replicates them to another MongoDB instance using Kafka as the message broker.

## Architecture

The system consists of three main components:

1. **MongoDB Source**: The source database where changes are captured
2. **Kafka**: Message broker for reliable event streaming
3. **MongoDB Target**: The target database where changes are replicated

### Components

#### 1. CDC Producer

- Watches multiple MongoDB collections for changes
- Implements near-realtime updates with deduplication
- Buffers changes and sends them to Kafka at configured intervals
- Uses KRaft mode for Kafka (no ZooKeeper dependency)

#### 2. CDC Consumer

- Consumes messages from Kafka topics
- Applies changes to the target MongoDB
- Handles multiple collections
- Implements error handling and retries

## Features

- **Multi-Collection Support**: Watch and replicate multiple collections simultaneously
- **Near-Realtime Updates**: Configurable update interval (default: 30 minutes)
- **Deduplication**: Only the latest state of a document is sent within the update window
- **Error Handling**: Comprehensive error handling and retry mechanisms
- **Metrics**: Prometheus metrics for monitoring
- **Logging**: Structured logging with Pino
- **Graceful Shutdown**: Proper cleanup of resources

## Configuration

### Environment Variables

```env
# MongoDB Configuration
SOURCE_MONGO_URI=mongodb://source:27017
TARGET_MONGO_URI=mongodb://target:27017
MONGODB_DATABASE=your_database
MONGODB_COLLECTIONS=users,products,orders

# Update Configuration
UPDATE_INTERVAL_MINUTES=30
DEDUPLICATION_WINDOW_MINUTES=30

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=mongodb-cdc
KAFKA_TOPIC=mongodb.cdc
KAFKA_GROUP_ID=mongodb-cdc-group

# Retry Configuration
MAX_RETRIES=5
INITIAL_RETRY_DELAY=1000
MAX_RETRY_DELAY=30000

# Metrics Configuration
METRICS_PORT=9090
ENABLE_METRICS=true

# Logging Configuration
LOG_LEVEL=info
```

## Metrics

### Producer Metrics

- `cdc_producer_events_processed_total`: Total events processed
- `cdc_producer_event_processing_duration_seconds`: Event processing duration
- `cdc_producer_errors_total`: Error counts

### Consumer Metrics

- `cdc_consumer_events_consumed_total`: Total events consumed
- `cdc_consumer_event_processing_duration_seconds`: Event processing duration
- `cdc_consumer_errors_total`: Error counts

## Error Types

### Producer Errors

- `connection_error`: Connection failures
- `startup_error`: Startup failures
- `shutdown_error`: Shutdown failures
- `fatal_error`: Fatal errors
- `processing_error`: Change processing errors
- `kafka_send_error`: Kafka send failures
- `change_stream_error`: Change stream errors
- `scheduled_update_error`: Scheduled update failures

### Consumer Errors

- `connection_error`: Connection failures
- `startup_error`: Startup failures
- `shutdown_error`: Shutdown failures
- `fatal_error`: Fatal errors
- `processing_error`: Message processing errors
- `collection_init_error`: Collection initialization errors
- `message_processing_error`: Message processing failures

## How It Works

### Change Capture Process

1. **Change Detection**:

   - Producer watches specified collections using MongoDB Change Streams
   - Changes are captured in real-time
   - Each change includes operation type, document ID, and full document

2. **Update Buffering**:

   - Changes are buffered in memory
   - Only the latest state of each document is kept
   - Updates are scheduled based on configured interval

3. **Message Publishing**:

   - Buffered updates are sent to Kafka
   - Each collection has its own topic
   - Messages include operation type and document data

4. **Change Application**:
   - Consumer reads messages from Kafka topics
   - Updates are applied to target MongoDB
   - Supports insert, update, and delete operations

### Deduplication Logic

1. When a document is updated:

   - The update is stored in the pending updates map
   - A timer is scheduled for the update interval
   - If another update arrives before the timer expires:
     - The previous timer is cancelled
     - The new update replaces the old one
     - A new timer is scheduled

2. When the timer expires:
   - The latest state is sent to Kafka
   - The update is removed from the pending updates
   - The timer is cleared

## Running the System

1. Start the services:

```bash
docker-compose up -d
```

2. Start the producer:

```bash
npm run start:producer
```

3. Start the consumer:

```bash
npm run start:consumer
```

## Monitoring

- Access metrics at `http://localhost:9090/metrics`
- Monitor logs for operation status
- Check Kafka topics for message flow
- Monitor MongoDB change streams

## Best Practices

1. **Configuration**:

   - Set appropriate update intervals
   - Configure sufficient retry attempts
   - Monitor memory usage for large collections

2. **Error Handling**:

   - Monitor error metrics
   - Set up alerts for critical errors
   - Review logs for troubleshooting

3. **Performance**:

   - Adjust batch sizes if needed
   - Monitor Kafka lag
   - Check MongoDB performance

4. **Security**:
   - Use secure MongoDB connections
   - Configure Kafka security
   - Monitor access patterns

## Limitations

- Memory usage increases with number of pending updates
- Update interval affects real-time nature of replication
- Network latency impacts overall performance
- Large documents may impact Kafka performance
# data-pipeline
