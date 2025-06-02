import { Registry, Counter, Histogram } from 'prom-client';

const register = new Registry();

// Producer metrics
const producerMetrics = {
  eventsProcessed: new Counter({
    name: 'cdc_producer_events_processed_total',
    help: 'Total number of CDC events processed by producer',
    labelNames: ['operation_type', 'collection'],
  }),
  eventProcessingDuration: new Histogram({
    name: 'cdc_producer_event_processing_duration_seconds',
    help: 'Duration of CDC event processing in seconds',
    labelNames: ['operation_type', 'collection'],
  }),
  errors: new Counter({
    name: 'cdc_producer_errors_total',
    help: 'Total number of errors in producer',
    labelNames: ['error_type', 'collection'],
  }),
};

// Consumer metrics
const consumerMetrics = {
  eventsConsumed: new Counter({
    name: 'cdc_consumer_events_consumed_total',
    help: 'Total number of CDC events consumed',
    labelNames: ['operation_type', 'collection'],
  }),
  eventProcessingDuration: new Histogram({
    name: 'cdc_consumer_event_processing_duration_seconds',
    help: 'Duration of CDC event processing in seconds',
    labelNames: ['operation_type', 'collection'],
  }),
  errors: new Counter({
    name: 'cdc_consumer_errors_total',
    help: 'Total number of errors in consumer',
    labelNames: ['error_type', 'collection'],
  }),
};

// Register all metrics
Object.values(producerMetrics).forEach(metric => register.registerMetric(metric));
Object.values(consumerMetrics).forEach(metric => register.registerMetric(metric));

export { register, producerMetrics, consumerMetrics }; 