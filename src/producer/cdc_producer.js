import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';
import config from '../config/config.js';
import logger from '../utils/logger.js';
import { producerMetrics } from '../utils/metrics.js';

class CDCProducer {
  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      retry: {
        initialRetryTime: config.initialRetryDelay,
        maxRetryTime: config.maxRetryDelay,
        retries: config.maxRetries,
      },
    });
    this.producer = this.kafka.producer();
    this.isRunning = false;
    this.changeStreams = new Map();
    this.pendingUpdates = new Map();
    this.updateTimers = new Map();
  }

  async connect() {
    try {
      await mongoose.connect(config.sourceMongoUri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });
      logger.info('Connected to source MongoDB');

      await this.producer.connect();
      logger.info('Connected to Kafka');

      this.isRunning = true;
    } catch (error) {
      producerMetrics.errors.inc({ 
        error_type: 'connection_error',
        collection: 'system'
      });
      logger.error({ err: error }, 'Failed to connect to services');
      throw error;
    }
  }

  scheduleUpdate(collection, documentId) {
    const key = `${collection}:${documentId}`;

    if (!this.updateTimers.has(key)) {
      // Schedule new update
      const timer = setTimeout(async () => {
        try {
          const update = this.pendingUpdates.get(key);
          if (update) {
            await this.sendToKafka(collection, update);
            this.pendingUpdates.delete(key);
            this.updateTimers.delete(key);
          }
        } catch (error) {
          producerMetrics.errors.inc({ 
            error_type: 'scheduled_update_error',
            collection 
          });
          logger.error({ err: error, collection, documentId }, 'Failed to process scheduled update');
        }
      }, config.updateIntervalMinutes * 60 * 1000);

      this.updateTimers.set(key, timer);
    }
  }

  async sendToKafka(collection, event) {
    try {
      const topic = `${config.kafkaTopic}.${collection}`;
      await this.producer.send({
        topic,
        messages: [
          {
            key: event.documentId.toString(),
            value: JSON.stringify(event),
          },
        ],
      });

      producerMetrics.eventsProcessed.inc({ 
        operation_type: event.operationType,
        collection 
      });
      
      logger.info({
        operationType: event.operationType,
        documentId: event.documentId,
        collection,
      }, 'Event sent to Kafka');
    } catch (error) {
      producerMetrics.errors.inc({ 
        error_type: 'kafka_send_error',
        collection 
      });
      logger.error({ err: error, collection }, 'Failed to send event to Kafka');
      throw error;
    }
  }

  handleChange(collection, change) {
    const timer = producerMetrics.eventProcessingDuration.startTimer();
    try {
      const event = {
        operationType: change.operationType,
        documentId: change.documentKey._id,
        fullDocument: change.fullDocument,
        timestamp: new Date().toISOString(),
      };

      const key = `${collection}:${event.documentId}`;
      
      // Store the latest update
      this.pendingUpdates.set(key, event);
      
      // Schedule the update
      this.scheduleUpdate(collection, event.documentId);

      timer({ 
        operation_type: event.operationType,
        collection 
      });
    } catch (error) {
      producerMetrics.errors.inc({ 
        error_type: 'change_processing_error',
        collection 
      });
      logger.error({ err: error, collection }, 'Failed to process change');
    }
  }

  async watchCollection(collection) {
    const db = mongoose.connection.db;
    const coll = db.collection(collection);
    
    const changeStream = coll.watch([], {
      fullDocument: 'updateLookup',
    });

    logger.info(`Watching collection: ${collection}`);

    changeStream.on('change', (change) => {
      this.handleChange(collection, change);
    });

    changeStream.on('error', (error) => {
      producerMetrics.errors.inc({ 
        error_type: 'change_stream_error',
        collection 
      });
      logger.error({ err: error, collection }, 'Change stream error');
    });

    this.changeStreams.set(collection, changeStream);
  }

  async start() {
    try {
      await this.connect();
      
      // Watch all configured collections
      for (const collection of config.collections) {
        await this.watchCollection(collection);
      }
      
      this.setupGracefulShutdown();
      logger.info('CDC producer started successfully');
    } catch (error) {
      producerMetrics.errors.inc({ 
        error_type: 'startup_error',
        collection: 'system'
      });
      logger.error({ err: error }, 'Failed to start CDC producer');
      process.exit(1);
    }
  }

  setupGracefulShutdown() {
    const shutdown = async () => {
      logger.info('Shutting down CDC producer...');
      this.isRunning = false;
      
      try {
        // Close all change streams
        for (const [collection, stream] of this.changeStreams) {
          await stream.close();
          logger.info(`Closed change stream for collection: ${collection}`);
        }

        // Clear all pending timers
        for (const timer of this.updateTimers.values()) {
          clearTimeout(timer);
        }

        await this.producer.disconnect();
        await mongoose.connection.close();
        logger.info('CDC producer shutdown complete');
        process.exit(0);
      } catch (error) {
        producerMetrics.errors.inc({ 
          error_type: 'shutdown_error',
          collection: 'system'
        });
        logger.error({ err: error }, 'Error during shutdown');
        process.exit(1);
      }
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  }
}

// Start the producer
const producer = new CDCProducer();
producer.start().catch((error) => {
  producerMetrics.errors.inc({ 
    error_type: 'fatal_error',
    collection: 'system'
  });
  logger.error({ err: error }, 'Fatal error in CDC producer');
  process.exit(1);
}); 