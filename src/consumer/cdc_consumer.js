import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';
import config from '../config/config.js';
import logger from '../utils/logger.js';
import { consumerMetrics } from '../utils/metrics.js';

class CDCConsumer {
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
    this.consumer = this.kafka.consumer({ groupId: config.kafkaGroupId });
    this.isRunning = false;
    this.collections = new Map();
  }

  async connect() {
    try {
      await mongoose.connect(config.targetMongoUri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });
      logger.info('Connected to target MongoDB');

      await this.consumer.connect();
      logger.info('Connected to Kafka');

      this.isRunning = true;
    } catch (error) {
      consumerMetrics.errors.inc({ 
        error_type: 'connection_error',
        collection: 'system'
      });
      logger.error({ err: error }, 'Failed to connect to services');
      throw error;
    }
  }

  async processMessage(collection, message) {
    const timer = consumerMetrics.eventProcessingDuration.startTimer();
    try {
      const event = JSON.parse(message.value.toString());
      const coll = this.collections.get(collection);

      switch (event.operationType) {
        case 'insert':
        case 'update':
          await coll.findOneAndUpdate(
            { _id: event.documentId },
            { $set: event.fullDocument },
            { upsert: true }
          );
          break;

        case 'delete':
          await coll.deleteOne({ _id: event.documentId });
          break;

        default:
          logger.warn({ 
            operationType: event.operationType,
            collection 
          }, 'Unsupported operation type');
          return;
      }

      consumerMetrics.eventsConsumed.inc({ 
        operation_type: event.operationType,
        collection 
      });
      timer({ 
        operation_type: event.operationType,
        collection 
      });

      logger.info({
        operationType: event.operationType,
        documentId: event.documentId,
        collection,
      }, 'Event processed successfully');
    } catch (error) {
      consumerMetrics.errors.inc({ 
        error_type: 'processing_error',
        collection 
      });
      logger.error({ err: error, collection }, 'Failed to process event');
      throw error; // Let Kafka handle retry
    }
  }

  async initializeCollections() {
    try {
      const db = mongoose.connection.db;
      for (const collectionName of config.collections) {
        this.collections.set(collectionName, db.collection(collectionName));
        logger.info(`Initialized collection: ${collectionName}`);
      }
    } catch (error) {
      consumerMetrics.errors.inc({ 
        error_type: 'collection_init_error',
        collection: 'system'
      });
      logger.error({ err: error }, 'Failed to initialize collections');
      throw error;
    }
  }

  async start() {
    try {
      await this.connect();
      await this.initializeCollections();
      
      // Subscribe to all collection topics
      const topics = config.collections.map(collection => 
        `${config.kafkaTopic}.${collection}`
      );

      await this.consumer.subscribe({
        topics,
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            // Extract collection name from topic
            const collection = topic.split('.').pop();
            await this.processMessage(collection, message);
          } catch (error) {
            consumerMetrics.errors.inc({ 
              error_type: 'message_processing_error',
              collection: topic.split('.').pop() || 'unknown'
            });
            logger.error({
              err: error,
              topic,
              partition,
              offset: message.offset,
            }, 'Error processing message');
          }
        },
      });

      this.setupGracefulShutdown();
      logger.info('CDC consumer started successfully');
    } catch (error) {
      consumerMetrics.errors.inc({ 
        error_type: 'startup_error',
        collection: 'system'
      });
      logger.error({ err: error }, 'Failed to start CDC consumer');
      process.exit(1);
    }
  }

  setupGracefulShutdown() {
    const shutdown = async () => {
      logger.info('Shutting down CDC consumer...');
      this.isRunning = false;
      
      try {
        await this.consumer.disconnect();
        await mongoose.connection.close();
        logger.info('CDC consumer shutdown complete');
        process.exit(0);
      } catch (error) {
        consumerMetrics.errors.inc({ 
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

// Start the consumer
const consumer = new CDCConsumer();
consumer.start().catch((error) => {
  consumerMetrics.errors.inc({ 
    error_type: 'fatal_error',
    collection: 'system'
  });
  logger.error({ err: error }, 'Fatal error in CDC consumer');
  process.exit(1);
}); 