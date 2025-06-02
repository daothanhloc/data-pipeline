import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables
dotenv.config({ path: join(__dirname, '../../.env') });

const config = {
  // MongoDB Configuration
  sourceMongoUri: process.env.SOURCE_MONGO_URI || '',
  targetMongoUri: process.env.TARGET_MONGO_URI || '',
  database: process.env.MONGODB_DATABASE || 'test',
  collections: (process.env.MONGODB_COLLECTIONS || 'users,products,orders').split(','),
  
  // Update Configuration
  updateIntervalMinutes: parseInt(process.env.UPDATE_INTERVAL_MINUTES || '30', 10),
  deduplicationWindowMinutes: parseInt(process.env.DEDUPLICATION_WINDOW_MINUTES || '30', 10),

  // Kafka Configuration
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  kafkaClientId: process.env.KAFKA_CLIENT_ID || 'mongodb-cdc',
  kafkaTopic: process.env.KAFKA_TOPIC || 'mongodb.cdc',
  kafkaGroupId: process.env.KAFKA_GROUP_ID || 'mongodb-cdc-group',

  // Retry Configuration
  maxRetries: parseInt(process.env.MAX_RETRIES || '5', 10),
  initialRetryDelay: parseInt(process.env.INITIAL_RETRY_DELAY || '1000', 10),
  maxRetryDelay: parseInt(process.env.MAX_RETRY_DELAY || '30000', 10),

  // Metrics Configuration
  metricsPort: parseInt(process.env.METRICS_PORT || '9090', 10),
  enableMetrics: process.env.ENABLE_METRICS === 'true',

  // Logging Configuration
  logLevel: process.env.LOG_LEVEL || 'info',
};

// Validate required configuration
const requiredFields = ['sourceMongoUri', 'targetMongoUri', 'kafkaBrokers'];
const missingFields = requiredFields.filter(field => !config[field]);

if (missingFields.length > 0) {
  throw new Error(`Missing required configuration fields: ${missingFields.join(', ')}`);
}

export default config; 