# @koala42/redis-highway

High performance Redis stream-based queue for Node.js. Supports Redis single instances and Valkey single instances.
Designed for high throughput and massive concurrency with low overhead.

## Features
- **Lightweight**: Uses optimized Lua scripts and pipelines for maximum performance and reduced I/O.
- **Micro-Batching**: Supports batch processing for high-volume message consumption.
- **Granular Retries**: Consumer group isolation - if one group fails, only that group retries.
- **Reliability**: Auto-claiming of stuck messages (crashed consumers) and Dead Letter Queue (DLQ) support.
- **Metrics**: Built-in tracking for throughput, queue depth, DLQ size, and retries. Prometheus export ready.
- **ZSTD Compression**: Optional payload compression using Node.js built-in ZSTD. Workers auto-detect compressed messages.

## Installation

```bash
npm install @koala42/redis-highway
```

## Usage

### Producer

```typescript
import { Redis } from 'ioredis';
import { Producer } from '@koala42/redis-highway';

const redis = new Redis();
const producer = new Producer<{hello: string}>(redis, {
  streamName: 'my-stream',
  compression: false // Set to true to enable ZSTD compression
});

// Send job
await producer.push(
  { hello: 'world' }, // Type-safe payload
  ['group-A', 'group-B'], // Target specific consumer groups
  { ttl: 3600 } // Optional: expiration time in seconds
);
```

### Worker

```typescript
import { Redis } from 'ioredis';
import { Worker } from '@koala42/redis-highway';

class MyWorker extends Worker<{hello: string}> {
  async process(data: {hello: string}) {
    console.log('Processing:', data.hello);
    // throw new Error('fail'); // Triggers automatic retry logic
  }
}

const redis = new Redis();
const worker = new MyWorker(
  redis,
  {
    groupName: 'group-A',
    streamName: 'my-stream',
    concurrency: 10 // Number of concurrent jobs to process
  }
);

await worker.start();

// To stop gracefully
// await worker.stop();
```

### Batch Worker
Process messages in batches for higher throughput.

```typescript
import { Redis } from 'ioredis';
import { BatchWorker } from '@koala42/redis-highway';

class MyBatchWorker extends BatchWorker<{hello: string}> {
  async process(batchedData: {hello: string}[]) {
    console.log(`Processing batch of ${batchedData.length} items`);
    // Example: Bulk insert into database
  }
}

const batchWorker = new MyBatchWorker(
  redis,
  {
    groupName: 'group-B',
    streamName: 'my-stream',
    concurrency: 50, // Total items processing limit
    batchSize: 10,   // Items per batch
    maxFetchCount: 50
  }
);

await batchWorker.start();
```

### DLQ Worker
Process messages from the Dead Letter Queue. Use this to handle jobs that have exhausted all retries.

**Important:** DLQ Worker has no built-in error handling or retry policy. If `process()` throws an error, the message is lost. This is by design - DLQ processing is meant for manual intervention, logging, or forwarding to external systems.

```typescript
import { Redis } from 'ioredis';
import { DlqWorker, DlqMessageEntity } from '@koala42/redis-highway';

class MyDlqWorker extends DlqWorker<{hello: string}> {
  async process(message: DlqMessageEntity<{hello: string}>) {
    console.log('Failed job data:', message.data);
    console.log('Original error:', message.errorMessage);
    console.log('Failed at:', new Date(message.failedAt));
    console.log('Original consumer group:', message.group);

    // Example: Log to external system, send alert, or store for manual review
    await externalLogger.log(message);
  }
}

const redis = new Redis();
const dlqWorker = new MyDlqWorker(redis, {
  streamName: 'my-stream' // Must match your main worker's stream
});

await dlqWorker.start();

// To stop gracefully
// await dlqWorker.stop();
```

#### DLQ Worker Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `streamName` | string | - | **Required**. The Redis stream key (same as your main workers). |
| `blockTimeoutMs` | number | 5000 | Redis XREADGROUP block duration in milliseconds. |
| `waitTimeoutMs` | number | 5000 | Wait time between processing cycles when no messages are available. |

#### DlqMessageEntity Properties

| Property | Type | Description |
|----------|------|-------------|
| `data` | T | The original job payload. |
| `errorMessage` | string | The error message from the last failed attempt. |
| `failedAt` | number | Unix timestamp when the job was moved to DLQ. |
| `group` | string | The consumer group that failed to process this job. |
| `messageUuid` | string | The original job's unique identifier. |
| `streamMessageId` | string | The Redis stream message ID. |

### Metrics

```typescript
import { Metrics } from '@koala42/redis-highway';

const metrics = new Metrics(redis, 'my-stream');

// Get raw metrics object
const stats = await metrics.getMetrics(['group-A', 'group-B']);
console.log(stats.throughput);

// Get Prometheus formatted string
const promMetrics = await metrics.getPrometheusMetrics(['group-A'], 'my_app_queue');
```

### Compression

Enable ZSTD compression to reduce Redis memory usage and network bandwidth for large payloads.

```typescript
const producer = new Producer<{hello: string}>(redis, {
  streamName: 'my-stream',
  compression: true // Enable ZSTD compression
});

// Messages are automatically compressed before being sent to Redis
await producer.push({ hello: 'world' }, ['group-A']);
```

**Key points:**
- Compression uses Node.js built-in ZSTD (no external dependencies required, Node.js 22+)
- Workers automatically detect and decompress compressed messages
- No configuration changes needed on workers - they handle both compressed and uncompressed messages
- Recommended for payloads larger than 1KB where compression benefits outweigh CPU overhead

## Configuration

### Worker Options
The second argument to `Worker` and `BatchWorker` constructors is the primary configuration object.

| Option | Type | Description |
|--------|------|-------------|
| `groupName` | string | **Required**. The consumer group name (e.g., 'email-service'). |
| `streamName` | string | **Required**. The Redis stream key. |
| `concurrency` | number | **Required**. Maximum number of messages processed in parallel by this worker instance. |
| `batchSize` | number | **Required (BatchWorker only)**. Number of messages to process in a single call. |
| `maxFetchCount` | number | **Required (BatchWorker only)**. limit for XREADGROUP count. |

### Control Options
The third argument is for fine-tuning retry and recovery behavior.

```typescript
const worker = new MyWorker(redis, { ... }, {
  maxRetries: 3,         // Default: 3
  blockTimeMs: 2000,     // Default: 2000. XREADGROUP block time.
  minIdleTimeMs: 120000, // Default: 2 minutes. Time before a message is considered stuck.
  claimIntervalMs: 120000,// Default: 2 minutes. How often to check for stuck messages.
  collectMetrics: true    // Default: true. Enable throughput tracking.
});
```

| Option | Default | Description |
|--------|---------|-------------|
| `maxRetries` | 3 | Number of times to retry a failed message before moving it to DLQ. |
| `blockTimeMs` | 2000 | Redis blocking timeout for fetching new messages (in ms). |
| `minIdleTimeMs` | 120000 | Messages pending longer than this are candidates for auto-claim (recovery). |
| `claimIntervalMs` | 120000 | Interval for checking and claiming stuck messages. |
| `collectMetrics` | true | If true, increments throughput counters in Redis. |

## Usage with NestJS

```typescript
// Producer Service
@Injectable()
export class EntryService {
  private readonly producer: Producer<MyPayload>;

  constructor(@InjectRedis() private readonly redis: Redis) {
    this.producer = new Producer(this.redis, {
      streamName: 'my-stream',
      compression: false
    });
  }

  async addToQueue(data: MyPayload) {
    await this.producer.push(data, ['group-A']);
  }
}

// Worker Service
@Injectable()
export class ProcessorService extends Worker<MyPayload> implements OnModuleInit, OnModuleDestroy {
  constructor(@InjectRedis() redis: Redis) {
    super(redis, {
      groupName: 'group-A',
      streamName: 'my-stream',
      concurrency: 50
    });
  }

  async onModuleInit() {
    await this.start();
  }

  async onModuleDestroy() {
    await this.stop();
  }

  async process(data: MyPayload) {
    // Process your job here
  }
}
```

## Roadmap & Missing Features
tracked in [Github Issues](https://github.com/Koala42/redis-highway/issues)

## AI Usage Disclosure
- AI will not be used for the development, ever
- AI may be used to do code reviews
- AI may be used to write unit tests
