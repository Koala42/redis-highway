# @koala42/redis-highway

High performance Redis stream-based queue for Node.js.

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
const producer = new Producer(redis, 'my-stream');

// Send job
await producer.push(
  JSON.stringify({ hello: 'world' }), 
  ['group-A', 'group-B'] // Target specific consumer groups
);
```

### Worker

```typescript
import { Redis } from 'ioredis';
import { Worker } from '@koala42/redis-highway';

class MyWorker extends Worker {
  async process(data: any) {
    console.log('Processing:', data);
    // throw new Error('fail'); // Automatic retry/DLQ logic
  }
}

const redis = new Redis();
const worker = new MyWorker(redis, 'group-A', 'my-stream');

await worker.start();
```

### Metrics

```typescript
import { Metrics } from '@koala42/redis-highway';

const metrics = new Metrics(redis, 'my-stream');

// Prometheus format
const payload = await metrics.getPrometheusMetrics(['group-A']);
```

## Features

- **Granular Retries**: If one group fails, only that group retries.
- **DLQ**: Dead Letter Queue support after max retries.
- **Metrics**: Throughput, Waiting, DLQ, Prometheus export.
