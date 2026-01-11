# @koala42/redis-highway

High performance Redis stream-based queue for Node.js. Supports Redis single instances and Valkey single instances

## Missing features ATM
- Automatic job data serialization with generic types as in Worker<T>
- In worker process functions expose more than just data: T like job id and current status
- Option to customize/enable/disable metrics


## Roadmap
- Support redis cluster, that is probably possible only for job payloads and DLQ, since the stream is only one

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
  JSON.stringify({ hello: 'world' }), // Message serialization is not done automatically
  ['group-A', 'group-B'] // Target specific consumer groups
);
```

### Worker

```typescript
import { Redis } from 'ioredis';
import { Worker } from '@koala42/redis-highway';

class MyWorker extends Worker<T> {
  async process(data: T) {
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

## Usage with NestJS

```typescript

// Producer
@Injectable()
export class EntryService {
  privater readonly producer: Producer;
  
  constructor(){
    this.producer = new Producer(
      new Redis(...), // Or reuse existing ioredis connection
      'my-stream'
    )
  }
  
  public async sth(): Promise<void>{
    await producer.push(
      JSON.stringify({ hello: 'world' }), // Message serialization is not done automatically
      ['group-A', 'group-B'] // Target specific consumer groups
    );
  }
}


// Processor
@Injectable()
export class ProcessorService extends Worker<T> implements OnModuleInit, OnModuleDestroy {
  constructor(){
    super(
      new Redis(...), // or reuse existing redis conn
      'group-A',
      'my-stream',
      50 // concurrency
    )
  }
  
  async onModuleInit(): Promise<void>{
    await this.start()
  }
  
  onModuleDestroy(){
    this.stop()
  }
  
  async process(data: T): Promise<void>{
    console.log("Processing job", JSON.stringify(data))
  }
}
````

## Features
- **Lightweight**: Uses light Lua scripts and pipelines wherever possible, making it highly concurrents for inserts and for processing as well, because of the reduced I/O load compared to BullMQ
- **Granular Retries**: If one group fails, only that group retries.
- **DLQ**: Dead Letter Queue support after max retries.
- **Metrics**: Throughput, Waiting, DLQ, Prometheus export.
