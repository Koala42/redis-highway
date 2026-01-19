import { motion } from 'framer-motion';
import { CodeBlock } from '../components/CodeBlock';
import { Navbar } from '../components/Navbar';

export const GetStarted = () => {
  const installCode = `npm install @koala42/redis-highway`;

  const producerCode = `import { Redis } from 'ioredis';
import { Producer } from '@koala42/redis-highway';

const redis = new Redis();
const producer = new Producer(redis, 'my-stream');

// Send job
await producer.push(
  { hello: 'world' }, 
  ['group-A', 'group-B'] // Target specific consumer groups
);`;

  const workerCode = `import { Redis } from 'ioredis';
import { Worker } from '@koala42/redis-highway';

class MyWorker extends Worker<T> {
  async process(data: T) {
    // Process data...
    console.log('Processing:', data);
  }
}

const redis = new Redis();
const worker = new MyWorker(redis, 'group-A', 'my-stream');

await worker.start();`;

  const batchWorkerCode = `import { Redis } from 'ioredis';
import { BatchWorker } from '@koala42/redis-highway';

class MyBatchWorker extends BatchWorker<T> {
  async process(data: T[]) {
    // Process array of data items at once
    console.log('Processing batch of size:', data.length);
    await db.insertMany(data);
  }
}

const redis = new Redis();
// 4th arg is BatchWorkerOptions
const worker = new MyBatchWorker(redis, {
    groupName: 'group-A',
    streamName: 'my-stream',
    concurrency: 50,
    batchSize: 100,
    maxFetchCount: 1000
});

await worker.start();`;

  const dlqWorkerCode = `import { Redis } from 'ioredis';
import { DlqWorker, DlqMessageEntity } from '@koala42/redis-highway';

class MyDlqWorker extends DlqWorker<T> {
  async process(message: DlqMessageEntity<T>) {
    console.log('Failed job:', message.data);
    console.log('Error:', message.errorMessage);
    // Log to external system, send alert, etc.
  }
}

const redis = new Redis();
const dlqWorker = new MyDlqWorker(redis, {
  streamName: 'my-stream',
  blockTimeoutMs: 5000,  // Optional
  waitTimeoutMs: 5000    // Optional
});

await dlqWorker.start();`;

  const nestJsCode = `// Producer
@Injectable()
export class EntryService {
  private readonly producer: Producer;
  
  constructor(){
    this.producer = new Producer(
      new Redis(...), // Or reuse existing ioredis connection
      'my-stream'
    )
  }
  
  public async sth(): Promise<void>{
    await this.producer.push(
      { hello: 'world' },
      ['group-A', 'group-B']
    );
  }
}`;

  const metricsCode = `import { Metrics } from '@koala42/redis-highway';

const metrics = new Metrics(redis, 'my-stream');

// Prometheus format
const payload = await metrics.getPrometheusMetrics(['group-A']);`;

  return (
    <div className="min-h-screen bg-background text-foreground selection:bg-white/20">
      <Navbar />

      <main className="container mx-auto px-6 pt-32 pb-24 max-w-4xl">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <h1 className="text-4xl font-bold mb-6">Get Started</h1>
          <p className="text-muted-foreground text-lg mb-12">
            Everything you need to know to get running with Redis Highway.
          </p>

          <section className="mb-16">
            <h2 className="text-2xl font-semibold mb-6 flex items-center gap-2">
              <span className="flex items-center justify-center w-8 h-8 rounded-full bg-white/10 text-sm font-bold">1</span>
              Installation
            </h2>
            <CodeBlock language="bash" code={installCode} />
          </section>

          <section className="mb-16">
            <h2 className="text-2xl font-semibold mb-6 flex items-center gap-2">
              <span className="flex items-center justify-center w-8 h-8 rounded-full bg-white/10 text-sm font-bold">2</span>
              Basic Usage
            </h2>

            <div className="space-y-12">
              <div>
                <h3 className="text-lg font-medium mb-4 text-white">Producer</h3>
                <p className="text-muted-foreground mb-4 text-sm">
                  Create a producer to push messages to the stream.
                  Messages are objects and do not need manual stringification.
                </p>
                <CodeBlock code={producerCode} />
              </div>

              <div>
                <h3 className="text-lg font-medium mb-4 text-white">Worker</h3>
                <p className="text-muted-foreground mb-4 text-sm">
                  Standard Worker processes one message at a time.
                </p>
                <CodeBlock code={workerCode} />
              </div>

              <div>
                <h3 className="text-lg font-medium mb-4 text-white">Batch Worker</h3>
                <p className="text-muted-foreground mb-4 text-sm">
                  Batch Worker processes multiple messages at once for higher throughput (e.g., bulk database inserts).
                </p>
                <CodeBlock code={batchWorkerCode} />
              </div>

              <div>
                <h3 className="text-lg font-medium mb-4 text-white">DLQ Worker</h3>
                <p className="text-muted-foreground mb-4 text-sm">
                  Process messages from the Dead Letter Queue. Use this to handle jobs that have exhausted all retries.
                </p>
                <div className="mb-4 p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20">
                  <p className="text-yellow-400 text-sm">
                    <strong>Warning:</strong> DLQ Worker has no built-in error handling or retry policy.
                    If <code className="bg-white/10 px-1 rounded">process()</code> throws an error, the message is lost.
                    This is by design - DLQ processing is meant for manual intervention, logging, or forwarding to external systems.
                  </p>
                </div>
                <CodeBlock code={dlqWorkerCode} />
              </div>
            </div>
          </section>

          <section className="mb-16">
            <h2 className="text-2xl font-semibold mb-6">Usage with NestJS</h2>
            <p className="text-muted-foreground mb-6">
              Redis Highway works great with NestJS dependency injection.
            </p>
            <CodeBlock code={nestJsCode} />
          </section>

          <section className="mb-16">
            <h2 className="text-2xl font-semibold mb-6">Metrics</h2>
            <p className="text-muted-foreground mb-6">
              Export Prometheus metrics easily.
            </p>
            <CodeBlock code={metricsCode} />
          </section>

          {/* Advanced Options Section */}
          <section className="mb-16 pt-16 border-t border-white/10">
            <h2 className="text-2xl font-bold mb-8">Advanced Options</h2>

            <div className="grid gap-12">
              <div>
                <h3 className="text-xl font-semibold mb-4 text-white">Control Options</h3>
                <p className="text-muted-foreground mb-4">
                  These options control the lifecycle and error handling of any Worker.
                  Pass them as the 3rd argument to `Worker`.
                </p>
                <div className="overflow-x-auto border border-white/10 rounded-xl">
                  <table className="w-full text-sm text-left">
                    <thead className="text-xs uppercase bg-white/5 text-muted-foreground">
                      <tr>
                        <th className="px-6 py-3">Option</th>
                        <th className="px-6 py-3">Type</th>
                        <th className="px-6 py-3">Default</th>
                        <th className="px-6 py-3">Description</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/5 bg-[#0d0d0d]">
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">maxRetries</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">3</td>
                        <td className="px-6 py-4 text-gray-400">Max execution attempts before moving to DLQ.</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">blockTimeMs</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">5000</td>
                        <td className="px-6 py-4 text-gray-400">Redis XREADGROUP block duration.</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">claimIntervalMs</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">10000</td>
                        <td className="px-6 py-4 text-gray-400">Interval for checking stuck messages (AutoClaim).</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">minIdleTimeMs</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">60000</td>
                        <td className="px-6 py-4 text-gray-400">Min time before a message is considered stuck.</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">collectMetrics</td>
                        <td className="px-6 py-4 text-blue-400">boolean</td>
                        <td className="px-6 py-4 text-gray-500">true</td>
                        <td className="px-6 py-4 text-gray-400">Enable/Disable throughput tracking.</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 className="text-xl font-semibold mb-4 text-white">Batch Worker Options</h3>
                <p className="text-muted-foreground mb-4">
                  Specific configuration for `BatchWorker` to tune throughput.
                </p>
                <div className="overflow-x-auto border border-white/10 rounded-xl">
                  <table className="w-full text-sm text-left">
                    <thead className="text-xs uppercase bg-white/5 text-muted-foreground">
                      <tr>
                        <th className="px-6 py-3">Option</th>
                        <th className="px-6 py-3">Type</th>
                        <th className="px-6 py-3">Default</th>
                        <th className="px-6 py-3">Description</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/5 bg-[#0d0d0d]">
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">batchSize</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">-</td>
                        <td className="px-6 py-4 text-gray-400">Number of items to process in a single batch.</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">maxFetchCount</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">-</td>
                        <td className="px-6 py-4 text-gray-400">Max items to fetch from Redis per cycle (should be greater than batchSize).</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 className="text-xl font-semibold mb-4 text-white">DLQ Worker Options</h3>
                <p className="text-muted-foreground mb-4">
                  Configuration for `DlqWorker`. Note: DLQ Worker has no retry policy - if processing fails, the message is lost.
                </p>
                <div className="overflow-x-auto border border-white/10 rounded-xl">
                  <table className="w-full text-sm text-left">
                    <thead className="text-xs uppercase bg-white/5 text-muted-foreground">
                      <tr>
                        <th className="px-6 py-3">Option</th>
                        <th className="px-6 py-3">Type</th>
                        <th className="px-6 py-3">Default</th>
                        <th className="px-6 py-3">Description</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/5 bg-[#0d0d0d]">
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">streamName</td>
                        <td className="px-6 py-4 text-blue-400">string</td>
                        <td className="px-6 py-4 text-gray-500">-</td>
                        <td className="px-6 py-4 text-gray-400">Required. The Redis stream key (same as your main workers).</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">blockTimeoutMs</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">5000</td>
                        <td className="px-6 py-4 text-gray-400">Redis XREADGROUP block duration in milliseconds.</td>
                      </tr>
                      <tr>
                        <td className="px-6 py-4 font-mono text-white">waitTimeoutMs</td>
                        <td className="px-6 py-4 text-blue-400">number</td>
                        <td className="px-6 py-4 text-gray-500">5000</td>
                        <td className="px-6 py-4 text-gray-400">Wait time between cycles when no messages are available.</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        </motion.div>
      </main>
    </div>
  );
};
