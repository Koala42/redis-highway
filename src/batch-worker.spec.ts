
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { Producer } from './producer';
import { BatchWorker } from './batch-worker';
import { v7 as uuidv7 } from 'uuid';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

type JobData = { id: string; }

class TestBatchWorker extends BatchWorker<JobData> {
    public processedBatches: JobData[][] = [];
    public shouldFail = false;
    public failCount = 0;
    public maxFails = 0;

    constructor(
        redis: Redis,
        groupName: string,
        streamName: string,
        batchSize = 10,
        concurrency: number = 1,
        maxRetries = 3,
        blockTimeMs: number = 100
    ) {
        // Fix argument order: batchSize, concurrency, maxFetchSize, maxRetries, blockTimeMs
        super(redis, groupName, streamName, batchSize, concurrency, 20, maxRetries, blockTimeMs);
    }

    async process(data: JobData[]): Promise<void> {
        if (this.shouldFail) {
            this.failCount++;
            if (this.maxFails > 0 && this.failCount > this.maxFails) {
                // Stop failing
            } else {
                throw new Error("Simulated Batch Failure");
            }
        }
        this.processedBatches.push(data);
    }
}

describe('Batch Worker Integration', () => {
    let redisProducer: Redis;
    let redisWorker: Redis;
    let producer: Producer<JobData>;
    let streamName: string;
    let worker: TestBatchWorker;

    beforeEach(() => {
        // Use separate connections to avoid XREAD BLOCK blocking the producer
        redisProducer = new Redis(REDIS_URL);
        redisWorker = new Redis(REDIS_URL);

        streamName = `test-batch-queue-${uuidv7()}`;
        producer = new Producer(redisProducer, streamName);
    });

    afterEach(async () => {
        if (worker) await worker.stop();

        // Wait a bit
        await new Promise(r => setTimeout(r, 100));

        // Cleanup
        if (redisProducer.status === 'ready') {
            const keys = await redisProducer.keys(`${streamName}*`);
            if (keys.length) await redisProducer.del(...keys);
        }

        redisProducer.disconnect();
        redisWorker.disconnect();
    });

    const waitFor = async (condition: () => boolean | Promise<boolean>, timeout = 8000) => {
        const start = Date.now();
        while (Date.now() - start < timeout) {
            if (await condition()) return true;
            await new Promise(r => setTimeout(r, 100));
        }
        return false;
    };

    it('Should process a batch of messages', async () => {
        worker = new TestBatchWorker(redisWorker, 'group-Batch', streamName, 5, 1);

        // Push messages BEFORE starting worker to ensure a full batch is available
        // BatchWorker uses '0' pointer for group creation, so it will pick up existing messages
        for (let i = 0; i < 5; i++) {
            await producer.push({ id: `msg-${i}` }, ['group-Batch']);
        }

        await worker.start();

        await waitFor(() => worker.processedBatches.length > 0);

        expect(worker.processedBatches.length).toBeGreaterThanOrEqual(1);
        // It might still split it if redis pagination behaves weirdly, but usually it grabs Count
        const allProcessed = worker.processedBatches.flat();
        expect(allProcessed.length).toBe(5);
        expect(worker.processedBatches[0].length).toBe(5);
        expect(worker.processedBatches[0].map(j => j.id)).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4']);
    });

    it('Should process multiple batches', async () => {
        worker = new TestBatchWorker(redisWorker, 'group-Batch-Multi', streamName, 2, 1);

        for (let i = 0; i < 4; i++) {
            await producer.push({ id: `msg-${i}` }, ['group-Batch-Multi']);
        }

        await worker.start();

        await waitFor(() => worker.processedBatches.flat().length === 4);

        expect(worker.processedBatches.length).toBe(2);
    });

    it('Should retry failed batches', async () => {
        worker = new TestBatchWorker(redisWorker, 'group-Batch-Retry', streamName, 5, 1, 3);
        worker.shouldFail = true;
        worker.maxFails = 1;

        await worker.start();
        await producer.push({ id: 'retry-1' }, ['group-Batch-Retry']);

        await waitFor(() => worker.processedBatches.length === 1);

        expect(worker.failCount).toBeGreaterThanOrEqual(1);
        expect(worker.processedBatches.length).toBe(1);
        expect(worker.processedBatches[0][0].id).toBe('retry-1');
    });

    it('Should move to DLQ after max retries', async () => {
        worker = new TestBatchWorker(redisWorker, 'group-Batch-DLQ', streamName, 1, 1, 2);
        worker.shouldFail = true;
        worker.maxFails = 10;

        await worker.start();
        const id = await producer.push({ id: 'dlq-1' }, ['group-Batch-DLQ']);

        await waitFor(async () => {
            const len = await redisProducer.xlen(`${streamName}:dlq`);
            return len > 0;
        }, 10000);

        const dlqLen = await redisProducer.xlen(`${streamName}:dlq`);
        expect(dlqLen).toBe(1);

        const dlqMsgs = await redisProducer.xrange(`${streamName}:dlq`, '-', '+');
        const body = dlqMsgs[0][1];
        const payloadIdx = body.indexOf('payload');
        const payload = body[payloadIdx + 1];
        expect(JSON.parse(payload)).toEqual({ id: 'dlq-1' });
    });
});
