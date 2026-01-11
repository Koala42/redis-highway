
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Redis from 'ioredis';
import { Producer } from './producer';
import { Worker } from './worker';
import { Metrics } from './metrics';
import { v7 as uuidv7 } from 'uuid';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

class TestWorker extends Worker {
    public processedCount = 0;
    public lastProcessedId: string | null = null;
    public shouldFail = false;
    public failCount = 0;
    public maxFails = 0;

    constructor(
        redis: Redis,
        groupName: string,
        streamName: string,
        concurrency: number = 1,
        blockTimeMs: number = 100
    ) {
        super(redis, groupName, streamName, concurrency, blockTimeMs);
    }

    async process(data: any): Promise<void> {

        if (this.shouldFail) {
            this.failCount++;

            if (this.maxFails > 0 && this.failCount > this.maxFails) {
                // Stop failing after maxFails
            } else {
                throw new Error("Simulated Failure");
            }
        }

        this.processedCount++;
        if (data && data.id) {
            this.lastProcessedId = data.id;
        }
    }
}

describe('Redis Queue Integration', () => {
    let redis: Redis;
    let producer: Producer;
    let streamName: string;
    let workers: TestWorker[] = [];

    beforeEach(() => {
        redis = new Redis(REDIS_URL);
        streamName = `test-queue-${uuidv7()}`;
        producer = new Producer(redis, streamName);
        workers = [];
    });

    afterEach(async () => {
        for (const w of workers) {
            w.stop();
        }

        await new Promise(r => setTimeout(r, 500));

        // Cleanup Redis keys using the existing connection before closing
        if (redis.status === 'ready') {
            const keys = await redis.keys(`${streamName}*`);
            if (keys.length) await redis.del(...keys);
        }

        redis.disconnect();
    });

    const waitFor = async (condition: () => boolean | Promise<boolean>, timeout = 5000) => {
        const start = Date.now();

        while (Date.now() - start < timeout) {
            if (await condition()) return true;
            await new Promise(r => setTimeout(r, 100));
        }

        return false;
    };

    describe('Core Functionality', () => {
        it('Should deliver message to all target groups', async () => {
            const w1 = new TestWorker(redis, 'group-A', streamName, 1, 100);
            const w2 = new TestWorker(redis, 'group-B', streamName, 1, 100);

            workers.push(w1, w2);

            await w1.start();
            await w2.start();

            const id = await producer.push(JSON.stringify({ id: 'msg-1' }), ['group-A', 'group-B']);

            await waitFor(() => w1.processedCount === 1 && w2.processedCount === 1);

            expect(w1.processedCount).toBe(1);
            expect(w2.processedCount).toBe(1);

            // Test cleanup keys (but not explicit XDEL here yet, checking keys gone)
            const statusKey = `${streamName}:status:${id}`;
            const dataKey = `${streamName}:data:${id}`;

            expect(await redis.exists(statusKey)).toBe(0);
            expect(await redis.exists(dataKey)).toBe(0);
        });

        it('Should only deliver to targeted groups', async () => {
            const wA = new TestWorker(redis, 'group-A', streamName, 1, 100);
            const wB = new TestWorker(redis, 'group-B', streamName, 1, 100);
            workers.push(wA, wB);

            await wA.start();
            await wB.start();

            await producer.push(JSON.stringify({ id: 'msg-only-a' }), ['group-A']);

            await waitFor(() => wA.processedCount === 1);

            expect(wA.processedCount).toBe(1);
            expect(wB.processedCount).toBe(0);
        });

        it('Should retry only the failed group', async () => {
            const wOk = new TestWorker(redis, 'group-Ok', streamName, 1, 100);
            const wFail = new TestWorker(redis, 'group-Fail', streamName, 1, 100);

            wFail.shouldFail = true;
            wFail.maxFails = 1; // Fail once, then succeed

            workers.push(wOk, wFail);
            await wOk.start();
            await wFail.start();

            await producer.push(JSON.stringify({ id: 'retry-test' }), ['group-Ok', 'group-Fail']);

            // Wait for wOk to finish and wFail to try at least twice (fail + success)
            await waitFor(() => wOk.processedCount === 1 && wFail.processedCount === 1, 8000);

            expect(wOk.processedCount).toBe(1); // Processed once
            expect(wFail.failCount).toBeGreaterThanOrEqual(1); // Failed at least once
            expect(wFail.processedCount).toBe(1); // Eventually succeeded


            expect(wOk.processedCount).toBe(1); // wOk should NOT process the retry
        });


        it('Should move to DLQ after max retries', async () => {
            const wDead = new TestWorker(redis, 'group-Dead', streamName, 1, 100);
            wDead.shouldFail = true;
            wDead.maxFails = 10; // Fail forever (more than max retries which is 3)

            workers.push(wDead);
            await wDead.start();

            const id = await producer.push(JSON.stringify({ id: 'dlq-test' }), ['group-Dead']);

            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            }, 10000);

            const dlqLen = await redis.xlen(`${streamName}:dlq`);
            expect(dlqLen).toBe(1);

            expect(await redis.exists(`${streamName}:status:${id}`)).toBe(0);
        });
    });

    describe('Metrics & Monitoring', () => {
        it('Should track throughput and queue size', async () => {
            const w = new TestWorker(redis, 'group-Metrics', streamName, 1, 100);
            const metricsService = new Metrics(redis, streamName);

            workers.push(w);
            await w.start();

            let metrics = await metricsService.getMetrics(['group-Metrics']);
            expect(metrics.dlqLength).toBe(0);

            const id = await producer.push(JSON.stringify({ id: 'metrics-1' }), ['group-Metrics']);

            await waitFor(() => w.processedCount === 1);

            metrics = await metricsService.getMetrics(['group-Metrics']);
            expect(metrics.throughput['group-Metrics']).toBeGreaterThanOrEqual(1);

            w.shouldFail = true;
            w.maxFails = 10;
            await producer.push(JSON.stringify({ id: 'metrics-fail' }), ['group-Metrics']);

            await waitFor(() => redis.xlen(`${streamName}:dlq`).then(len => len > 0));

            metrics = await metricsService.getMetrics(['group-Metrics']);
            expect(metrics.dlqLength).toBe(1);
        });

        it('Should export Prometheus metrics', async () => {
            const w = new TestWorker(redis, 'group-Prom', streamName, 1, 100);
            const metricsService = new Metrics(redis, streamName);

            workers.push(w);
            await w.start();

            await producer.push(JSON.stringify({ id: 'prom-1' }), ['group-Prom']);
            await producer.push(JSON.stringify({ id: 'prom-2' }), ['group-Prom']);

            await waitFor(() => w.processedCount === 2);

            const promOutput = await metricsService.getPrometheusMetrics(['group-Prom']);

            expect(promOutput).toContain(`# TYPE redis_highway_queue_throughput_1m gauge`);
            expect(promOutput).toContain(`redis_highway_queue_throughput_1m{stream="${streamName}", group="group-Prom"} 2`);

            expect(promOutput).toContain(`# TYPE redis_highway_queue_jobs_total counter`);
            expect(promOutput).toContain(`redis_highway_queue_jobs_total{stream="${streamName}", group="group-Prom"} 2`);

            expect(promOutput).toContain(`# TYPE redis_highway_queue_waiting_jobs gauge`);
        });
    });

    describe('Stream Cleanup', () => {
        it('Should delete message from stream after processing', async () => {
            const w1 = new TestWorker(redis, 'group-A', streamName, 1, 100);
            workers.push(w1);
            await w1.start();

            const id = await producer.push(JSON.stringify({ id: 'msg-cleanup' }), ['group-A']);

            // Wait for processing
            await waitFor(() => w1.processedCount === 1);
            expect(w1.processedCount).toBe(1);

            // Wait for stream to be empty
            const success = await waitFor(async () => {
                const len = await redis.xlen(streamName);
                return len === 0;
            });

            expect(success).toBe(true);

            const messages = await redis.xrange(streamName, '-', '+');
            expect(messages.length).toBe(0);
        });

        it('Should delete message from stream only after ALL groups processed it', async () => {
            const w1 = new TestWorker(redis, 'group-A', streamName, 1, 100);
            const w2 = new TestWorker(redis, 'group-B', streamName, 1, 100);
            workers.push(w1, w2);

            await w1.start(); // Only start w1

            const id = await producer.push(JSON.stringify({ id: 'msg-multi' }), ['group-A', 'group-B']);

            // Wait for w1 to process
            await waitFor(() => w1.processedCount === 1);

            let len = await redis.xlen(streamName);
            expect(len).toBe(1); // Should still exist because group-B pending

            // Start w2
            await w2.start();

            // Wait for w2 to process
            await waitFor(() => w2.processedCount === 1);

            // Wait for stream to be empty
            const success = await waitFor(async () => {
                len = await redis.xlen(streamName);
                return len === 0;
            });

            expect(success).toBe(true);
        });
    });
});
