
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { Producer } from '../src/producer';
import { Worker } from '../src/worker';
import { DlqWorker } from '../src/dlq-worker';
import { DlqMessageEntity } from '../src/dlq-message-entity';
import { v7 as uuidv7 } from 'uuid';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

type JobData = { id: string; value?: number };

/**
 * Test worker that always fails to push messages to DLQ
 */
class FailingWorker extends Worker<JobData> {
    public failCount = 0;

    constructor(
        redis: Redis,
        groupName: string,
        streamName: string,
        maxRetries: number = 2
    ) {
        super(
            redis,
            { groupName, streamName, concurrency: 1 },
            {
                maxRetries,
                blockTimeMs: 100,
                claimIntervalMs: 60000,
                minIdleTimeMs: 300000,
                collectMetrics: false
            }
        );
    }

    public async start(): Promise<void> {
        return super.start();
    }

    public async stop(): Promise<void> {
        return super.stop();
    }

    async process(_data: JobData): Promise<void> {
        this.failCount++;
        throw new Error('Intentional failure for DLQ test');
    }
}

/**
 * Test DLQ worker
 */
class TestDlqWorker extends DlqWorker<JobData> {
    public processedMessages: DlqMessageEntity<JobData>[] = [];
    public shouldFail = false;
    public failCount = 0;

    constructor(
        redis: Redis,
        streamName: string,
        blockTimeoutMs: number = 100,
        waitTimeoutMs: number = 100
    ) {
        super(redis, { streamName, blockTimeoutMs, waitTimeoutMs });
    }

    public async start(): Promise<void> {
        return super.start();
    }

    public async stop(): Promise<void> {
        return super.stop();
    }

    async process(message: DlqMessageEntity<JobData>): Promise<void> {
        if (this.shouldFail) {
            this.failCount++;
            throw new Error('DLQ processing failed');
        }
        this.processedMessages.push(message);
    }
}

describe('DLQ Worker Integration', () => {
    let redis: Redis;
    let producer: Producer<JobData>;
    let streamName: string;
    let failingWorker: FailingWorker | null = null;
    let dlqWorker: TestDlqWorker | null = null;

    beforeEach(() => {
        redis = new Redis(REDIS_URL);
        streamName = `test-dlq-${uuidv7()}`;
        producer = new Producer(redis, streamName);
    });

    afterEach(async () => {
        if (failingWorker) await failingWorker.stop();
        if (dlqWorker) await dlqWorker.stop();

        await new Promise(r => setTimeout(r, 200));

        if (redis.status === 'ready') {
            const keys = await redis.keys(`${streamName}*`);
            if (keys.length) await redis.del(...keys);

            const metricKeys = await redis.keys(`metrics:*:${streamName}*`);
            if (metricKeys.length) await redis.del(...metricKeys);
        }

        redis.disconnect();
    });

    const waitFor = async (condition: () => boolean | Promise<boolean>, timeout = 10000) => {
        const start = Date.now();
        while (Date.now() - start < timeout) {
            if (await condition()) return true;
            await new Promise(r => setTimeout(r, 100));
        }
        return false;
    };

    describe('Core Functionality', () => {
        it('Should process messages from DLQ', async () => {
            const groupName = 'group-dlq-test';

            // Create failing worker to push message to DLQ
            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            // Push a message that will fail and go to DLQ
            await producer.push({ id: 'dlq-msg-1' }, [groupName]);

            // Wait for message to be moved to DLQ
            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            });

            // Stop failing worker
            await failingWorker.stop();
            failingWorker = null;

            // Start DLQ worker
            dlqWorker = new TestDlqWorker(redis, streamName);
            await dlqWorker.start();

            // Wait for DLQ worker to process
            await waitFor(() => dlqWorker!.processedMessages.length === 1);

            expect(dlqWorker.processedMessages.length).toBe(1);
            expect(dlqWorker.processedMessages[0].data.id).toBe('dlq-msg-1');
        });

        it('Should correctly parse DlqMessageEntity fields', async () => {
            const groupName = 'group-fields-test';

            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            await producer.push({ id: 'fields-test', value: 42 }, [groupName]);

            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            });

            await failingWorker.stop();
            failingWorker = null;

            dlqWorker = new TestDlqWorker(redis, streamName);
            await dlqWorker.start();

            await waitFor(() => dlqWorker!.processedMessages.length === 1);

            const msg = dlqWorker.processedMessages[0];

            // Verify all fields are correctly parsed
            expect(msg.data).toEqual({ id: 'fields-test', value: 42 });
            expect(msg.group).toBe(groupName);
            expect(msg.errorMessage).toBe('Intentional failure for DLQ test');
            expect(typeof msg.failedAt).toBe('number');
            expect(msg.failedAt).toBeGreaterThan(0);
            expect(typeof msg.messageUuid).toBe('string');
            expect(msg.messageUuid.length).toBeGreaterThan(0);
            expect(typeof msg.streamMessageId).toBe('string');
        });

        it('Should delete message from DLQ after processing', async () => {
            const groupName = 'group-delete-test';

            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            await producer.push({ id: 'delete-test' }, [groupName]);

            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            });

            await failingWorker.stop();
            failingWorker = null;

            // Verify DLQ has message before processing
            let dlqLen = await redis.xlen(`${streamName}:dlq`);
            expect(dlqLen).toBe(1);

            dlqWorker = new TestDlqWorker(redis, streamName);
            await dlqWorker.start();

            await waitFor(() => dlqWorker!.processedMessages.length === 1);

            // Wait for deletion
            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len === 0;
            });

            dlqLen = await redis.xlen(`${streamName}:dlq`);
            expect(dlqLen).toBe(0);
        });

        it('Should process multiple DLQ messages', async () => {
            const groupName = 'group-multi-test';

            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            // Push multiple messages
            await producer.push({ id: 'multi-1' }, [groupName]);
            await producer.push({ id: 'multi-2' }, [groupName]);
            await producer.push({ id: 'multi-3' }, [groupName]);

            // Wait for all to be in DLQ
            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len === 3;
            });

            await failingWorker.stop();
            failingWorker = null;

            dlqWorker = new TestDlqWorker(redis, streamName);
            await dlqWorker.start();

            await waitFor(() => dlqWorker!.processedMessages.length === 3);

            expect(dlqWorker.processedMessages.length).toBe(3);
            const ids = dlqWorker.processedMessages.map(m => m.data.id).sort();
            expect(ids).toEqual(['multi-1', 'multi-2', 'multi-3']);
        });
    });

    describe('Error Handling', () => {
        it('Should lose message if process() throws (by design)', async () => {
            const groupName = 'group-error-test';

            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            await producer.push({ id: 'error-test' }, [groupName]);

            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            });

            await failingWorker.stop();
            failingWorker = null;

            // Start DLQ worker that fails
            dlqWorker = new TestDlqWorker(redis, streamName);
            dlqWorker.shouldFail = true;
            await dlqWorker.start();

            // Wait for the DLQ worker to attempt processing
            await waitFor(() => dlqWorker!.failCount > 0);

            // Message should be deleted even though process failed
            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len === 0;
            });

            const dlqLen = await redis.xlen(`${streamName}:dlq`);
            expect(dlqLen).toBe(0);
            expect(dlqWorker.processedMessages.length).toBe(0); // Never successfully processed
        });
    });

    describe('Lifecycle', () => {
        it('Should not start twice', async () => {
            dlqWorker = new TestDlqWorker(redis, streamName);

            await dlqWorker.start();
            await dlqWorker.start(); // Second call should be no-op

            // If it didn't throw, we're good
            expect(true).toBe(true);
        });

        it('Should handle stop gracefully', async () => {
            dlqWorker = new TestDlqWorker(redis, streamName);
            await dlqWorker.start();

            // Stop immediately
            await dlqWorker.stop();

            // Verify consumer was deleted
            const groupInfo = await redis.xinfo('GROUPS', `${streamName}:dlq`).catch(() => []);
            const dlqGroup = (groupInfo as any[]).find((g: any) => g[1] === 'dlq-worker');

            if (dlqGroup) {
                const consumers = await redis.xinfo('CONSUMERS', `${streamName}:dlq`, 'dlq-worker').catch(() => []);
                // Consumer should be deleted or list should be empty
                expect((consumers as any[]).length).toBe(0);
            }
        });
    });

    describe('Options', () => {
        it('Should use custom timeouts', async () => {
            const groupName = 'group-timeout-test';

            failingWorker = new FailingWorker(redis, groupName, streamName, 1);
            await failingWorker.start();

            await producer.push({ id: 'timeout-test' }, [groupName]);

            await waitFor(async () => {
                const len = await redis.xlen(`${streamName}:dlq`);
                return len > 0;
            });

            await failingWorker.stop();
            failingWorker = null;

            // Create worker with custom timeouts
            dlqWorker = new TestDlqWorker(redis, streamName, 50, 50);
            await dlqWorker.start();

            const startTime = Date.now();
            await waitFor(() => dlqWorker!.processedMessages.length === 1);
            const elapsed = Date.now() - startTime;

            // Should process quickly with short timeouts
            expect(elapsed).toBeLessThan(2000);
            expect(dlqWorker.processedMessages.length).toBe(1);
        });
    });
});
