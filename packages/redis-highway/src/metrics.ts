import { Redis } from 'ioredis';
import { KeyManager } from './keys';

export interface QueueMetrics {
    streamLength: number;
    dlqLength: number;
    throughput: Record<string, number>;
}

export class Metrics {
    private keys: KeyManager;

    constructor(private readonly redis: Redis, private readonly streamName: string) {
        this.keys = new KeyManager(streamName);
    }

    /**
     * Get current metrics for the queue
     * @param groupNames - List of consumer groups to fetch throughput for
     */
    async getMetrics(groupNames: string[], current: boolean = false): Promise<QueueMetrics> {
        const pipeline = this.redis.pipeline();

        pipeline.xlen(this.streamName);
        pipeline.xlen(this.keys.getDlqStreamKey());

        const timestamp = Date.now();
        groupNames.forEach(group => {
            pipeline.get(this.keys.getThroughputKey(group, timestamp, current));
        });

        const results = await pipeline.exec();

        if (!results) {
            throw new Error("Pipeline execution failed");
        }

        const getResult = (index: number) => {
            const [err, res] = results[index];
            if (err) throw err;
            return res;
        };

        const streamLength = getResult(0) as number;
        const dlqLength = getResult(1) as number;

        const throughput: Record<string, number> = {};
        groupNames.forEach((group, index) => {
            const val = getResult(index + 2);
            throughput[group] = parseInt((val as string) || '0', 10);
        });

        return {
            streamLength,
            dlqLength,
            throughput
        };
    }

    /**
     * Get prometheus compatible metrics
     * @param groupNames target group names for throughput metrics
     * @param prefix - export prefix
     * @returns metrics as string
     */
    public async getPrometheusMetrics(groupNames: string[], prefix: string = 'redis_highway_queue', current: boolean = false): Promise<string> {
        const pipeline = this.redis.pipeline();

        pipeline.xlen(this.streamName);
        pipeline.xlen(this.keys.getDlqStreamKey());

        const timestamp = Date.now();
        groupNames.forEach(group => {
            pipeline.get(this.keys.getThroughputKey(group, timestamp, current));
            pipeline.get(this.keys.getTotalKey(group));
            pipeline.get(this.keys.getRetriesKey(group, timestamp))
        });

        const results = await pipeline.exec();
        if (!results) throw new Error("Pipeline execution failed");

        const getResult = (index: number) => {
            const [err, res] = results[index];
            if (err) throw err;
            return res;
        };

        const streamLength = getResult(0) as number;
        const dlqLength = getResult(1) as number;

        const response: string[] = [];

        response.push(
            `# HELP ${prefix}_waiting_jobs Total jobs waiting in stream`,
            `# TYPE ${prefix}_waiting_jobs gauge`,
            `${prefix}_waiting_jobs{stream="${this.streamName}"} ${streamLength}`,

            `# HELP ${prefix}_dlq_jobs Total jobs in DLQ`,
            `# TYPE ${prefix}_dlq_jobs gauge`,
            `${prefix}_dlq_jobs{stream="${this.streamName}"} ${dlqLength}`
        );

        groupNames.forEach((group, index) => {
            const baseIndex = 2 + (index * 3);

            const throughputVal = parseInt((getResult(baseIndex) as string) || '0', 10);
            const totalVal = parseInt((getResult(baseIndex + 1) as string) || '0', 10);
            const retryCountVal = parseInt((getResult(baseIndex + 2) as string) || '0', 10);

            response.push(
                `# HELP ${prefix}_throughput_1m Jobs processed in the last minute`,
                `# TYPE ${prefix}_throughput_1m gauge`,
                `${prefix}_throughput_1m{stream="${this.streamName}", group="${group}"} ${throughputVal}`,

                `# HELP ${prefix}_retries_1m Retries in the last minute`,
                `# TYPE ${prefix}_retries_1m gauge`,
                `${prefix}_retries_1m{stream="${this.streamName}", group="${group}"} ${retryCountVal}`,

                `# HELP ${prefix}_jobs_total Total jobs processed`,
                `# TYPE ${prefix}_jobs_total counter`,
                `${prefix}_jobs_total{stream="${this.streamName}", group="${group}"} ${totalVal}`
            );
        });

        return response.join('\n');
    }
}
