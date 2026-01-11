import { Redis } from 'ioredis';
export interface QueueMetrics {
    streamLength: number;
    dlqLength: number;
    throughput: Record<string, number>;
}
export declare class Metrics {
    private readonly redis;
    private readonly streamName;
    private keys;
    constructor(redis: Redis, streamName: string);
    /**
     * Get current metrics for the queue
     * @param groupNames - List of consumer groups to fetch throughput for
     */
    getMetrics(groupNames: string[]): Promise<QueueMetrics>;
    /**
     * Get prometheus compatible metrics
     * @param groupNames target group names for throughput metrics
     * @param prefix - export prefix
     * @returns metrics as string
     */
    getPrometheusMetrics(groupNames: string[], prefix?: string): Promise<string>;
}
