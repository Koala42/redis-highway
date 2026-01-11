import { Redis } from 'ioredis';
export interface JobOptions {
    ttl?: number | null;
    streamName?: string;
}
export declare class Producer {
    private readonly redis;
    private readonly streamName;
    private keys;
    constructor(redis: Redis, streamName: string);
    /**
     * Push message to queue
     * @param payload - serialized string payload
     * @param targetGroups - target consumers
     * @param opts - Job options
     * @returns Created job ID (uuidv7)
     */
    push(payload: string, targetGroups: string[], opts?: JobOptions): Promise<string>;
}
