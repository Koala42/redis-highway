import Redis from "ioredis";
export declare abstract class Worker<T = any> {
    protected redis: Redis;
    protected groupName: string;
    protected streamName: string;
    protected concurrency: number;
    protected blockTimeMs: number;
    private isRunning;
    private activeCount;
    private readonly events;
    private keys;
    private readonly MAX_RETRIES;
    constructor(redis: Redis, groupName: string, streamName: string, concurrency?: number, blockTimeMs?: number);
    /**
     * Start worker
     * @returns
     */
    start(): Promise<void>;
    stop(): void;
    private fetchLoop;
    private spawnWorker;
    private processInternal;
    private handleFailure;
    private finalize;
    private consumerName;
    abstract process(data: T): Promise<void>;
}
