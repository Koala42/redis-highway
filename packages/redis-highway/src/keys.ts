export class KeyManager {
    constructor(private readonly streamName: string) { }

    getStreamKey(): string {
        return this.streamName;
    }

    /**
     * Job status stores information about Job
     * How many targets should consume it
     * And targets add their completed timestamps there
     */
    getJobStatusKey(id: string): string {
        return `${this.streamName}:status:${id}`;
    }

    /**
     * Dead letter queue stream name
     */
    getDlqStreamKey(): string {
        return `${this.streamName}:dlq`;
    }

    /**
     * Metrics for storing throughput
     * @param current - if true, takes throughput in the running minute, if false, takes -1 minute for closed throughput bucket
     */
    getThroughputKey(groupName: string, timestamp: number, current: boolean = true): string {
        const minute = Math.floor(timestamp / 60_000) * 60_000;
        return `metrics:throughput:${this.streamName}:${groupName}:${current ? minute : minute - 60_000}`;
    }

    /**
     * Metrics - retries key
     */
    getRetriesKey(groupName: string, timestamp: number): string {
        const minute = Math.floor(timestamp / 60000) * 60000;
        return `metrics:retry-count:${this.streamName}:${groupName}:${minute}`;
    }

    /**
     * Total jobs processed metrics
     */
    getTotalKey(groupName: string): string {
        return `metrics:total:${this.streamName}:${groupName}`;
    }
}
