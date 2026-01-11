export class KeyManager {
    constructor(private readonly streamName: string) { }

    getStreamKey(): string {
        return this.streamName;
    }

    getJobStatusKey(id: string): string {
        return `${this.streamName}:status:${id}`;
    }

    getJobDataKey(id: string): string {
        return `${this.streamName}:data:${id}`;
    }

    getDlqStreamKey(): string {
        return `${this.streamName}:dlq`;
    }

    getThroughputKey(groupName: string, timestamp: number): string {
        const minute = Math.floor(timestamp / 60000) * 60000;
        return `metrics:throughput:${this.streamName}:${groupName}:${minute}`;
    }

    getTotalKey(groupName: string): string {
        return `metrics:total:${this.streamName}:${groupName}`;
    }
}
