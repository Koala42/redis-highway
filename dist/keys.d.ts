export declare class KeyManager {
    private readonly streamName;
    constructor(streamName: string);
    getStreamKey(): string;
    getJobStatusKey(id: string): string;
    getJobDataKey(id: string): string;
    getDlqStreamKey(): string;
    getThroughputKey(groupName: string, timestamp: number): string;
    getTotalKey(groupName: string): string;
}
