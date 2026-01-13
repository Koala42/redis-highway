export type StreamMessage = [string, string[]];
export type XReadGroupResponse = [string, StreamMessage[]][];


export interface BaseWorkerOptions {
  groupName: string;
  streamName: string;
  concurrency: number;
}

export interface BaseWorkerControlOptions {
  maxRetries: number;
  blockTimeMs: number;
  claimIntervalMs: number;
  minIdleTimeMs: number;
  collectMetrics: boolean;
}

export const defaultBaseWorkerControlOptions: BaseWorkerControlOptions = {
  maxRetries: 3,
  minIdleTimeMs: 120_000,
  blockTimeMs: 2_000,
  claimIntervalMs: 120_000,
  collectMetrics: true
}

export interface BatchWorkerOptions extends BaseWorkerOptions {
  batchSize: number;
  maxFetchCount: number
}
