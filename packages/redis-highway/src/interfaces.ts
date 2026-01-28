import { ChainableCommander } from "ioredis";

export type StreamMessage = [string, string[]];
export type XReadGroupResponse = [string, StreamMessage[]][];


export interface BaseWorkerOptions {
  groupName: string;
  streamName: string;
  concurrency: number;
}

export interface DlqWorkerOptions {
  streamName: string;
  blockTimeoutMs?: number; // Defaults to 5 seconds (5_000)
  waitTimeoutMs?: number; // Defaults to 5 seconds (5_000)
}

export interface BaseWorkerControlOptions {
  maxRetries: number;
  blockTimeMs: number;
  claimIntervalMs: number;
  minIdleTimeMs: number;
  collectMetrics: boolean;
}

export interface BaseWorkerCustomMetricsOptions<T extends Record<string, unknown>> {
  finalIncrementMetricKey?: ((item: T) => string | null) | null
}

export const defaultBaseWorkerCustomMetrics: BaseWorkerCustomMetricsOptions<{}> = {
  finalIncrementMetricKey: null
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


export interface ProducerOptions {
  streamName: string;
  compression: boolean;
}
