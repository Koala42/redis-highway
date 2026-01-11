import { Redis, Pipeline } from 'ioredis'
import { v7 as uuidv7 } from 'uuid'
import { KeyManager } from './keys'

export interface JobOptions {
  ttl?: number | null;
  streamName?: string;
}


export class Producer {
  private keys: KeyManager;

  constructor(private readonly redis: Redis, private readonly streamName: string) {
    this.keys = new KeyManager(streamName);
  }

  /**
   * Push message to queue
   * @param payload - serialized string payload
   * @param targetGroups - target consumers
   * @param opts - Job options
   * @returns Created job ID (uuidv7)
   */
  async push(payload: string, targetGroups: string[], opts?: JobOptions): Promise<string> {
    const id = uuidv7()
    const ttl = opts?.ttl || null; // 24 hours in seconds

    const pipeline = this.redis.pipeline()

    const dataKey = this.keys.getJobDataKey(id);
    const statusKey = this.keys.getJobStatusKey(id);

    // Create job data
    if (ttl) {
      pipeline.set(dataKey, payload, 'EX', ttl)
    } else {
      pipeline.set(dataKey, payload)
    }

    // Initialize job metadata - status
    pipeline.hset(statusKey, '__target', targetGroups.length)
    if (ttl) {
      pipeline.expire(statusKey, ttl)
    }

    // Push message to stream
    pipeline.xadd(
      this.streamName,
      '*',
      'id',
      id,
      'target',
      targetGroups.join(',')
    )

    await pipeline.exec()
    return id
  }
}
