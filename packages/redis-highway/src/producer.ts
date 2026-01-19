import { Redis, Pipeline } from 'ioredis'
import { v7 as uuidv7 } from 'uuid'
import { KeyManager } from './keys'
import { StreamMessageEntity } from './stream-message-entity';

export interface JobOptions {
  ttl?: number | null;
}


export class Producer<T extends Record<string, unknown>> {
  private keys: KeyManager;

  constructor(private readonly redis: Redis, private readonly streamName: string) {
    this.keys = new KeyManager(streamName);
  }

  /**
   * Push message to queue
   * @param payload
   * @param targetGroups - target consumers
   * @param opts - Job options
   * @returns Created job ID (uuidv7)
   */
  async push(payload: T, targetGroups: string[], opts?: JobOptions): Promise<string> {
    const serializedPayload = JSON.stringify(payload)
    const id = uuidv7()
    const ttl = opts?.ttl || null; // Defaults to null, no expiry

    const pipeline = this.redis.pipeline()
    const statusKey = this.keys.getJobStatusKey(id);

    // Initialize job metadata - status
    pipeline.hset(statusKey, '__target', targetGroups.length)
    if (ttl) {
      pipeline.expire(statusKey, ttl)
    }

    // Push message to stream
    pipeline.xadd(
      this.streamName,
      '*',
      ...StreamMessageEntity.getStreamFields(id, targetGroups, serializedPayload)
    )

    await pipeline.exec()
    return id
  }
}
