import { Redis, Pipeline } from 'ioredis'
import { v7 as uuidv7 } from 'uuid'
import { KeyManager } from './keys'
import { StreamMessageEntity } from './stream-message-entity';
import { ProducerOptions } from './interfaces';
import { Serializer } from './serializer';

export interface JobOptions {
  ttl?: number | null;
}


export class Producer<T extends Record<string, unknown>> {
  private readonly _keys: KeyManager;
  private readonly _redis: Redis;
  private readonly _streamName: string;
  private readonly _compression: boolean;

  constructor(redis: Redis, options: ProducerOptions) {
    this._redis = redis;
    this._streamName = options.streamName
    this._keys = new KeyManager(this._streamName);
    this._compression = options.compression
  }

  /**
   * Push message to queue
   * @param payload
   * @param targetGroups - target consumers
   * @param opts - Job options
   * @returns Created job ID (uuidv7)
   */
  async push(payload: T, targetGroups: string[], opts?: JobOptions): Promise<string> {
    const id = uuidv7()
    const ttl = opts?.ttl || null; // Defaults to null, no expiry

    const pipeline = this._redis.pipeline()
    const statusKey = this._keys.getJobStatusKey(id);

    // Initialize job metadata - status
    pipeline.hset(statusKey, '__target', targetGroups.length)
    if (ttl) {
      pipeline.expire(statusKey, ttl)
    }

    const serializedPayload = this._compression ? await Serializer.compressPayload(payload) : JSON.stringify(payload);

    // Push message to stream
    pipeline.xadd(
      this._streamName,
      '*',
      ...StreamMessageEntity.getStreamFields(id, targetGroups, serializedPayload, this._compression)
    )

    await pipeline.exec()
    return id
  }
}
