import Redis from "ioredis";
import { EventEmitter } from "events";
import { LUA_MARK_DONE } from "./lua";
import { KeyManager } from "./keys";
import { XReadGroupResponse, StreamMessage } from "./interfaces";
import { StreamMessageEntity } from "./stream-message-entity";
import { v7 as uuidv7 } from 'uuid';

export abstract class Worker<T extends Record<string, unknown>> {
  private isRunning = false;
  private activeCount = 0;
  private readonly events = new EventEmitter()
  private keys: KeyManager;
  private consumerId = uuidv7()

  constructor(
    protected redis: Redis,
    protected groupName: string,
    protected streamName: string,
    protected concurrency: number = 1,
    protected MAX_RETRIES = 3,
    protected blockTimeMs: number = 2000
  ) {
    this.events.setMaxListeners(100)
    this.keys = new KeyManager(streamName);
  }


  /**
   * Start worker
   * @returns
   */
  public async start(): Promise<void> {
    if (this.isRunning) {
      return
    }
    this.isRunning = true;

    try {
      await this.redis.xgroup('CREATE', this.streamName, this.groupName, '0', 'MKSTREAM')
    } catch (e: any) {
      if (!e.message.includes('BUSYGROUP')) {
        throw e
      }
    }

    this.fetchLoop()
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.events.emit('job_finished'); // Wake up fetch loop if it's waiting

    // Wait for active jobs to finish
    while (this.activeCount > 0) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }

  private async fetchLoop(): Promise<void> {
    while (this.isRunning) {
      const freeSlots = this.concurrency - this.activeCount

      if (freeSlots <= 0) {
        await new Promise((resolve) => this.events.once('job_finished', resolve))
        continue
      }

      try {
        const results = await this.redis.xreadgroup(
          'GROUP', this.groupName, this.consumerName(),
          'COUNT', freeSlots,
          'BLOCK', this.blockTimeMs,
          'STREAMS', this.streamName, '>'
        ) as unknown as XReadGroupResponse | null;

        if (results) {
          const messages = results[0][1]
          for (const msg of messages) {
            this.spawnWorker(msg);
          }
        }
      } catch (err) {
        console.error(`[${this.groupName}] Fetch Error:`, err)
        await new Promise((resolve) => setTimeout(resolve, 1_000))
      }
    }
  }

  private spawnWorker(msg: StreamMessage): void {
    this.activeCount++

    this.processInternal(msg).finally(() => {
      this.activeCount--
      this.events.emit('job_finished')
    })
  }


  private async processInternal(msg: StreamMessage): Promise<void> {
    const streamMessage = new StreamMessageEntity(msg)

    if (!streamMessage.routes.includes(this.groupName)) {
      await this.redis.xack(this.streamName, this.groupName, streamMessage.streamMessageId)
      return;
    }

    try {
      const dataKey = this.keys.getJobDataKey(streamMessage.messageUuid);
      const payload = await this.redis.get(dataKey)
      if (!payload) {
        // Data missing or expired
        await this.finalize(streamMessage.messageUuid, streamMessage.streamMessageId)
        return
      }

      await this.process(JSON.parse(payload) as T);

      await this.finalize(streamMessage.messageUuid, streamMessage.streamMessageId)
    } catch (err: any) {
      console.error(`[${this.groupName}] Job failed ${streamMessage.messageUuid}`, err)
      await this.handleFailure(streamMessage.messageUuid, streamMessage.streamMessageId, streamMessage.retryCount, err.message)
    }
  }

  private async handleFailure(uuid: string, msgId: string, currentRetries: number, errorMsg: string): Promise<void> {
    // 1. ACK the failed message - removes from stream later
    await this.redis.xack(this.streamName, this.groupName, msgId);

    // If current retries is lower than max retries, enque it back for another run
    if (currentRetries < this.MAX_RETRIES) {
      console.log(`[${this.groupName}] Retrying job ${uuid} (Attempt ${currentRetries + 1}/${this.MAX_RETRIES})`);

      const pipeline = this.redis.pipeline();

      // Refresh TTL to ensure data persists through retries (e.g., +1 hour)
      pipeline.expire(this.keys.getJobDataKey(uuid), 3600);
      pipeline.expire(this.keys.getJobStatusKey(uuid), 3600);

      pipeline.xadd(
        this.streamName,
        '*',
        'id', uuid,
        'target', this.groupName, // Instead of all groups, target the failed one
        'retryCount', currentRetries + 1
      );

      await pipeline.exec();

    } else {
      // If retries is larger than allowed, insert the job with all data to dead letter queue
      // 2b. DEAD LETTER QUEUE (DLQ)
      console.error(`[${this.groupName}] Job ${uuid} exhausted retries. Moving to DLQ.`);

      const payload = await this.redis.get(this.keys.getJobDataKey(uuid));

      await this.redis.xadd(
        this.keys.getDlqStreamKey(),
        '*',
        'id', uuid,
        'group', this.groupName,
        'error', errorMsg,
        'payload', payload || 'MISSING',
        'failedAt', Date.now()
      );

      // Delete job from stream and mark it as "done"
      await this.finalize(uuid, msgId, true);
    }
  }

  private async finalize(messageUuid: string, msgId: string, fromError = false): Promise<void> {
    const timestamp = Date.now()
    const statusKey = this.keys.getJobStatusKey(messageUuid);
    const dataKey = this.keys.getJobDataKey(messageUuid);
    const throughputKey = this.keys.getThroughputKey(this.groupName, timestamp);
    const totalKey = this.keys.getTotalKey(this.groupName);

    await this.redis.eval(
      LUA_MARK_DONE,
      6,
      statusKey, dataKey, this.streamName, this.groupName, throughputKey, totalKey,
      this.groupName, timestamp, msgId
    )
  }

  private consumerName(): string {
    return `${this.groupName}-${process.pid}-${this.consumerId}`
  }

  abstract process(data: T): Promise<void>
}
