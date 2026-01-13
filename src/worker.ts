import Redis from "ioredis";
import { EventEmitter } from "events";
import { LUA_MARK_DONE, LUA_FINALIZE_COMPLEX } from "./lua";
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
  private blockingRedis: Redis;

  constructor(
    protected redis: Redis,
    protected groupName: string,
    protected streamName: string,
    protected concurrency: number = 1,
    protected MAX_RETRIES = 3,
    protected blockTimeMs: number = 2000,
    protected claimIntervalMs: number = 60000,
    protected minIdleTimeMs: number = 300000,
  ) {
    this.events.setMaxListeners(100)
    this.keys = new KeyManager(streamName);
    this.blockingRedis = this.redis.duplicate();
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
    this.autoClaimLoop()
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.events.emit('job_finished');

    if (this.blockingRedis) {
      try {
        await this.blockingRedis.quit();
      } catch (e) { }
    }

    while (this.activeCount > 0) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }

  private async autoClaimLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        await new Promise(resolve => setTimeout(resolve, this.claimIntervalMs));
        if (!this.isRunning) {
          break;
        }

        let cursor = '0-0';
        let continueClaiming = true;

        while (continueClaiming && this.isRunning) {
          const result = await this.redis.xautoclaim(
            this.streamName,
            this.groupName,
            this.consumerName(),
            this.minIdleTimeMs,
            cursor,
            'COUNT', this.concurrency
          ) as [string, StreamMessage[]] | null;

          if (!result) {
            continueClaiming = false;
            break;
          }

          const [nextCursor, messages] = result;
          cursor = nextCursor;

          if (messages && messages.length > 0) {

            const pipeline = this.redis.pipeline();

            for (const msg of messages) {
              const entity = new StreamMessageEntity<T>(msg);

              pipeline.xack(this.streamName, this.groupName, entity.streamMessageId);

              const statusKey = this.keys.getJobStatusKey(entity.messageUuid);
              const timestamp = Date.now();
              pipeline.eval(
                LUA_FINALIZE_COMPLEX,
                2,
                statusKey, this.streamName,
                this.groupName, timestamp, entity.streamMessageId
              );

              if (entity.retryCount < this.MAX_RETRIES) {
                pipeline.xadd(
                  this.streamName,
                  '*',
                  'id', entity.messageUuid,
                  'target', entity.routes.join(','),
                  'retryCount', entity.retryCount + 1,
                  'data', entity.data ? JSON.stringify(entity.data) : ''
                );

                pipeline.hset(statusKey, '__target', entity.routes.length);
              } else {
                console.error(`[${this.groupName}] Job ${entity.messageUuid} run outof retries (stuck). Moving to DLQ`);
                // DLQ
                pipeline.xadd(
                  this.keys.getDlqStreamKey(),
                  '*',
                  'id', entity.messageUuid,
                  'group', this.groupName,
                  'error', 'Stuck message recovered max retries',
                  'payload', entity.data ? JSON.stringify(entity.data) : '',
                  'failedAt', Date.now()
                );

                pipeline.del(statusKey);
              }
            }

            await pipeline.exec();
          } else {
            continueClaiming = false;
          }

          if (nextCursor === '0-0') {
            continueClaiming = false;
          }
        }

      } catch (e: any) {
        if (this.isRunning) {
          console.error(`[${this.groupName}] auto claim err:`, e.message);
        }
      }
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
        const results = await this.blockingRedis.xreadgroup(
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
    const streamMessage = new StreamMessageEntity<T>(msg)

    if (!streamMessage.routes.includes(this.groupName)) {
      await this.redis.xack(this.streamName, this.groupName, streamMessage.streamMessageId)
      return;
    }

    try {
      await this.process(streamMessage.data);

      await this.finalize(streamMessage.messageUuid, streamMessage.streamMessageId)
    } catch (err: any) {
      console.error(`[${this.groupName}] Job failed ${streamMessage.messageUuid}`, err)
      await this.handleFailure(streamMessage.messageUuid, streamMessage.streamMessageId, streamMessage.retryCount, err.message, streamMessage.data)
    }
  }

  private async handleFailure(uuid: string, msgId: string, currentRetries: number, errorMsg: string, payloadData: T | null): Promise<void> {
    // Ack
    await this.redis.xack(this.streamName, this.groupName, msgId);

    const payloadString = payloadData ? JSON.stringify(payloadData) : '';

    if (currentRetries < this.MAX_RETRIES && payloadData) {
      const pipeline = this.redis.pipeline();

      pipeline.xadd(
        this.streamName,
        '*',
        'id', uuid,
        'target', this.groupName,
        'retryCount', currentRetries + 1,
        'data', payloadString
      );

      await pipeline.exec();

    } else {
      console.error(`[${this.groupName}] Job ${uuid} run outof retries. Moving to DLQ`);

      await this.redis.xadd(
        this.keys.getDlqStreamKey(),
        '*',
        'id', uuid,
        'group', this.groupName,
        'error', errorMsg,
        'payload', payloadString,
        'failedAt', Date.now()
      );

      await this.finalize(uuid, msgId);
    }
  }

  private async finalize(messageUuid: string, msgId: string): Promise<void> {
    const timestamp = Date.now()
    const statusKey = this.keys.getJobStatusKey(messageUuid);
    const throughputKey = this.keys.getThroughputKey(this.groupName, timestamp);
    const totalKey = this.keys.getTotalKey(this.groupName);

    await this.redis.eval(
      LUA_MARK_DONE,
      5,
      statusKey, this.streamName, this.groupName, throughputKey, totalKey,
      this.groupName, timestamp, msgId
    )
  }

  private consumerName(): string {
    return `${this.groupName}-${process.pid}-${this.consumerId}`
  }

  abstract process(data: T): Promise<void>
}
