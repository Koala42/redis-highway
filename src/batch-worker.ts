import { EventEmitter } from "events";
import { KeyManager } from "./keys";
import Redis from "ioredis";
import { StreamMessage, XReadGroupResponse } from "./interfaces";
import { StreamMessageEntity } from "./stream-message-entity";
import { LUA_FINALIZE_COMPLEX } from "./lua";
import { v7 as uuidv7 } from "uuid";

export abstract class BatchWorker<T extends Record<string, unknown>> {
  private isRunning = false;
  private activeCount = 0;
  private keys: KeyManager;
  private blockingRedis: Redis;
  private readonly events = new EventEmitter();
  private readonly consumerId = uuidv7()

  constructor(
    protected redis: Redis,
    protected groupName: string,
    protected streamName: string,
    protected batchSize = 10, // How many jobs are passed to the process function (max)
    protected concurrency: number = 1, // How many concurrent loops should run
    protected maxFetchSize: number = 20, // How many jobs are fetched at once from redis stream
    protected maxRetries = 3,
    protected blockTimeMs: number = 2_000, // How long should the blocking redis wait for logs from stream
    protected maxFetchCount: number = 5_000,
    protected claimIntervalMs: number = 60_000, // Check for stuck jobs every minute
    protected minIdleTimeMs: number = 120_000, // Job considered stuck after 2 minutes
  ) {
    if (batchSize < 1) {
      throw new Error('Batch size cannot be less then 0')
    }

    this.events.setMaxListeners(100);
    this.keys = new KeyManager(streamName)
    this.blockingRedis = this.redis.duplicate()
  }

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
    this.autoClaimLoop();
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.events.emit('job_finished');

    if (this.blockingRedis) {
      try {
        await this.blockingRedis.quit();
      } catch (e) {
        // whatever
      }
    }

    while (this.activeCount > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
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
            this.getConsumerName(),
            this.minIdleTimeMs,
            cursor,
            'COUNT', this.batchSize
          ) as [string, StreamMessage[]] | null;

          if (!result || !result.length) {
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

              if (entity.retryCount < this.maxRetries) {
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
                console.error(`[${this.groupName}] Job ${entity.messageUuid} run out of retries (stuck). Moving to DLQ`);
                pipeline.xadd(
                  this.keys.getDlqStreamKey(),
                  '*',
                  'id', entity.messageUuid,
                  'group', this.groupName,
                  'error', 'Stuck message recovered max retries',
                  'payload', entity.data ? JSON.stringify(entity.data) : 'MISSING',
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
          console.error(`[${this.groupName}] Auto claim err:`, e.message);
        }
      }
    }
  }

  private async fetchLoop(): Promise<void> {
    while (this.isRunning) {
      const freeSlots = this.concurrency - this.activeCount;

      if (freeSlots <= 0) {
        await new Promise((resolve) => this.events.once('job_finished', resolve))
        continue
      }

      const calculatedCount = freeSlots * this.batchSize;
      const itemsCount = Math.min(calculatedCount, this.maxFetchCount);

      try {
        const results = await this.blockingRedis.xreadgroup(
          'GROUP', this.groupName, this.getConsumerName(),
          'COUNT', itemsCount,
          'BLOCK', this.blockTimeMs,
          'STREAMS', this.streamName, '>'
        ) as XReadGroupResponse | null

        if (!results) {
          continue
        }

        const messages = results[0][1]

        for (let i = 0; i < messages.length; i += this.batchSize) {
          const chunk = messages.slice(i, i + this.batchSize)
          this.spawnWorker(chunk)
        }

      } catch (err) {
        if (this.isRunning) { // Quicker grace shutdown
          console.error(`[${this.groupName}] Fetch Error: `, err)
          await new Promise((resolve) => setTimeout(resolve, 1_000))
        }
      }
    }
  }

  /**
   * Spawn worker for current processing
   * @param messages
   */
  private spawnWorker(messages: StreamMessage[]): void {
    this.activeCount++

    this.processInternal(messages).finally(() => {
      this.activeCount--
      this.events.emit('job_finished')
    })
  }

  private async processInternal(rawMessages: StreamMessage[]): Promise<void> {
    const allMessages = rawMessages.map((msg) => new StreamMessageEntity<T>(msg))

    const messages: StreamMessageEntity<T>[] = []; // Messages to process
    const ignoredMessages: StreamMessageEntity<T>[] = []; // Messages to ignore

    for (const message of allMessages) {
      if (message.routes.includes(this.groupName)) {
        messages.push(message)
      } else {
        ignoredMessages.push(message)
      }
    }

    // ACK ignored messages
    if (ignoredMessages.length) {
      const pipeline = this.redis.pipeline()

      for (const ignoredMessage of ignoredMessages) {
        pipeline.xack(this.streamName, this.groupName, ignoredMessage.streamMessageId)
      }

      await pipeline.exec()
    }

    if (!messages.length) {
      return
    }

    const messagesData: T[] = messages.map((msg) => msg.data)

    try {
      await this.process(messagesData)
      await this.finalize(messages)
    } catch (err: any) {
      console.error(`[${this.groupName}] Processing failed`, err)
      await this.handleFailure(messages, err.message)
    }
  }

  private async handleFailure(messages: StreamMessageEntity<T>[], errorMessage: string): Promise<void> {
    const pipeline = this.redis.pipeline()

    // ack
    for (const message of messages) {
      pipeline.xack(this.streamName, this.groupName, message.streamMessageId)
    }

    const messagesToDlq: StreamMessageEntity<T>[] = [];

    for (const message of messages) {
      if (message.routes.includes(this.groupName)) {
        if (message.retryCount < this.maxRetries && message.data) {
          const payloadString = JSON.stringify(message.data);

          pipeline.xadd(
            this.streamName,
            '*',
            'id', message.messageUuid,
            'target', this.groupName,
            'retryCount', message.retryCount + 1,
            'data', payloadString
          );
        } else {
          console.error(`[${this.groupName}] Job ${message.messageUuid} run out of retries. Moving to DLQ`);
          messagesToDlq.push(message);

          pipeline.xadd(
            this.keys.getDlqStreamKey(),
            '*',
            'id', message.messageUuid,
            'group', this.groupName,
            'error', errorMessage,
            'payload', message.data ? JSON.stringify(message.data) : 'MISSING',
            'failedAt', Date.now()
          )
        }
      } else {
        console.error(`[${this.groupName}] Job ${message.messageUuid} failed but not routed to this group. Moving to DLQ.`);
        messagesToDlq.push(message);

        pipeline.xadd(
          this.keys.getDlqStreamKey(),
          '*',
          'id', message.messageUuid,
          'group', this.groupName,
          'error', `Failed but not routed to ${this.groupName}: ${errorMessage}`,
          'payload', JSON.stringify(message.data),
          'failedAt', Date.now()
        )
      }
    }

    await pipeline.exec()

    if (messagesToDlq.length > 0) {
      await this.finalize(messagesToDlq)
    }
  }

  private async finalize(messages: StreamMessageEntity<T>[]): Promise<void> {
    if (messages.length === 0) {
      return
    }

    const pipeline = this.redis.pipeline();
    const timestamp = Date.now();
    const throughputKey = this.keys.getThroughputKey(this.groupName, timestamp);
    const totalKey = this.keys.getTotalKey(this.groupName);

    const ids = messages.map(m => m.streamMessageId);
    pipeline.xack(this.streamName, this.groupName, ...ids);

    pipeline.incrby(throughputKey, ids.length);
    pipeline.expire(throughputKey, 86400);
    pipeline.incrby(totalKey, ids.length);


    for (const msg of messages) {
      const statusKey = this.keys.getJobStatusKey(msg.messageUuid);

      pipeline.eval(
        LUA_FINALIZE_COMPLEX,
        2,
        statusKey, this.streamName,
        this.groupName, timestamp, msg.streamMessageId
      );
    }

    await pipeline.exec();
  }

  private getConsumerName(): string {
    return `${this.groupName}-${process.pid}-${this.consumerId}`
  }

  abstract process(data: T[]): Promise<void>
}
