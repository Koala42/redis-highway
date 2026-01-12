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
  private readonly events = new EventEmitter();
  private keys: KeyManager;
  private readonly consumerId = uuidv7()

  constructor(
    protected redis: Redis,
    protected groupName: string,
    protected streamName: string,
    protected batchSize = 10,
    protected concurrency: number = 1,
    protected maxRetries = 3,
    protected blockTimeMs: number = 2000,
  ) {
    if (batchSize < 1) {
      throw new Error('Batch size cannot be less then 0')
    }

    this.events.setMaxListeners(100);
    this.keys = new KeyManager(streamName)
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
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.events.emit('job_finished');

    while (this.activeCount > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  private async fetchLoop(): Promise<void> {
    while (this.isRunning) {
      const freeSlots = this.concurrency - this.activeCount;

      if (freeSlots <= 0) {
        await new Promise((resolve) => this.events.once('job_finished', resolve))
        continue
      }

      const itemsCount = freeSlots * this.batchSize;

      try {
        const results = await this.redis.xreadgroup(
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
        console.error(`[${this.groupName}] Fetch Error: `, err)
        await new Promise((resolve) => setTimeout(resolve, 1_000))
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

    // If no messsages need to be process, return. Fires job finished event for another loop to pickup next logs
    if (!messages.length) {
      return
    }

    // Get jobs data
    const pipeline = this.redis.pipeline()
    for (const message of messages) {
      pipeline.get(this.keys.getJobDataKey(message.messageUuid))
    }
    const response = await pipeline.exec() as [Error | null, string | null][] | null;

    // TODO: Add error handling
    if (!response) {
      return
    }

    // Parse job data into message entities (lol, titties)
    messages.forEach((message, index) => {
      const foundData = response[index] || null;

      if (!foundData) {
        return
      }

      const [error, data] = foundData

      if (error) {
        console.error(`[${this.groupName}] Failed getting job data err: `, error)
        return
      }

      if (!data) {
        console.error(`[${this.groupName}] Data not found for job`)
        return
      }

      message.data = JSON.parse(data)
    })

    const messagesData: T[] = [];
    const messagesToFinalize: StreamMessageEntity<T>[] = [];

    messages.forEach((message) => {
      messagesToFinalize.push(message);

      if (message.data) {
        messagesData.push(message.data);
      }
    });


    // TODO improve error handling
    if (!messagesData.length) {
      return
    }

    try {
      await this.process(messagesData)
      await this.finalize(messagesToFinalize)
    } catch (err: any) {
      console.error(`[${this.groupName}] Jobs failed`, err)
      await this.handleFailure(messages, err.message)
    }
  }

  private async handleFailure(messages: StreamMessageEntity<T>[], errorMessage: string): Promise<void> {
    const pipeline = this.redis.pipeline()

    // 1. ACK the failed message - removes from stream later (or rather, confirms we processed this specific delivery)
    for (const message of messages) {
      pipeline.xack(this.streamName, this.groupName, message.streamMessageId)
    }

    const messagesToDlq: StreamMessageEntity<T>[] = [];

    for (const message of messages) {
      if (message.retryCount < this.maxRetries) {
        // Retry
        console.log(`[${this.groupName}] Retrying job ${message.messageUuid} (Attempt ${message.retryCount + 1}/${this.maxRetries})`);

        // Refresh TTL
        pipeline.expire(this.keys.getJobDataKey(message.messageUuid), 3600);
        pipeline.expire(this.keys.getJobStatusKey(message.messageUuid), 3600);

        pipeline.xadd(
          this.streamName,
          '*',
          'id', message.messageUuid,
          'target', this.groupName,
          'retryCount', message.retryCount + 1
        );
      } else {
        // DLQ
        console.error(`[${this.groupName}] Job ${message.messageUuid} exhausted retries. Moving to DLQ.`);
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
    }

    await pipeline.exec()

    if (messagesToDlq.length > 0) {
      await this.finalize(messagesToDlq)
    }
  }

  private async finalize(messages: StreamMessageEntity<T>[]): Promise<void> {
    if (messages.length === 0) return;

    const pipeline = this.redis.pipeline();
    const timestamp = Date.now();
    const throughputKey = this.keys.getThroughputKey(this.groupName, timestamp);
    const totalKey = this.keys.getTotalKey(this.groupName);

    // 1. Batch xacks
    const ids = messages.map(m => m.streamMessageId);
    pipeline.xack(this.streamName, this.groupName, ...ids);

    // 2. Batch metrics
    pipeline.incrby(throughputKey, ids.length);
    pipeline.expire(throughputKey, 86400);
    pipeline.incrby(totalKey, ids.length);


    // Lua scripts to only check if data should be deleted
    for (const msg of messages) {
      const statusKey = this.keys.getJobStatusKey(msg.messageUuid);
      const dataKey = this.keys.getJobDataKey(msg.messageUuid);

      pipeline.eval(
        LUA_FINALIZE_COMPLEX,
        3,
        statusKey, dataKey, this.streamName, // Keys
        this.groupName, timestamp, msg.streamMessageId // args
      );
    }

    await pipeline.exec();
  }

  private getConsumerName(): string {
    return `${this.groupName}-${process.pid}-${this.consumerId}`
  }

  abstract process(data: T[]): Promise<void>
}
