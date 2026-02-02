import {BaseWorkerControlOptions, BaseWorkerCustomMetricsOptions, BatchWorkerOptions, defaultBaseWorkerControlOptions, defaultBaseWorkerCustomMetrics, RedisClient, StreamMessage } from "./interfaces";
import { StreamMessageEntity } from "./stream-message-entity";
import { BaseWorker } from "./base-worker";

export abstract class BatchWorker<T extends Record<string, unknown>> extends BaseWorker<T> {
  private readonly _batchSize: number;
  private readonly _maxFetchCount: number;

  constructor(
    redis: RedisClient,
    options: BatchWorkerOptions,
    controlOptions: BaseWorkerControlOptions = defaultBaseWorkerControlOptions,
    metricsOptions: BaseWorkerCustomMetricsOptions<T> = defaultBaseWorkerCustomMetrics
  ) {
    super(redis, options, controlOptions, metricsOptions)

    this._batchSize = options.batchSize
    this._maxFetchCount = options.maxFetchCount

    if(this._batchSize === 1){
      console.warn('Why would you create batch worker with batch size 1')
    }

    if (this._batchSize < 1) {
      throw new Error('Batch size cannot be less then 0')
    }
  }

  protected async _fetchLoop(): Promise<void> {
    while (this._isRunning) {
      const freeSlots = this._concurrency - this._activeCount;

      if (freeSlots <= 0) {
        await new Promise((resolve) => this._events.once('job_finished', resolve))
        continue
      }

      const calculatedCount = freeSlots * this._batchSize;
      const itemsCount = Math.min(calculatedCount, this._maxFetchCount);

      try {
        const results = await this._readGroup(itemsCount)

        if (!results) {
          continue
        }

        const messages = results[0][1]

        for (let i = 0; i < messages.length; i += this._batchSize) {
          const chunk = messages.slice(i, i + this._batchSize)
          this.spawnWorker(chunk)
        }

      } catch (err) {
        if (this._isRunning) { // Quicker grace shutdown
          console.error(`[${this._groupName}] Fetch Error: `, err)
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
    this._activeCount++

    this.processInternal(messages).finally(() => {
      this._activeCount--
      this._events.emit('job_finished')
    })
  }

  private async processInternal(rawMessages: StreamMessage[]): Promise<void> {
    const allMessages = rawMessages.map((msg) => new StreamMessageEntity<T>(msg))

    const messages: StreamMessageEntity<T>[] = []; // Messages to process
    const ignoredMessages: StreamMessageEntity<T>[] = []; // Messages to ignore

    for (const message of allMessages) {
      if (message.routes.includes(this._groupName)) {
        messages.push(message)
      } else {
        ignoredMessages.push(message)
      }
    }

    // ACK ignored messages
    if (ignoredMessages.length) {
      const ignoredMessagesStreamIds = ignoredMessages.map((msg) => msg.streamMessageId)
      await this.redis.xack(this._streamName, this._groupName, ...ignoredMessagesStreamIds)
    }

    if (!messages.length) {
      return
    }

    const messagesData: T[] = messages.map((msg) => msg.data)

    try {
      await this.process(messagesData)
      await this._finalize(messages)
    } catch (err: any) {
      console.error(`[${this._groupName}] Processing failed`, err)
      await this._handleFailure(messages, err.message)
    }
  }

  abstract process(data: T[]): Promise<void>
}
