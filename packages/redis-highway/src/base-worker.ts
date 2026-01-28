import { EventEmitter } from "events";
import { KeyManager } from "./keys";
import { v7 as uuidv7 } from 'uuid'
import Redis from "ioredis";
import { BaseWorkerControlOptions, BaseWorkerCustomMetricsOptions, BaseWorkerOptions, StreamMessage, XReadGroupResponse } from "./interfaces";
import { StreamMessageEntity } from "./stream-message-entity";
import { LUA_FINALIZE, LUA_FINALIZE_CUSTOM_METRIC } from "./lua";
import { DlqMessageEntity } from "./dlq-message-entity";

export abstract class BaseWorker<T extends Record<string, unknown>> {
  protected _isRunning = false;
  protected _activeCount = 0;
  protected readonly _consumerName: string;
  protected readonly _events = new EventEmitter()
  protected readonly _keys: KeyManager;
  protected readonly _consumerId = uuidv7()
  protected _blockingRedis: Redis

  protected readonly _groupName: string;
  protected readonly _streamName: string;
  protected readonly _concurrency: number;
  protected readonly _maxRetries: number;
  protected readonly _blockTimeMs: number;
  protected readonly _claimIntervalMs: number;
  protected readonly _minIdleTimeMs: number;
  protected readonly _collectMetrics: boolean;
  protected readonly _finalIncrementMetricKey: ((item: T)  => string | null) | null


  constructor(
    protected redis: Redis,
    options: BaseWorkerOptions,
    controlOptions: BaseWorkerControlOptions,
    metricsOptions: BaseWorkerCustomMetricsOptions<T>
  ) {
    this._events.setMaxListeners(100)
    this._groupName = options.groupName;
    this._streamName = options.streamName;
    this._concurrency = options.concurrency;
    this._maxRetries = controlOptions.maxRetries;
    this._blockTimeMs = controlOptions.blockTimeMs;
    this._claimIntervalMs = controlOptions.claimIntervalMs;
    this._minIdleTimeMs = controlOptions.minIdleTimeMs;
    this._collectMetrics = controlOptions.collectMetrics;
    this._finalIncrementMetricKey = metricsOptions.finalIncrementMetricKey ?? null;
    this._consumerName = `${this._groupName}-${this._consumerId}`

    this._keys = new KeyManager(options.streamName)
    this._blockingRedis = redis.duplicate()
  }

  /**
   * Start the worker process
   * Starts fetch loop and auto claim loop
   */
  protected async start(): Promise<void> {
    if (this._isRunning) {
      return
    }

    this._isRunning = true

    try {
      await this.redis.xgroup('CREATE', this._streamName, this._groupName, '0', 'MKSTREAM')
    } catch (e: any) {
      if (!e.message.includes('BUSYGROUP')) {
        throw e
      }
    }

    this._fetchLoop().catch((e) => console.error("Fetch loop crashed", e))
    this._autoClaimLoop().catch((e) => console.error('Auto claim loop crashed', e))
  }

  /**
   * Gracefully stops the worker
   * Waits for any running jobs
   */
  protected async stop(): Promise<void> {
    this._isRunning = false;
    this._events.emit('job_finished');

    if (this._blockingRedis) {
      await this._blockingRedis.quit().catch()
    }

    while (this._activeCount > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50))
    }

    await this.redis.xgroup('DELCONSUMER', this._streamName, this._groupName, this._consumerName).catch()
  }

  /**
   * Auto claim loop
   * Checks which messages are read but not acked for longer than minIdleTimeMs (PEL)
   * Acks them and based on retry policy either enqueues them again or moves to DLQ
   */
  protected async _autoClaimLoop(): Promise<void> {
    while (this._isRunning) {
      try {
        await new Promise(resolve => setTimeout(resolve, this._claimIntervalMs));

        if (!this._isRunning) {
          break;
        }

        let cursor = '0-0';
        let continueClaiming = true;

        while (continueClaiming && this._isRunning) {
          const result = await this._autoClaimMessages(this._concurrency, cursor)

          if (!result) {
            continueClaiming = false;
            break;
          }

          const [nextCursor, msgs] = result;
          cursor = nextCursor;

          if (msgs && msgs.length > 0) {
            const messages = msgs.map((msg) => new StreamMessageEntity<T>(msg))
            await this._handleFailure(messages, 'Stuck messages')
          } else {
            continueClaiming = false;
          }

          if (nextCursor === '0-0') {
            continueClaiming = false;
          }
        }

      } catch (e: any) {
        if (this._isRunning) {
          console.error(`[${this._groupName}] auto claim err:`, e.message);
        }
      }
    }
  }

  protected async _handleFailure(messages: StreamMessageEntity<T>[], errorMessage: string): Promise<void> {
    if (!messages.length) {
      return
    }

    const timestamp = Date.now()
    const pipeline = this.redis.pipeline()
    const messagesStreamIds = messages.map((message) => message.streamMessageId)
    pipeline.xack(this._streamName, this._groupName, ...messagesStreamIds)

    const messagesToDLQ: StreamMessageEntity<T>[] = []

    let retryCountIncr = 0

    for (const message of messages) {
      if (message.retryCount < this._maxRetries) {
        retryCountIncr++

        const newJobId = uuidv7();

        pipeline.xadd(
          this._streamName,
          '*',
          ...StreamMessageEntity.getStreamFields(newJobId, this._groupName, message.serializedData, message.compressed, message.retryCount + 1)
        );

        const newStatusKey = this._keys.getJobStatusKey(newJobId);
        pipeline.hset(newStatusKey, '__target', 1);

        const statusKey = this._keys.getJobStatusKey(message.messageUuid)
        pipeline.eval(
          LUA_FINALIZE,
          2,
          statusKey, this._streamName,
          this._groupName, timestamp, message.streamMessageId
        )
      } else {
        console.error(`[${this._groupName}] Job ${message.messageUuid} run out of retries. Moving to DLQ`);
        messagesToDLQ.push(message);

        // Add message to DLQ stream
        pipeline.xadd(
          this._keys.getDlqStreamKey(),
          '*',
          ...DlqMessageEntity.getStreamFields(message.messageUuid, this._groupName, errorMessage, message.serializedData, timestamp)
        )

        const statusKey = this._keys.getJobStatusKey(message.messageUuid)
        pipeline.eval(
          LUA_FINALIZE,
          2,
          statusKey, this._streamName,
          this._groupName, timestamp, message.streamMessageId
        )
      }
    }

    if(retryCountIncr){
      const retryCountKey = this._keys.getRetriesKey(this._groupName, timestamp)
      pipeline.incrby(retryCountKey, retryCountIncr)
    }


    await pipeline.exec()
  }

  /**
   * Helper methods
   */

  /**
   * Read messages from stream
   * @param count
   * @returns XReadGroupResponse
   */
  protected async _readGroup(count: number): Promise<XReadGroupResponse | null> {
    return this._blockingRedis.xreadgroup(
      'GROUP', this._groupName, this._consumerName,
      'COUNT', count,
      'BLOCK', this._blockTimeMs,
      'STREAMS', this._streamName, '>'
    ) as unknown as XReadGroupResponse | null;
  }

  /**
   * Auto claim messages
   * @param count
   * @param cursor
   * @returns
   */
  protected async _autoClaimMessages(count: number, cursor: string): Promise<[string, StreamMessage[]] | null> {
    return this.redis.xautoclaim(
      this._streamName,
      this._groupName,
      this._consumerName,
      this._minIdleTimeMs,
      cursor,
      'COUNT', this._concurrency
    ) as unknown as [string, StreamMessage[]] | null;
  }

  /**
   * Finalize messages
   * @param messages
   * @returns
   */
  protected async _finalize(messages: StreamMessageEntity<T>[]): Promise<void> {
    if (messages.length === 0) {
      return
    }

    const pipeline = this.redis.pipeline();
    const timestamp = Date.now();
    const throughputKey = this._keys.getThroughputKey(this._groupName, timestamp);
    const totalKey = this._keys.getTotalKey(this._groupName);

    const ids = messages.map(m => m.streamMessageId);
    pipeline.xack(this._streamName, this._groupName, ...ids);

    if (this._collectMetrics) {
      pipeline.incrby(throughputKey, ids.length);
      pipeline.expire(throughputKey, 86400);
      pipeline.incrby(totalKey, ids.length);
    }


    for (const msg of messages) {
      const statusKey = this._keys.getJobStatusKey(msg.messageUuid);
      const customMetricKey = this._finalIncrementMetricKey ? this._finalIncrementMetricKey(msg.data) : null

      if(customMetricKey){
        pipeline.eval(
          LUA_FINALIZE_CUSTOM_METRIC,
          3,
          statusKey, this._streamName, customMetricKey,
          this._groupName, timestamp, msg.streamMessageId
        )
      } else {
        pipeline.eval(
          LUA_FINALIZE,
          2,
          statusKey, this._streamName,
          this._groupName, timestamp, msg.streamMessageId
        );
      }
    }

    await pipeline.exec();
  }


  protected abstract _fetchLoop(): Promise<void>
}
