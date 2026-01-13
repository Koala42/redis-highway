import Redis from "ioredis";
import { StreamMessage, BaseWorkerOptions, BaseWorkerControlOptions, defaultBaseWorkerControlOptions } from "./interfaces";
import { StreamMessageEntity } from "./stream-message-entity";
import { BaseWorker } from "./base-worker";

export abstract class Worker<T extends Record<string, unknown>> extends BaseWorker<T> {

  constructor(
    redis: Redis,
    options: BaseWorkerOptions,
    controlOptions: BaseWorkerControlOptions = defaultBaseWorkerControlOptions
  ) {
    super(redis, options, controlOptions)
  }

  /**
   * Fetch loop (the main loop)
   * Based on free slots (concurrency - active count) gets new messages
   * Spawns worker process for them
   */
  protected async _fetchLoop(): Promise<void> {
    while (this._isRunning) {
      const freeSlots = this._concurrency - this._activeCount

      if (freeSlots <= 0) {
        await new Promise((resolve) => this._events.once('job_finished', resolve))
        continue
      }

      try {
        const results = await this._readGroup(freeSlots)

        if (results) {
          const messages = results[0][1]
          for (const msg of messages) {
            this.spawnWorker(msg);
          }
        }
      } catch (err) {
        console.error(`[${this._groupName}] Fetch Error:`, err)
        await new Promise((resolve) => setTimeout(resolve, 1_000))
      }
    }
  }

  /**
   * Spawns async background worker
   * @param msg
   */
  private spawnWorker(msg: StreamMessage): void {
    this._activeCount++

    this.processInternal(msg).finally(() => {
      this._activeCount--
      this._events.emit('job_finished')
    })
  }


  /**
   * Process message
   * @param msg
   * @returns
   */
  private async processInternal(msg: StreamMessage): Promise<void> {
    const streamMessage = new StreamMessageEntity<T>(msg)

    // Message was not targeted to this group
    // We ACK it for this consumer group but we don't need to update any statuses, since its not for this group
    if (!streamMessage.routes.includes(this._groupName)) {
      await this.redis.xack(this._streamName, this._groupName, streamMessage.streamMessageId)
      return;
    }

    try {
      await this.process(streamMessage.data);

      await this._finalize([streamMessage])
    } catch (err: any) {
      console.error(`[${this._groupName}] Job failed ${streamMessage.messageUuid}`, err)
      await this._handleFailure([streamMessage], err.message)
    }
  }


  abstract process(data: T): Promise<void>
}
