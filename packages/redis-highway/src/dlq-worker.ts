import { KeyManager } from "./keys";
import { DlqWorkerOptions, RedisClient, XReadGroupResponse } from "./interfaces";
import {v7 as uuidv7} from 'uuid'
import { DlqMessageEntity } from "./dlq-message-entity";


export abstract class DlqWorker<T extends Record<string, unknown>> {
  protected _isRunning = false;
  protected readonly _keys: KeyManager;
  protected readonly _consumerId = uuidv7()

  protected readonly _dlqStreamName: string;
  protected readonly _groupName = 'dlq-worker';
  protected readonly _consumerName = `${this._groupName}-${this._consumerId}`
  protected readonly _blockTimeoutMs: number;
  protected readonly _waitTimeoutMs: number;

  constructor(protected readonly _redis: RedisClient, options: DlqWorkerOptions){
    this._keys = new KeyManager(options.streamName)
    this._dlqStreamName = this._keys.getDlqStreamKey()
    this._blockTimeoutMs = options.blockTimeoutMs ?? 5_000;
    this._waitTimeoutMs = options.waitTimeoutMs ?? 5_000;
  }

  /**
   * Start DLQ worker
   * @returns
   */
  protected async start(): Promise<void> {
    if(this._isRunning){
      return
    }

    this._isRunning = true
    try {
      await this._redis.xgroup('CREATE', this._dlqStreamName, this._groupName, '0', 'MKSTREAM')
    } catch(e: any){
      if(!e.message.includes('BUSYGROUP')){
        throw e
      }
    }

    this.dlqLoop().catch((e) => console.error('DLQ loop crashed', e))
  }

  /**
   * Stop DLQ loop
   */
  protected async stop(): Promise<void> {
    this._isRunning = false
    await this._redis.xgroup('DELCONSUMER', this._dlqStreamName, this._groupName, this._consumerName).catch()
  }

  /**
   * Background* DLQ loopw
   */
  protected async dlqLoop(): Promise<void>{
    while(this._isRunning){
      try {
        const results = await this._readGroup()

        if(!results){
          await new Promise((resolve) => setTimeout(resolve, this._waitTimeoutMs))
          continue
        }

        const message = results[0][1][0]

        if(!message){
          await new Promise((resolve) => setTimeout(resolve, this._waitTimeoutMs))
          continue
        }

        const dlqMessage = new DlqMessageEntity<T>(message)

        // Ack and delete the message. XACKDEL is supported from 8.2
        await this._redis.multi()
          .xack(this._dlqStreamName, this._groupName, dlqMessage.streamMessageId)
          .xdel(this._dlqStreamName, dlqMessage.streamMessageId)
          .exec()

        await this.process(dlqMessage)
      } catch(e){
        console.error(`[${this._groupName}] Failed processing DLQ job`, e)
        await new Promise((resolve) => setTimeout(resolve, this._waitTimeoutMs))
      }
    }
  }

  /**
   * Reads job from DLQ
   * @returns
   */
  protected async _readGroup(): Promise<XReadGroupResponse | null>{
    return this._redis.xreadgroup(
        'GROUP', this._groupName, this._consumerName,
        'COUNT', 1,
        'BLOCK', this._blockTimeoutMs,
        'STREAMS', this._dlqStreamName, '>'
    ) as unknown as XReadGroupResponse | null
  }

  abstract process(data: DlqMessageEntity<T>): Promise<void>
}
