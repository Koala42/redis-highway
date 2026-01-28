import { StreamMessage } from "./interfaces";
import { Serializer } from "./serializer";

export class StreamMessageEntity<T extends Record<string, unknown>> {
  private readonly _streamMessageId: string; // Redis Stream message ID
  private readonly _rawFields: string[] = []
  private readonly _fields: Record<string, string> = {}
  private readonly _routes: string[] = []
  private readonly _messageUuid: string; // Custom ID for referencing status hash obj
  private readonly _retryCount: number;
  private readonly _data: T;
  private readonly _compressed: boolean;
  private readonly _rawData: string;

  constructor(message: StreamMessage) {
    this._streamMessageId = message[0];
    this._rawFields = message[1]

    for (let i = 0; i < this._rawFields.length; i += 2) {
      this._fields[this._rawFields[i]] = this._rawFields[i + 1]
    }

    this._messageUuid = this._fields['id'];
    this._routes = this._fields['target'].split(',')
    this._retryCount = parseInt(this._fields['retryCount'] || '0', 10)
    this._compressed = this._fields['zstd'] === 'true'
    this._data = this._compressed ? Serializer.decompressPayloadSync(this._fields['data']) : JSON.parse(this._fields['data'])
    this._rawData = this._fields['data']
  }

  get data(): T {
    return this._data
  }

  get serializedData(): string {
    return this._rawData
  }

  get compressed(): boolean {
    return this._compressed
  }

  get streamMessageId(): string {
    return this._streamMessageId
  }

  get messageUuid(): string {
    return this._messageUuid
  }

  get routes(): string[] {
    return this._routes
  }

  get retryCount(): number {
    return this._retryCount
  }

  public static getStreamFields(id: string, target: string | string[], serializedPayload: string, compression: boolean, retryCount?: number): (string | number)[]{
    const fields: (string | number)[] = ['id', id, 'target', Array.isArray(target) ? target.join(',') : target, 'data', serializedPayload]
    if(retryCount !== undefined){
      fields.push('retryCount', retryCount)
    }
    if (compression) {
      fields.push('zstd', 'true')
    }

    return fields
  }
}
