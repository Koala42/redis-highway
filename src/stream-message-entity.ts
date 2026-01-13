import { StreamMessage } from "./interfaces";

export class StreamMessageEntity<T extends Record<string, unknown>> {
  private readonly _streamMessageId: string; // Redis Stream message ID
  private readonly _rawFields: string[] = []
  private readonly _fields: Record<string, string> = {}
  private readonly _routes: string[] = []
  private readonly _messageUuid: string; // Custom ID for referencing status hash obj
  private readonly _retryCount: number;
  private readonly _data: T;

  constructor(message: StreamMessage) {
    this._streamMessageId = message[0];
    this._rawFields = message[1]

    for (let i = 0; i < this._rawFields.length; i += 2) {
      this._fields[this._rawFields[i]] = this._rawFields[i + 1]
    }

    this._messageUuid = this._fields['id'];
    this._routes = this._fields['target'].split(',')
    this._retryCount = parseInt(this._fields['retryCount'] || '0', 10)
    this._data = JSON.parse(this._fields['data'])
  }

  get data(): T {
    return this._data
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
}
