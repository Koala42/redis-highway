import { StreamMessage } from "./interfaces";

export class DlqMessageEntity<T extends Record<string, unknown>> {
  private readonly _streamMessageId: string;
  private readonly _rawFields: string[] = [];
  private readonly _fields: Record<string, string> = {}
  private readonly _group: string;
  private readonly _errorMessage: string
  private readonly _failedAt: number;
  private readonly _messageUuid: string;
  private readonly _data: T;

  constructor(message: StreamMessage){
    this._streamMessageId = message[0]
    this._rawFields = message[1]

    for (let i = 0; i < this._rawFields.length; i += 2){
      this._fields[this._rawFields[i]] = this._rawFields[i + 1]
    }

    this._messageUuid = this._fields['id']
    this._group = this._fields['group']
    this._errorMessage = this._fields['error']
    this._failedAt = Number(this._fields['failedAt'])
    this._data = JSON.parse(this._fields['payload'])
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

  get group(): string {
    return this._group
  }

  get errorMessage(): string {
    return this._errorMessage
  }

  get failedAt(): number {
    return this._failedAt
  }

  public static getStreamFields(id: string, group: string, error: string, payload: string, failedAt: number): (string | number)[] {
    return [
      'id', id,
      'group', group,
      'error', error,
      'payload', payload,
      'failedAt', failedAt
    ]
  }
}
