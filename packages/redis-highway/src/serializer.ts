import { zstdCompress, zstdDecompress, zstdDecompressSync } from "zlib";

export class Serializer {
  /**
   * Compress payload using zstd
   * @param payload - JSON like payload
   * @returns base64 encoded payload
   */
  static async compressPayload(payload: Record<string, unknown>): Promise<string> {
    const minifiedPayload = JSON.stringify(payload)

    const compressedPayload = await new Promise<Buffer<ArrayBuffer>>((resolve) => {
      zstdCompress(Buffer.from(minifiedPayload), (_, result) => resolve(result))
    })

    return Buffer.from(compressedPayload).toString("base64");
  }

  /**
   * Decompress payload using zstd
   * @param compressedPayload - base64 encoded compressed payload
   * @returns Parsed JSON payload
   */
  static async decompressPayload(compressedPayload: string): Promise<Record<string, unknown>> {
    const decoded = Buffer.from(compressedPayload, "base64")

    const decompressed = await new Promise<Buffer<ArrayBuffer>>((resolve) => {
      zstdDecompress(decoded, (_, result) => resolve(result))
    })

    return JSON.parse(decompressed.toString())
  }

  /**
   * Decompress payload sync
   * @param compressedPayload - base64 encoded payload
   * @returns Parsed JSON payload
   */
  static decompressPayloadSync<T extends Record<string, unknown>>(compressedPayload: string): Promise<T> {
    const decoded = Buffer.from(compressedPayload, "base64")
    const decompressed = zstdDecompressSync(decoded)
    return JSON.parse(decompressed.toString())
  }
}
