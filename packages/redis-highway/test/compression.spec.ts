import { describe, expect, it } from "vitest";
import { Serializer } from "../src/serializer";

describe("Compression", () => {
  it("Should compress and decompress", async () => {
    const testData = {
      message: "Hello",
      value: true,
      subObject: {
        labels: ["A", "B", "C"]
      }
    }

    const compressedPayload = await Serializer.compressPayload(testData)
    const decompressedPayload = await Serializer.decompressPayload(compressedPayload)
    expect(JSON.stringify(decompressedPayload)).toBe(JSON.stringify(testData))
  })
})
