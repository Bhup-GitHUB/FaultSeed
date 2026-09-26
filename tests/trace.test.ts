import { describe, expect, test } from "bun:test"
import { TraceLog } from "../src/simulation/trace"

describe("event trace", () => {
  test("renders readable deterministic records", () => {
    const trace = new TraceLog()
    trace.record(3, "node-a", "REPLICATE", { key: "x", index: 1 }, "node-b")

    expect(trace.render()).toBe("000001 t=3 node-a -> node-b REPLICATE index=1 key=x")
  })

  test("hashes records canonically", () => {
    const first = new TraceLog()
    const second = new TraceLog()
    first.record(0, "client", "PUT", { key: "x", value: "1" })
    second.record(0, "client", "PUT", { value: "1", key: "x" })

    expect(first.hash()).toBe(second.hash())
    expect(first.hash()).toHaveLength(64)
    second.record(1, "node-a", "COMMIT", { index: 1 })
    expect(first.hash()).not.toBe(second.hash())
  })

  test("rejects invalid event time", () => {
    const trace = new TraceLog()
    expect(() => trace.record(-1, "node-a", "START")).toThrow(RangeError)
  })
})
