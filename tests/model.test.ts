import { describe, expect, test } from "bun:test"
import { applyLog, decodeState, encodeState } from "../src/db/model"
import type { PersistedState } from "../src/db/types"

describe("key-value log model", () => {
  test("applies puts, reads, and deletes in log order", () => {
    const state = applyLog(new Map(), [
      { index: 1, term: 1, operation: { type: "put", key: "x", value: "one" } },
      { index: 2, term: 1, operation: { type: "put", key: "x", value: "two" } },
      { index: 3, term: 2, operation: { type: "delete", key: "x" } }
    ], 2)

    expect(state.get("x")).toBe("two")
    applyLog(state, [{ index: 3, term: 2, operation: { type: "delete", key: "x" } }], 3)
    expect(state.has("x")).toBe(false)
  })

  test("round-trips durable protocol state", () => {
    const state: PersistedState = {
      term: 4,
      votedFor: "node-b",
      log: [{ index: 1, term: 3, operation: { type: "put", key: "x", value: "1" } }],
      commitIndex: 1
    }

    expect(decodeState(encodeState(state))).toEqual(state)
    expect(decodeState(null)).toEqual({ term: 0, votedFor: null, log: [], commitIndex: 0 })
  })

  test("rejects malformed durable state", () => {
    expect(() => decodeState("not-json")).toThrow()
    expect(() => decodeState(JSON.stringify({ term: 1, votedFor: null, log: [], commitIndex: 1 }))).toThrow(TypeError)
  })
})
