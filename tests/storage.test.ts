import { describe, expect, test } from "bun:test"
import { EventScheduler } from "../src/simulation/scheduler"
import { Random } from "../src/simulation/random"
import { SimStorage, type StorageOperation } from "../src/simulation/storage"
import { TraceLog } from "../src/simulation/trace"

function createStorage(config = {}) {
  const scheduler = new EventScheduler<unknown>()
  const trace = new TraceLog()
  const results = [] as ReturnType<SimStorage["complete"]>[]
  const storage = new SimStorage(
    ["a", "b", "c"],
    new Random(17),
    trace,
    () => scheduler.now,
    (delay, type, payload) => scheduler.schedule(delay, type, payload),
    config
  )
  return { scheduler, trace, results, storage }
}

function flush(runtime: ReturnType<typeof createStorage>): void {
  runtime.scheduler.runUntil(runtime.scheduler.now + 100, event => {
    if (event.type === "storage.complete") {
      runtime.results.push(runtime.storage.complete(event.payload as StorageOperation))
    }
  })
}

describe("simulated storage", () => {
  test("persists values independently for each node", () => {
    const runtime = createStorage()
    runtime.storage.write("a", "key", "value", "w1")
    flush(runtime)
    runtime.storage.read("a", "key", "r1")
    runtime.storage.read("b", "key", "r2")
    flush(runtime)

    expect(runtime.results.find(result => result.id === "r1")?.value).toBe("value")
    expect(runtime.results.find(result => result.id === "r2")?.value).toBeNull()
    expect(runtime.storage.durableRead("a", "key")).toBe("value")
    expect(runtime.storage.durableRead("b", "key")).toBeNull()
  })

  test("models failed, dropped, and corrupted writes", () => {
    const failed = createStorage({ failureProbability: 1 })
    failed.storage.write("a", "key", "value", "w1")
    flush(failed)
    expect(failed.results[0].error).toBe("write failed")
    expect(failed.storage.durableRead("a", "key")).toBeNull()

    const dropped = createStorage({ droppedWriteProbability: 1 })
    dropped.storage.write("a", "key", "value", "w1")
    flush(dropped)
    expect(dropped.results[0].error).toBeNull()
    expect(dropped.storage.durableRead("a", "key")).toBeNull()

    const corrupted = createStorage({ corruptionProbability: 1 })
    corrupted.storage.write("a", "key", "value", "w1")
    flush(corrupted)
    expect(corrupted.storage.durableRead("a", "key")).toBe("value#corrupt")
  })

  test("preserves durable values across simulated restart", () => {
    const runtime = createStorage()
    runtime.storage.write("b", "log", "entry", "w1")
    flush(runtime)
    const restarted = runtime.storage.durableEntries("b")

    expect(restarted.get("log")).toBe("entry")
    expect(runtime.trace.hash()).toHaveLength(64)
  })
})
