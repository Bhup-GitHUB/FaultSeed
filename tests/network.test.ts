import { describe, expect, test } from "bun:test"
import { EventScheduler } from "../src/simulation/scheduler"
import { SimNetwork, type Packet } from "../src/simulation/network"
import { Random } from "../src/simulation/random"
import { TraceLog } from "../src/simulation/trace"

function createNetwork(seed: number, config = {}) {
  const scheduler = new EventScheduler<unknown>()
  const trace = new TraceLog()
  const delivered: Packet<string>[] = []
  const network = new SimNetwork<string>(
    ["a", "b", "c"],
    new Random(seed),
    trace,
    () => scheduler.now,
    (delay, type, payload) => scheduler.schedule(delay, type, payload),
    packet => delivered.push(packet),
    config
  )
  return { scheduler, trace, delivered, network }
}

function flush(network: ReturnType<typeof createNetwork>): void {
  network.scheduler.runUntil(network.scheduler.now + 100, event => {
    if (event.type === "network.delivery") network.network.deliver(event.payload as Packet<string>)
  })
}

describe("simulated network", () => {
  test("repeats packet schedules for the same seed", () => {
    const first = createNetwork(42, { minLatency: 1, maxLatency: 8, extraDelayProbability: 0.4 })
    const second = createNetwork(42, { minLatency: 1, maxLatency: 8, extraDelayProbability: 0.4 })
    for (let index = 0; index < 20; index += 1) {
      first.network.send("a", "b", String(index))
      second.network.send("a", "b", String(index))
    }
    flush(first)
    flush(second)

    expect(first.delivered).toEqual(second.delivered)
    expect(first.trace.hash()).toBe(second.trace.hash())
  })

  test("blocks cross-partition traffic and restores it after healing", () => {
    const runtime = createNetwork(1)
    runtime.network.partition([["a"], ["b", "c"]])
    runtime.network.send("a", "b", "blocked")
    runtime.network.send("b", "c", "allowed")
    flush(runtime)
    runtime.network.heal()
    runtime.network.send("a", "b", "restored")
    flush(runtime)

    expect(runtime.delivered.map(packet => packet.message)).toEqual(["allowed", "restored"])
  })

  test("supports deterministic drops, delay, and duplication", () => {
    const runtime = createNetwork(3, {
      dropProbability: 1,
      duplicateProbability: 1,
      extraDelayProbability: 1,
      maxExtraDelay: 5
    })
    runtime.network.send("a", "b", "dropped")
    flush(runtime)
    expect(runtime.delivered).toHaveLength(0)

    const duplicate = createNetwork(3, { duplicateProbability: 1 })
    duplicate.network.send("a", "b", "copy")
    flush(duplicate)
    expect(duplicate.delivered.map(packet => packet.copy)).toEqual([0, 1])
  })
})
