import { describe, expect, test } from "bun:test"
import { Random, randomStreams } from "../src/simulation/random"

describe("seeded random source", () => {
  test("repeats a sequence for the same seed", () => {
    const first = new Random(48291)
    const second = new Random(48291)

    expect(Array.from({ length: 30 }, () => first.next())).toEqual(
      Array.from({ length: 30 }, () => second.next())
    )
  })

  test("different seeds produce different sequences", () => {
    const first = new Random(48291)
    const second = new Random(48292)

    expect(Array.from({ length: 10 }, () => first.next())).not.toEqual(
      Array.from({ length: 10 }, () => second.next())
    )
  })

  test("provides bounded helpers and independent named streams", () => {
    const first = new Random(7)
    const second = new Random(7)
    const streamsA = randomStreams(7)
    const streamsB = randomStreams(7)
    const workloadA = streamsA.workload.next()
    const workloadB = streamsB.workload.next()
    const networkA = streamsA.network.next()
    const networkB = streamsB.network.next()

    expect(first.int(3, 3)).toBe(3)
    expect(first.bool(0)).toBe(false)
    expect(second.pick(["a"])).toBe("a")
    expect(workloadA).toBe(workloadB)
    expect(workloadA).not.toBe(networkA)
    expect(networkA).toBe(networkB)
  })

  test("rejects invalid ranges and probabilities", () => {
    const random = new Random(1)

    expect(() => random.int(2, 1)).toThrow(RangeError)
    expect(() => random.bool(1.1)).toThrow(RangeError)
    expect(() => random.pick([])).toThrow(RangeError)
  })
})
