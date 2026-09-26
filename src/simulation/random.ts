export class Random {
  private state: number

  constructor(seed: number) {
    if (!Number.isSafeInteger(seed)) throw new RangeError("Seed must be a safe integer")
    this.state = seed >>> 0
  }

  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0
    let value = this.state
    value = Math.imul(value ^ (value >>> 15), value | 1)
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
    return ((value ^ (value >>> 14)) >>> 0) / 0x100000000
  }

  int(min: number, max: number): number {
    if (!Number.isSafeInteger(min) || !Number.isSafeInteger(max) || min > max) {
      throw new RangeError("Expected an inclusive integer range")
    }
    const span = max - min + 1
    if (span > 0x100000000) throw new RangeError("Integer range is too large")
    return min + Math.floor(this.next() * span)
  }

  bool(probability = 0.5): boolean {
    if (probability < 0 || probability > 1 || !Number.isFinite(probability)) {
      throw new RangeError("Probability must be between zero and one")
    }
    return this.next() < probability
  }

  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new RangeError("Cannot pick from an empty list")
    return items[this.int(0, items.length - 1)]
  }

  shuffle<T>(items: readonly T[]): T[] {
    const result = [...items]
    for (let i = result.length - 1; i > 0; i -= 1) {
      const j = this.int(0, i)
      const value = result[i]
      result[i] = result[j]
      result[j] = value
    }
    return result
  }
}

export function deriveSeed(seed: number, domain: string): number {
  if (!Number.isSafeInteger(seed)) throw new RangeError("Seed must be a safe integer")
  let hash = 0x811c9dc5 ^ (seed >>> 0)
  for (const char of domain) {
    hash = Math.imul(hash ^ char.charCodeAt(0), 0x01000193)
  }
  return hash >>> 0
}

export function randomStreams(seed: number): Record<"workload" | "network" | "storage" | "fault", Random> {
  return {
    workload: new Random(deriveSeed(seed, "workload")),
    network: new Random(deriveSeed(seed, "network")),
    storage: new Random(deriveSeed(seed, "storage")),
    fault: new Random(deriveSeed(seed, "fault"))
  }
}
