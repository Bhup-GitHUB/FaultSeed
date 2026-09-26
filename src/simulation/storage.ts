import { Random } from "./random"
import { TraceLog } from "./trace"
import type { Schedule } from "./network"

export type StorageOutcome = "success" | "failure" | "drop" | "corrupt"

export type StorageOperation = {
  id: string
  node: string
  kind: "read" | "write"
  key: string
  value: string | null
  outcome: StorageOutcome
}

export type StorageResult = {
  id: string
  node: string
  kind: "read" | "write"
  key: string
  value: string | null
  error: string | null
}

export type StorageConfig = {
  minLatency: number
  maxLatency: number
  failureProbability: number
  droppedWriteProbability: number
  corruptionProbability: number
  extraDelayProbability: number
  maxExtraDelay: number
}

const defaults: StorageConfig = {
  minLatency: 1,
  maxLatency: 3,
  failureProbability: 0,
  droppedWriteProbability: 0,
  corruptionProbability: 0,
  extraDelayProbability: 0,
  maxExtraDelay: 8
}

export class SimStorage {
  private readonly config: StorageConfig
  private readonly durable = new Map<string, Map<string, string>>()

  constructor(
    private readonly nodes: readonly string[],
    private readonly random: Random,
    private readonly trace: TraceLog,
    private readonly now: () => number,
    private readonly schedule: Schedule,
    config: Partial<StorageConfig> = {}
  ) {
    this.config = { ...defaults, ...config }
    for (const node of nodes) this.durable.set(node, new Map())
    this.validateConfig()
  }

  read(node: string, key: string, id: string): void {
    this.request({ id, node, kind: "read", key, value: null })
  }

  write(node: string, key: string, value: string, id: string): void {
    this.request({ id, node, kind: "write", key, value })
  }

  complete(operation: StorageOperation): StorageResult {
    const data = this.dataFor(operation.node)

    if (operation.kind === "read") {
      const value = data.get(operation.key) ?? null
      this.trace.record(this.now(), operation.node, "STORAGE_READ", { id: operation.id, key: operation.key, found: value !== null })
      return { id: operation.id, node: operation.node, kind: operation.kind, key: operation.key, value, error: null }
    }

    if (operation.outcome === "failure") {
      this.trace.record(this.now(), operation.node, "STORAGE_WRITE_FAIL", { id: operation.id, key: operation.key })
      return { id: operation.id, node: operation.node, kind: operation.kind, key: operation.key, value: null, error: "write failed" }
    }

    if (operation.outcome === "drop") {
      this.trace.record(this.now(), operation.node, "STORAGE_WRITE_DROP", { id: operation.id, key: operation.key })
      return { id: operation.id, node: operation.node, kind: operation.kind, key: operation.key, value: null, error: "write dropped" }
    }

    const value = operation.outcome === "corrupt" ? `${operation.value}#corrupt` : operation.value!
    data.set(operation.key, value)
    const action = operation.outcome === "corrupt" ? "STORAGE_WRITE_CORRUPT" : "STORAGE_WRITE"
    this.trace.record(this.now(), operation.node, action, { id: operation.id, key: operation.key })
    return { id: operation.id, node: operation.node, kind: operation.kind, key: operation.key, value, error: null }
  }

  corrupt(node: string, key: string): boolean {
    const data = this.dataFor(node)
    const value = data.get(key)
    if (value === undefined) return false
    data.set(key, `${value}#corrupt`)
    this.trace.record(this.now(), node, "STORAGE_CORRUPT", { key })
    return true
  }

  durableRead(node: string, key: string): string | null {
    return this.dataFor(node).get(key) ?? null
  }

  durableEntries(node: string): ReadonlyMap<string, string> {
    return new Map(this.dataFor(node))
  }

  private request(request: Omit<StorageOperation, "outcome">): void {
    const outcome = request.kind === "write" ? this.writeOutcome() : "success"
    const delay = this.latency()
    const operation = { ...request, outcome }
    this.trace.record(this.now(), request.node, `STORAGE_${request.kind.toUpperCase()}_SCHEDULE`, { id: request.id, delay, key: request.key })
    this.schedule(delay, "storage.complete", operation)
  }

  private writeOutcome(): StorageOutcome {
    if (this.random.bool(this.config.failureProbability)) return "failure"
    if (this.random.bool(this.config.droppedWriteProbability)) return "drop"
    if (this.random.bool(this.config.corruptionProbability)) return "corrupt"
    return "success"
  }

  private latency(): number {
    let delay = this.random.int(this.config.minLatency, this.config.maxLatency)
    if (this.random.bool(this.config.extraDelayProbability)) {
      delay += this.random.int(1, this.config.maxExtraDelay)
    }
    return delay
  }

  private dataFor(node: string): Map<string, string> {
    const data = this.durable.get(node)
    if (!data) throw new RangeError(`Unknown storage node: ${node}`)
    return data
  }

  private validateConfig(): void {
    const { minLatency, maxLatency, failureProbability, droppedWriteProbability, corruptionProbability, extraDelayProbability, maxExtraDelay } = this.config
    if (!Number.isSafeInteger(minLatency) || minLatency < 0) throw new RangeError("Minimum storage latency must be nonnegative")
    if (!Number.isSafeInteger(maxLatency) || maxLatency < minLatency) throw new RangeError("Invalid maximum storage latency")
    if (!Number.isSafeInteger(maxExtraDelay) || maxExtraDelay < 1) throw new RangeError("Maximum storage delay must be positive")
    for (const probability of [failureProbability, droppedWriteProbability, corruptionProbability, extraDelayProbability]) {
      if (!Number.isFinite(probability) || probability < 0 || probability > 1) {
        throw new RangeError("Storage probabilities must be between zero and one")
      }
    }
  }
}
