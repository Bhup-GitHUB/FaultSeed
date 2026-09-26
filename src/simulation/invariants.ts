import type { ClientRequest, ClientResult, KvOperation, LogEntry, NodeId } from "../db/types"

export type InvariantType =
  | "ACKNOWLEDGED_WRITE_LOST"
  | "VERSION_REGRESSION"
  | "CONFLICTING_COMMIT"
  | "READ_CORRECTNESS"
  | "REPLICA_DIVERGENCE"
  | "STORAGE_CORRUPTION"

export type InvariantDetails = {
  key?: string
  expected?: string | null
  actual?: string | null
  node?: NodeId
  version?: number
}

export class InvariantViolation extends Error {
  constructor(
    readonly type: InvariantType,
    readonly seed: number,
    readonly virtualTime: number,
    readonly event: number,
    readonly details: InvariantDetails
  ) {
    super(`Invariant ${type} failed at event ${event}`)
    this.name = "InvariantViolation"
  }

  format(): string {
    const fields = Object.entries(this.details).map(([key, value]) => `${key}: ${String(value)}`).join("\n")
    return [
      "INVARIANT VIOLATION",
      "",
      `type: ${this.type}`,
      `seed: ${this.seed}`,
      `virtualTime: ${this.virtualTime}`,
      `event: ${this.event}`,
      ...(fields ? ["", fields] : [])
    ].join("\n")
  }
}

type AckedWrite = {
  requestId: string
  index: number
  operation: Extract<KvOperation, { type: "put" | "delete" }>
}

export type NodeSnapshot = {
  id: NodeId
  generation: number
  commitIndex: number
  log: readonly LogEntry[]
  values: ReadonlyMap<string, string>
}

export class InvariantMonitor {
  private requests = new Map<string, ClientRequest>()
  private acknowledged = new Map<string, AckedWrite>()
  private highestVersion = new Map<NodeId, number>()
  private checkedCommit = new Map<string, number>()
  private committedEntries = new Map<number, LogEntry>()
  private checkedLeader = new Map<string, string>()

  constructor(private readonly seed: number) {}

  register(request: ClientRequest): void {
    this.requests.set(request.id, request)
  }

  clientResult(result: ClientResult, time: number, event: number): void {
    const request = this.requests.get(result.id)
    this.requests.delete(result.id)
    if (!request || result.status !== "ok" || result.version === undefined) return
    const operation = request.operation
    if (operation.type === "put" || operation.type === "delete") {
      this.acknowledged.set(request.id, {
        requestId: request.id,
        index: result.version,
        operation: operation as Extract<KvOperation, { type: "put" | "delete" }>
      })
    }
  }

  observe(snapshot: NodeSnapshot, time: number, event: number): void {
    const highest = this.highestVersion.get(snapshot.id) ?? 0
    if (snapshot.commitIndex < highest) {
      throw new InvariantViolation("VERSION_REGRESSION", this.seed, time, event, {
        node: snapshot.id,
        version: snapshot.commitIndex,
        expected: String(highest)
      })
    }
    this.highestVersion.set(snapshot.id, snapshot.commitIndex)
    const generation = `${snapshot.id}:${snapshot.generation}`
    const checked = this.checkedCommit.get(generation) ?? 0
    for (let index = checked; index < snapshot.commitIndex; index += 1) {
      const entry = snapshot.log[index]
      if (!entry) continue
      const committed = this.committedEntries.get(index + 1)
      if (committed && !sameEntry(entry, committed)) {
        throw new InvariantViolation("CONFLICTING_COMMIT", this.seed, time, event, {
          node: snapshot.id,
          version: index + 1,
          expected: JSON.stringify(committed.operation),
          actual: JSON.stringify(entry.operation)
        })
      }
      if (!committed) this.committedEntries.set(index + 1, entry)
    }
    this.checkedCommit.set(generation, Math.max(checked, snapshot.commitIndex))
  }

  checkLeader(snapshot: NodeSnapshot, time: number, event: number): void {
    const generation = `${snapshot.id}:${snapshot.generation}`
    const state = `${snapshot.commitIndex}:${this.acknowledged.size}`
    if (this.checkedLeader.get(generation) === state) return

    for (const write of this.acknowledged.values()) {
      const entry = snapshot.log[write.index - 1]
      if (!entry || !sameOperation(entry.operation, write.operation)) {
        const key = write.operation.key
        throw new InvariantViolation("ACKNOWLEDGED_WRITE_LOST", this.seed, time, event, {
          key,
          expected: write.operation.type === "put" ? write.operation.value : null,
          actual: snapshot.values.get(key) ?? null,
          node: snapshot.id,
          version: write.index
        })
      }
    }
    this.checkedLeader.set(generation, state)
  }

  checkRead(result: ClientResult, snapshot: NodeSnapshot, time: number, event: number): void {
    if (result.status !== "ok" || result.version === undefined) return
    const request = this.requests.get(result.id)
    const operation = request?.operation
    if (!operation || operation.type !== "get") return
    const expected = snapshot.values.get(operation.key) ?? null
    if (result.value !== expected) {
      throw new InvariantViolation("READ_CORRECTNESS", this.seed, time, event, {
        key: operation.key,
        expected,
        actual: result.value ?? null,
        node: snapshot.id,
        version: result.version
      })
    }
  }

  checkConvergence(snapshots: readonly NodeSnapshot[], time: number, event: number): void {
    if (snapshots.length < 2) return
    const expected = snapshots[0]
    for (const actual of snapshots.slice(1)) {
      if (!sameValues(expected.values, actual.values)) {
        throw new InvariantViolation("REPLICA_DIVERGENCE", this.seed, time, event, {
          node: actual.id,
          version: actual.commitIndex
        })
      }
    }
  }
}

function sameEntry(left: LogEntry, right: LogEntry): boolean {
  return left.index === right.index && left.term === right.term && sameOperation(left.operation, right.operation)
}

function sameOperation(left: KvOperation, right: KvOperation): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function sameValues(left: ReadonlyMap<string, string>, right: ReadonlyMap<string, string>): boolean {
  if (left.size !== right.size) return false
  for (const [key, value] of left) if (right.get(key) !== value) return false
  return true
}
