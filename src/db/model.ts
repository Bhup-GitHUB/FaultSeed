import type { KvOperation, LogEntry, PersistedState } from "./types"

export function applyOperation(state: Map<string, string>, operation: KvOperation): Map<string, string> {
  if (operation.type === "put") state.set(operation.key, operation.value)
  if (operation.type === "delete") state.delete(operation.key)
  return state
}

export function applyLog(state: Map<string, string>, log: readonly LogEntry[], through: number): Map<string, string> {
  for (const entry of log) {
    if (entry.index > through) break
    applyOperation(state, entry.operation)
  }
  return state
}

export function encodeState(state: PersistedState): string {
  return JSON.stringify({
    term: state.term,
    votedFor: state.votedFor,
    log: state.log,
    commitIndex: state.commitIndex
  })
}

export function decodeState(value: string | null): PersistedState {
  if (value === null) return { term: 0, votedFor: null, log: [], commitIndex: 0 }
  const parsed: unknown = JSON.parse(value)
  if (!isPersistedState(parsed)) throw new TypeError("Invalid persisted node state")
  return parsed
}

function isPersistedState(value: unknown): value is PersistedState {
  if (!value || typeof value !== "object") return false
  const state = value as Record<string, unknown>
  if (!Number.isSafeInteger(state.term) || (state.term as number) < 0) return false
  if (state.votedFor !== null && state.votedFor !== "node-a" && state.votedFor !== "node-b" && state.votedFor !== "node-c") return false
  if (!Number.isSafeInteger(state.commitIndex) || (state.commitIndex as number) < 0) return false
  if (!Array.isArray(state.log) || (state.commitIndex as number) > state.log.length) return false
  return state.log.every((entry, index) => isLogEntry(entry, index + 1))
}

function isLogEntry(value: unknown, expectedIndex: number): value is LogEntry {
  if (!value || typeof value !== "object") return false
  const entry = value as Record<string, unknown>
  if (entry.index !== expectedIndex || !Number.isSafeInteger(entry.term) || (entry.term as number) < 0) return false
  const operation = entry.operation
  if (!operation || typeof operation !== "object") return false
  const op = operation as Record<string, unknown>
  if (op.type === "noop") return true
  if (op.type === "delete") return typeof op.key === "string"
  return op.type === "put" && typeof op.key === "string" && typeof op.value === "string"
}
