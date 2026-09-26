import { mkdir, readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"
import type { InvariantViolation } from "../simulation/invariants"
import type { SimulationConfig } from "../simulation/runner"

export type ReplayRecord = {
  format: 1
  config: SimulationConfig
  invariant: string
  traceHash: string
}

export function replayPath(seed: number): string {
  return join(process.cwd(), ".faultseed", "failures", `${seed}.json`)
}

export async function saveFailure(config: SimulationConfig, violation: InvariantViolation, traceHash: string): Promise<string> {
  const path = replayPath(config.seed)
  await mkdir(join(process.cwd(), ".faultseed", "failures"), { recursive: true })
  const record: ReplayRecord = {
    format: 1,
    config,
    invariant: violation.type,
    traceHash
  }
  await writeFile(path, `${JSON.stringify(record, null, 2)}\n`, "utf8")
  return path
}

export async function loadFailure(seed: number): Promise<ReplayRecord | null> {
  const paths = [
    replayPath(seed),
    join(process.cwd(), "fixtures", `${seed}.json`)
  ]

  for (const path of paths) {
    try {
      const value: unknown = JSON.parse(await readFile(path, "utf8"))
      if (!isReplayRecord(value) || value.config.seed !== seed) throw new TypeError("Invalid replay metadata")
      return value
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") continue
      throw error
    }
  }

  return null
}

function isReplayRecord(value: unknown): value is ReplayRecord {
  if (!value || typeof value !== "object") return false
  const record = value as Record<string, unknown>
  if (record.format !== 1 || typeof record.invariant !== "string" || typeof record.traceHash !== "string") return false
  if (!record.config || typeof record.config !== "object") return false
  const config = record.config as Record<string, unknown>
  return Number.isSafeInteger(config.seed) &&
    Number.isSafeInteger(config.operations) &&
    typeof config.faultRate === "number" &&
    Number.isSafeInteger(config.maxVirtualTime)
}
