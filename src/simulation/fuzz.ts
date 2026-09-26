import type { SimulationConfig } from "./runner"
import { Simulation } from "./runner"

export type FuzzConfig = Omit<SimulationConfig, "seed"> & {
  runs: number
  seedStart: number
}

export type FuzzFailure = {
  seed: number
  invariant: string
  traceHash: string
  virtualTime: number
}

export type FuzzResult = {
  runs: number
  operations: number
  virtualTime: number
  elapsedMs: number
  passed: number
  failures: FuzzFailure[]
}

export function runFuzz(config: FuzzConfig): FuzzResult {
  if (!Number.isSafeInteger(config.runs) || config.runs < 1) throw new RangeError("Run count must be positive")
  if (!Number.isSafeInteger(config.seedStart)) throw new RangeError("Starting seed must be a safe integer")

  const startedAt = performance.now()
  let operations = 0
  let virtualTime = 0
  let passed = 0
  const failures: FuzzFailure[] = []

  for (let index = 0; index < config.runs; index += 1) {
    const seed = config.seedStart + index
    if (!Number.isSafeInteger(seed)) throw new RangeError("Generated seed is outside the safe integer range")
    const simulationConfig: SimulationConfig = {
      seed,
      operations: config.operations,
      faultRate: config.faultRate,
      maxVirtualTime: config.maxVirtualTime,
      ...(config.network ? { network: config.network } : {}),
      ...(config.storage ? { storage: config.storage } : {})
    }
    const result = new Simulation(simulationConfig).run()
    operations += result.operations
    virtualTime += result.virtualTime

    if (result.violation) {
      failures.push({
        seed,
        invariant: result.violation.type,
        traceHash: result.traceHash,
        virtualTime: result.virtualTime
      })
    } else {
      passed += 1
    }
  }

  return {
    runs: config.runs,
    operations,
    virtualTime,
    elapsedMs: performance.now() - startedAt,
    passed,
    failures
  }
}
