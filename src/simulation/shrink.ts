import type { InvariantType } from "./invariants"
import { Simulation, type SimulationConfig, type SimulationResult } from "./runner"

export type ShrinkResult = {
  config: SimulationConfig
  result: SimulationResult
  trials: number
}

export function shrinkFailure(config: SimulationConfig, invariant: InvariantType): ShrinkResult {
  const original = new Simulation(config).run()
  if (original.violation?.type !== invariant) return { config, result: original, trials: 1 }

  let lower = 0
  let upper = original.operations
  let trials = 1

  while (lower < upper) {
    const operations = Math.floor((lower + upper) / 2)
    const candidateConfig = { ...config, operations }
    const candidate = new Simulation(candidateConfig).run()
    trials += 1
    if (candidate.violation?.type === invariant) upper = operations
    else lower = operations + 1
  }

  const reducedConfig = { ...config, operations: lower }
  const reduced = new Simulation(reducedConfig).run()
  trials += 1
  if (reduced.violation?.type !== invariant) return { config, result: original, trials }
  return { config: reducedConfig, result: reduced, trials }
}
