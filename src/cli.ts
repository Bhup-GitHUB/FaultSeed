import { runFuzz } from "./simulation/fuzz"
import { Simulation, type SimulationConfig, type SimulationResult } from "./simulation/runner"
import { loadFailure, saveFailure } from "./replay/store"

export type CliArgs = {
  command: string | null
  options: Map<string, string | true>
  positionals: string[]
}

export function parseCli(args: string[]): CliArgs {
  const [command, ...rest] = args
  const options = new Map<string, string | true>()
  const positionals: string[] = []

  for (let i = 0; i < rest.length; i += 1) {
    const token = rest[i]
    if (!token.startsWith("--")) {
      positionals.push(token)
      continue
    }

    const equal = token.indexOf("=")
    const key = token.slice(2, equal === -1 ? undefined : equal)
    if (!key) throw new Error("Empty option name")
    if (equal !== -1) {
      options.set(key, token.slice(equal + 1))
      continue
    }

    const next = rest[i + 1]
    if (!next || next.startsWith("--")) {
      options.set(key, true)
      continue
    }
    options.set(key, next)
    i += 1
  }

  return { command: command ?? null, options, positionals }
}

export function help(): string {
  return [
    "FaultSeed",
    "",
    "Commands:",
    "  sim --seed <number> [--ops <count>] [--fault-rate <0..1>]",
    "  fuzz --runs <count> [--seed-start <number>] [--ops <count>]",
    "  replay --seed <number>",
    "  trace --seed <number>",
    "",
    "Use --trace to print the full event trace or --quiet for one-line results."
  ].join("\n")
}

type Options = CliArgs["options"]

function value(options: Options, name: string, fallback?: string): string | undefined {
  const found = options.get(name)
  if (found === true) {
    if (fallback !== undefined) return fallback
    throw new Error(`Option --${name} requires a value`)
  }
  return found ?? fallback
}

function integer(options: Options, name: string, fallback: number): number {
  const raw = value(options, name, String(fallback))!
  const parsed = Number(raw)
  if (!Number.isSafeInteger(parsed)) throw new RangeError(`Option --${name} must be an integer`)
  return parsed
}

function decimal(options: Options, name: string, fallback: number): number {
  const raw = value(options, name, String(fallback))!
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) throw new RangeError(`Option --${name} must be numeric`)
  return parsed
}

function simulationConfig(options: Options, seed: number, base?: SimulationConfig): SimulationConfig {
  return {
    seed,
    operations: integer(options, "ops", base?.operations ?? 100),
    faultRate: decimal(options, "fault-rate", base?.faultRate ?? 0.05),
    maxVirtualTime: integer(options, "max-virtual-time", base?.maxVirtualTime ?? 100_000)
  }
}

function seedFrom(args: CliArgs, required = false): number {
  const raw = value(args.options, "seed") ?? args.positionals[0]
  if (raw === undefined) {
    if (required) throw new Error("Provide --seed <number>")
    return 1
  }
  const seed = Number(raw)
  if (!Number.isSafeInteger(seed)) throw new RangeError("Seed must be a safe integer")
  return seed
}

function printSummary(result: SimulationResult): void {
  console.log(`seed: ${result.seed}`)
  console.log(`operations: ${result.operations}`)
  console.log(`completed: ${result.completed}`)
  console.log(`unavailable: ${result.failures}`)
  console.log(`virtualTime: ${result.virtualTime} ms`)
  console.log(`events: ${result.events}`)
  console.log(`traceHash: ${result.traceHash}`)
}

type OutputMode = "quiet" | "normal" | "trace"

async function reportResult(config: SimulationConfig, result: SimulationResult, mode: OutputMode): Promise<void> {
  if (mode === "normal" || mode === "trace") printSummary(result)
  else if (!result.violation) console.log(`PASS seed=${result.seed} traceHash=${result.traceHash}`)
  if (result.violation) {
    const path = await saveFailure(config, result.violation, result.traceHash)
    console.error("\nSimulation failed\n")
    console.error(result.violation.format())
    console.error(`\nReplay with:\nbun run replay --seed ${config.seed}`)
    console.error(`Saved replay metadata: ${path}`)
    process.exitCode = 1
  }
  if (mode === "trace") console.log(`\n${result.trace.render()}`)
}

async function runSimulation(args: CliArgs, command: "sim" | "trace"): Promise<void> {
  const seed = seedFrom(args)
  const config = simulationConfig(args.options, seed)
  const result = new Simulation(config).run()
  const mode = command === "trace" || args.options.has("trace")
    ? "trace"
    : args.options.has("quiet") ? "quiet" : "normal"
  await reportResult(config, result, mode)
}

async function replay(args: CliArgs): Promise<void> {
  const seed = seedFrom(args, true)
  const saved = await loadFailure(seed)
  const config = simulationConfig(args.options, seed, saved?.config)
  if (saved) console.log(`historical failure: ${saved.invariant}`)
  const result = new Simulation(config).run()
  const mode = args.options.has("trace") ? "trace" : args.options.has("quiet") ? "quiet" : "normal"
  await reportResult(config, result, mode)
}

async function fuzz(args: CliArgs): Promise<void> {
  const runs = integer(args.options, "runs", 1000)
  const seedStart = integer(args.options, "seed-start", 1)
  const config = simulationConfig(args.options, seedStart)
  const result = runFuzz({ ...config, runs, seedStart })

  console.log("FaultSeed")
  console.log("")
  console.log(`Simulations: ${result.runs}`)
  console.log(`Operations: ${result.operations}`)
  console.log(`Virtual time: ${result.virtualTime} ms`)
  console.log(`Real runtime: ${(result.elapsedMs / 1000).toFixed(2)}s`)
  console.log("")
  console.log(`Passed: ${result.passed}`)
  console.log(`Failed: ${result.failures.length}`)

  if (result.failures.length > 0) {
    console.log("")
    console.log("Failing seeds:")
    for (const failure of result.failures) {
      console.log(failure.seed)
      const failed = new Simulation({ ...config, seed: failure.seed }).run()
      if (failed.violation) await saveFailure({ ...config, seed: failure.seed }, failed.violation, failed.traceHash)
    }
    process.exitCode = 1
  }
}

export async function runCli(argv: string[]): Promise<void> {
  const args = parseCli(argv)
  if (!args.command || args.command === "help" || args.command === "--help") {
    console.log(help())
    return
  }
  if (args.command === "sim" || args.command === "trace") return runSimulation(args, args.command)
  if (args.command === "replay") return replay(args)
  if (args.command === "fuzz") return fuzz(args)
  throw new Error(`Unknown command: ${args.command}`)
}

if (import.meta.main) {
  runCli(Bun.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
  })
}
