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

    const key = token.slice(2)
    const next = rest[i + 1]

    if (!key) throw new Error("Empty option name")
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
    "Deterministic simulation testing for replicated key-value systems."
  ].join("\n")
}

if (import.meta.main) {
  const parsed = parseCli(Bun.argv.slice(2))

  if (!parsed.command || parsed.command === "--help" || parsed.command === "help") {
    console.log(help())
  } else {
    console.error(`Unknown command: ${parsed.command}`)
    process.exitCode = 1
  }
}
