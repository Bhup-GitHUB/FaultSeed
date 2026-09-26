import { createHash } from "node:crypto"

export type TraceData = Record<string, string | number | boolean | null>

export type TraceEvent = {
  sequence: number
  time: number
  actor: string
  action: string
  target: string | null
  data: TraceData
}

export class TraceLog {
  private events: TraceEvent[] = []

  constructor(private readonly enabled = true) {}

  get entries(): readonly TraceEvent[] {
    return this.events
  }

  record(time: number, actor: string, action: string, data: TraceData = {}, target: string | null = null): void {
    if (!this.enabled) return
    if (!Number.isFinite(time) || time < 0) throw new RangeError("Trace time must be nonnegative")
    const event: TraceEvent = {
      sequence: this.events.length + 1,
      time,
      actor,
      action,
      target,
      data: Object.fromEntries(Object.entries(data).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0))
    }
    this.events.push(event)
  }

  hash(): string {
    if (!this.enabled) return ""
    const serialized = JSON.stringify(this.events.map(event => ({
      sequence: event.sequence,
      time: event.time,
      actor: event.actor,
      action: event.action,
      target: event.target,
      data: event.data
    })))
    return createHash("sha256").update(serialized).digest("hex")
  }

  render(): string {
    return this.events.map(event => {
      const route = event.target ? ` -> ${event.target}` : ""
      const fields = Object.entries(event.data).map(([key, value]) => `${key}=${String(value)}`).join(" ")
      const suffix = fields ? ` ${fields}` : ""
      return `${String(event.sequence).padStart(6, "0")} t=${event.time} ${event.actor}${route} ${event.action}${suffix}`
    }).join("\n")
  }
}
