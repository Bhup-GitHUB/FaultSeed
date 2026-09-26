import { mkdir, readFile, rename, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { DatabaseNode } from "../db/node"
import type { ClientOperation, ClientRequest, ClientResult, NodeEffect, NodeId, NodeInput, ProtocolMessage } from "../db/types"

export type RealNodeConfig = {
  id: NodeId
  port: number
  peers: Record<NodeId, string>
  dataDirectory: string
}

type PendingClient = {
  resolve: (result: ClientResult) => void
  timer: ReturnType<typeof setTimeout>
}

const nodeIds: readonly NodeId[] = ["node-a", "node-b", "node-c"]

export class RealNodeRuntime {
  private readonly node: DatabaseNode
  private server: ReturnType<typeof Bun.serve> | null = null
  private requestSequence = 0
  private readonly pending = new Map<string, PendingClient>()

  constructor(private readonly config: RealNodeConfig) {
    this.node = new DatabaseNode(config.id, {
      electionTimeout: config.id === "node-a" ? 2400 : config.id === "node-b" ? 3600 : 4800,
      heartbeatInterval: 600
    })
  }

  listen(): void {
    this.server = Bun.serve({
      port: this.config.port,
      fetch: request => this.handleRequest(request)
    })
    console.log(`FaultSeed ${this.config.id} listening on ${this.server.url}`)
    this.apply(this.node.handle({ type: "start" }))
  }

  stop(): void {
    this.server?.stop()
    this.server = null
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer)
      pending.resolve({ id, status: "unavailable" })
    }
    this.pending.clear()
  }

  private async handleRequest(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (request.method === "GET" && url.pathname === "/health") {
      return Response.json({ node: this.config.id, role: this.node.currentRole, term: this.node.currentTerm })
    }

    if (request.method !== "POST") return Response.json({ error: "method not allowed" }, { status: 405 })

    if (url.pathname === "/_message") {
      return this.handleMessage(request)
    }

    const type = url.pathname.slice("/kv/".length)
    if (!url.pathname.startsWith("/kv/") || !["put", "get", "delete"].includes(type)) {
      return Response.json({ error: "not found" }, { status: 404 })
    }

    try {
      const body = await request.json() as Record<string, unknown>
      const operation = this.parseOperation(type, body)
      const result = await this.submit(operation)
      return Response.json(result, { status: result.status === "ok" ? 200 : 503 })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return Response.json({ error: message }, { status: 400 })
    }
  }

  private async handleMessage(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { from?: unknown; message?: unknown }
      if (typeof body.from !== "string" || !nodeIds.includes(body.from as NodeId) || !isProtocolMessage(body.message)) {
        return Response.json({ error: "invalid protocol message" }, { status: 400 })
      }
      this.apply(this.node.handle({ type: "message", from: body.from as NodeId, message: body.message }))
      return Response.json({ accepted: true })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return Response.json({ error: message }, { status: 400 })
    }
  }

  private parseOperation(type: string, body: Record<string, unknown>): ClientOperation {
    if (typeof body.key !== "string" || body.key.length === 0) throw new TypeError("key must be a nonempty string")
    if (type === "put") {
      if (typeof body.value !== "string") throw new TypeError("value must be a string")
      return { type, key: body.key, value: body.value }
    }
    return { type: type as "get" | "delete", key: body.key }
  }

  private submit(operation: ClientOperation): Promise<ClientResult> {
    const id = `${this.config.id}-client-${this.requestSequence}`
    this.requestSequence += 1
    const request: ClientRequest = { id, operation }

    return new Promise(resolve => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        resolve({ id, status: "unavailable" })
      }, 5000)
      this.pending.set(id, { resolve, timer })
      this.apply(this.node.handle({ type: "client", request }))
    })
  }

  private apply(effects: NodeEffect[]): void {
    for (const effect of effects) {
      if (effect.type === "send") {
        void this.send(effect.to, effect.message)
      } else if (effect.type === "read") {
        void this.read(effect.id)
      } else if (effect.type === "write") {
        void this.write(effect.id, effect.value)
      } else if (effect.type === "schedule") {
        setTimeout(() => this.apply(this.node.handle({ type: "timer", name: effect.name, token: effect.token })), effect.delay)
      } else if (effect.type === "client") {
        const pending = this.pending.get(effect.result.id)
        if (pending) {
          clearTimeout(pending.timer)
          this.pending.delete(effect.result.id)
          pending.resolve(effect.result)
        }
      } else {
        console.log(`${this.config.id} ${effect.action} ${JSON.stringify(effect.data)}`)
      }
    }
  }

  private async send(to: NodeId, message: ProtocolMessage): Promise<void> {
    try {
      const response = await fetch(new URL("/_message", this.config.peers[to]), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ from: this.config.id, message })
      })
      if (!response.ok) console.error(`${this.config.id} peer ${to} returned ${response.status}`)
    } catch (error) {
      console.error(`${this.config.id} could not reach ${to}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  private async read(id: string): Promise<void> {
    let value: string | null = null
    let error: string | null = null
    try {
      value = await readFile(this.statePath(), "utf8")
    } catch (cause) {
      if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) {
        error = cause instanceof Error ? cause.message : String(cause)
      }
    }
    this.apply(this.node.handle({ type: "storage", id, value, error }))
  }

  private async write(id: string, value: string): Promise<void> {
    let error: string | null = null
    try {
      await mkdir(this.config.dataDirectory, { recursive: true })
      const path = this.statePath()
      const temporary = `${path}.${id.replaceAll(":", "-")}.tmp`
      await writeFile(temporary, value, "utf8")
      await rename(temporary, path)
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause)
    }
    this.apply(this.node.handle({ type: "storage", id, value: null, error }))
  }

  private statePath(): string {
    return join(this.config.dataDirectory, `${this.config.id}.json`)
  }
}

function isProtocolMessage(value: unknown): value is ProtocolMessage {
  if (!value || typeof value !== "object") return false
  const message = value as Record<string, unknown>
  if (typeof message.type !== "string" || !Number.isSafeInteger(message.term)) return false
  if (message.type === "request_vote") return nodeIds.includes(message.candidateId as NodeId)
  if (message.type === "vote_response") return nodeIds.includes(message.voterId as NodeId) && typeof message.granted === "boolean"
  if (message.type === "append_entries") return nodeIds.includes(message.leaderId as NodeId) && Array.isArray(message.entries)
  if (message.type === "append_response") return nodeIds.includes(message.followerId as NodeId) && typeof message.success === "boolean"
  return false
}
