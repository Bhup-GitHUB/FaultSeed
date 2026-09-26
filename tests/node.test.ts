import { describe, expect, test } from "bun:test"
import { DatabaseNode, type NodeConfig } from "../src/db/node"
import type { ClientRequest, NodeEffect, NodeId, NodeInput, ProtocolMessage } from "../src/db/types"
import { SimNetwork, type Packet } from "../src/simulation/network"
import { Random, randomStreams } from "../src/simulation/random"
import { EventScheduler } from "../src/simulation/scheduler"
import { SimStorage, type StorageOperation } from "../src/simulation/storage"
import { TraceLog } from "../src/simulation/trace"

type TimerEvent = { node: NodeId; generation: number; name: "election" | "heartbeat"; token: number }
type ClientEvent = { node: NodeId; generation: number; request: ClientRequest }

function createCluster(acknowledgement: NodeConfig["acknowledgement"] = "quorum") {
  const ids: NodeId[] = ["node-a", "node-b", "node-c"]
  const scheduler = new EventScheduler<unknown>()
  const trace = new TraceLog()
  const streams = randomStreams(48291)
  const generations = new Map<NodeId, number>()
  const nodes = new Map<NodeId, DatabaseNode>()
  const results = [] as { at: number; node: NodeId; result: Extract<NodeEffect, { type: "client" }>["result"] }[]
  let network: SimNetwork<ProtocolMessage>
  const storage = new SimStorage(ids, streams.storage, trace, () => scheduler.now,
    (delay, type, payload) => scheduler.schedule(delay, type, payload),
    { minLatency: 1, maxLatency: 1 })
  network = new SimNetwork(ids, streams.network, trace, () => scheduler.now,
    (delay, type, payload) => scheduler.schedule(delay, type, payload),
    packet => deliver(packet),
    { minLatency: 1, maxLatency: 1 })

  function config(id: NodeId): NodeConfig {
    return {
      electionTimeout: id === "node-a" ? 8 : id === "node-b" ? 14 : 20,
      heartbeatInterval: 2,
      acknowledgement
    }
  }

  function effects(id: NodeId, values: NodeEffect[]): void {
    for (const effect of values) {
      if (effect.type === "send") {
        network.send(id, effect.to, effect.message)
      } else if (effect.type === "read") {
        storage.read(id, effect.key, effect.id)
      } else if (effect.type === "write") {
        storage.write(id, effect.key, effect.value, effect.id)
      } else if (effect.type === "schedule") {
        scheduler.schedule(effect.delay, "timer", {
          node: id,
          generation: generations.get(id)!,
          name: effect.name,
          token: effect.token
        } satisfies TimerEvent)
      } else if (effect.type === "client") {
        results.push({ at: scheduler.now, node: id, result: effect.result })
      } else {
        trace.record(scheduler.now, id, effect.action, effect.data, effect.target ?? null)
      }
    }
  }

  function boot(id: NodeId): void {
    const generation = (generations.get(id) ?? 0) + 1
    generations.set(id, generation)
    const node = new DatabaseNode(id, config(id))
    nodes.set(id, node)
    effects(id, node.handle({ type: "start" }))
  }

  function deliver(packet: Packet<ProtocolMessage>): void {
    const node = nodes.get(packet.to as NodeId)
    if (!node) return
    effects(packet.to as NodeId, node.handle({ type: "message", from: packet.from as NodeId, message: packet.message }))
  }

  for (const id of ids) boot(id)

  function runUntil(until: number): void {
    scheduler.runUntil(until, event => {
      if (event.type === "network.delivery") {
        network.deliver(event.payload as Packet<ProtocolMessage>)
      } else if (event.type === "storage.complete") {
        const result = storage.complete(event.payload as StorageOperation)
        const node = nodes.get(result.node as NodeId)
        if (node) effects(result.node as NodeId, node.handle({ type: "storage", id: result.id, value: result.value, error: result.error }))
      } else if (event.type === "timer") {
        const timer = event.payload as TimerEvent
        if (generations.get(timer.node) !== timer.generation) return
        const node = nodes.get(timer.node)
        if (node) effects(timer.node, node.handle({ type: "timer", name: timer.name, token: timer.token }))
      } else if (event.type === "client") {
        const client = event.payload as ClientEvent
        if (generations.get(client.node) !== client.generation) return
        const node = nodes.get(client.node)
        if (node) effects(client.node, node.handle({ type: "client", request: client.request }))
      }
    })
  }

  function client(id: NodeId, request: ClientRequest): void {
    scheduler.schedule(0, "client", { node: id, generation: generations.get(id)!, request } satisfies ClientEvent)
  }

  function crash(id: NodeId): void {
    nodes.delete(id)
  }

  function currentLeader(): NodeId | undefined {
    return [...nodes.entries()].find(([, node]) => node.currentRole === "leader")?.[0]
  }

  return { scheduler, trace, streams, network, storage, nodes, results, runUntil, client, crash, boot, currentLeader }
}

describe("database nodes", () => {
  test("elects a leader, commits writes on a quorum, and serves barrier reads", () => {
    const cluster = createCluster()
    cluster.runUntil(40)
    expect(cluster.currentLeader()).toBe("node-a")

    cluster.client("node-a", { id: "put-1", operation: { type: "put", key: "x", value: "1" } })
    cluster.runUntil(60)

    expect(cluster.results.find(item => item.result.id === "put-1")?.result.status).toBe("ok")
    expect([...cluster.nodes.values()].map(node => node.committedValues.get("x"))).toEqual(["1", "1", "1"])

    cluster.client("node-a", { id: "get-1", operation: { type: "get", key: "x" } })
    cluster.runUntil(70)
    expect(cluster.results.find(item => item.result.id === "get-1")?.result.value).toBe("1")

    cluster.client("node-a", { id: "delete-1", operation: { type: "delete", key: "x" } })
    cluster.runUntil(90)
    expect(cluster.nodes.get("node-a")?.committedValues.has("x")).toBe(false)
  })

  test("elects a replacement and restores a restarted node from durable state", () => {
    const cluster = createCluster()
    cluster.runUntil(40)
    cluster.client("node-a", { id: "put-1", operation: { type: "put", key: "durable", value: "yes" } })
    cluster.runUntil(60)
    cluster.crash("node-a")
    cluster.runUntil(100)

    expect(cluster.currentLeader()).toBe("node-b")
    cluster.boot("node-a")
    cluster.runUntil(110)
    expect(cluster.nodes.get("node-a")?.committedValues.get("durable")).toBe("yes")
    expect(cluster.nodes.get("node-a")?.currentCommitIndex).toBeGreaterThan(0)
  })

  test("refuses client writes without a leader", () => {
    const cluster = createCluster()
    cluster.client("node-b", { id: "write-1", operation: { type: "put", key: "x", value: "1" } })
    cluster.runUntil(2)
    expect(cluster.results.find(item => item.result.id === "write-1")?.result.status).toBe("unavailable")
  })
})
