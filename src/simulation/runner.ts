import { DatabaseNode, type NodeConfig } from "../db/node"
import type { ClientRequest, ClientResult, NodeEffect, NodeId, NodeInput, ProtocolMessage } from "../db/types"
import { SimNetwork, type NetworkConfig, type Packet } from "./network"
import { generateFaults, type FaultEvent } from "./faults"
import { InvariantMonitor, InvariantViolation, type NodeSnapshot } from "./invariants"
import { randomStreams } from "./random"
import { EventScheduler } from "./scheduler"
import { SimStorage, type StorageConfig, type StorageOperation } from "./storage"
import { TraceLog } from "./trace"
import { generateWorkload, type WorkloadItem } from "./workload"

export type SimulationConfig = {
  seed: number
  operations: number
  faultRate: number
  maxVirtualTime: number
  network?: Partial<NetworkConfig>
  storage?: Partial<StorageConfig>
}

export type SimulationResult = {
  seed: number
  operations: number
  completed: number
  failures: number
  virtualTime: number
  events: number
  trace: TraceLog
  traceHash: string
  violation: InvariantViolation | null
}

type TimerEvent = {
  node: NodeId
  generation: number
  name: "election" | "heartbeat"
  token: number
}

type ClientEvent = {
  request: ClientRequest
}

type InputEvent = {
  node: NodeId
  generation: number
  input: NodeInput
}

const nodeIds: readonly NodeId[] = ["node-a", "node-b", "node-c"]

export class Simulation {
  readonly trace: TraceLog
  private readonly scheduler = new EventScheduler<unknown>()
  private readonly nodes = new Map<NodeId, DatabaseNode>()
  private readonly generations = new Map<NodeId, number>()
  private readonly monitor: InvariantMonitor
  private readonly workload: WorkloadItem[]
  private readonly faults: FaultEvent[]
  private readonly storage: SimStorage
  private readonly network: SimNetwork<ProtocolMessage>
  private eventNumber = 0
  private completed = 0
  private failures = 0
  private violation: InvariantViolation | null = null

  constructor(readonly config: SimulationConfig, recordTrace = true) {
    this.trace = new TraceLog(recordTrace)
    this.validateConfig()
    const streams = randomStreams(config.seed)
    this.monitor = new InvariantMonitor(config.seed)
    this.workload = generateWorkload(streams.workload, config.operations)
    this.faults = generateFaults(streams.fault, this.workload, config.faultRate, config.maxVirtualTime)
    const faultScale = Math.min(config.faultRate, 0.5)
    this.network = new SimNetwork(
      nodeIds,
      streams.network,
      this.trace,
      () => this.scheduler.now,
      (delay, type, payload) => this.scheduler.schedule(delay, type, payload),
      packet => this.deliver(packet),
      {
        minLatency: 1,
        maxLatency: 4,
        dropProbability: config.network?.dropProbability ?? faultScale * 0.15,
        duplicateProbability: config.network?.duplicateProbability ?? faultScale * 0.03,
        extraDelayProbability: config.network?.extraDelayProbability ?? faultScale * 0.25,
        maxExtraDelay: config.network?.maxExtraDelay ?? 24,
        ...config.network
      }
    )
    this.storage = new SimStorage(
      nodeIds,
      streams.storage,
      this.trace,
      () => this.scheduler.now,
      (delay, type, payload) => this.scheduler.schedule(delay, type, payload),
      {
        minLatency: 1,
        maxLatency: 3,
        failureProbability: faultScale * 0.05,
        droppedWriteProbability: faultScale * 0.02,
        corruptionProbability: faultScale * 0.005,
        extraDelayProbability: faultScale * 0.15,
        maxExtraDelay: 18,
        ...config.storage
      }
    )
  }

  run(): SimulationResult {
    for (const id of nodeIds) this.boot(id)
    this.scheduleWorkload()
    this.scheduleFaults()

    const plannedEnd = Math.max(
      0,
      ...this.workload.filter(item => item.time <= this.config.maxVirtualTime).map(item => item.time),
      ...this.faults.map(fault => fault.time)
    )
    const settleAt = Math.min(this.config.maxVirtualTime, plannedEnd + 1)
    this.scheduler.scheduleAt(settleAt, "settle", null)
    const finalTime = Math.min(this.config.maxVirtualTime, settleAt + 3 * this.maxElectionTimeout())

    try {
      this.scheduler.runUntil(finalTime, event => this.dispatch(event.type, event.payload))
      if (finalTime === settleAt + 3 * this.maxElectionTimeout()) {
        this.monitor.checkConvergence(this.snapshots(), this.scheduler.now, this.eventNumber)
      }
    } catch (error) {
      if (!(error instanceof InvariantViolation)) throw error
      this.violation = error
    }

    return {
      seed: this.config.seed,
      operations: this.workload.filter(item => item.time <= this.config.maxVirtualTime).length,
      completed: this.completed,
      failures: this.failures,
      virtualTime: this.scheduler.now,
      events: this.eventNumber,
      trace: this.trace,
      traceHash: this.trace.hash(),
      violation: this.violation
    }
  }

  private validateConfig(): void {
    if (!Number.isSafeInteger(this.config.seed)) throw new RangeError("Seed must be a safe integer")
    if (!Number.isSafeInteger(this.config.operations) || this.config.operations < 0) throw new RangeError("Operation count must be nonnegative")
    if (!Number.isFinite(this.config.faultRate) || this.config.faultRate < 0 || this.config.faultRate > 1) {
      throw new RangeError("Fault rate must be between zero and one")
    }
    if (!Number.isSafeInteger(this.config.maxVirtualTime) || this.config.maxVirtualTime < 1) {
      throw new RangeError("Maximum virtual time must be positive")
    }
  }

  private boot(id: NodeId): void {
    const generation = (this.generations.get(id) ?? 0) + 1
    this.generations.set(id, generation)
    const node = new DatabaseNode(id, this.nodeConfig(id), generation)
    this.nodes.set(id, node)
    this.applyEffects(id, node.handle({ type: "start" }))
    this.trace.record(this.scheduler.now, id, "START")
  }

  private nodeConfig(id: NodeId): NodeConfig {
    const electionTimeout = id === "node-a" ? 24 : id === "node-b" ? 36 : 48
    return { electionTimeout, heartbeatInterval: 6 }
  }

  private scheduleWorkload(): void {
    for (const item of this.workload) {
      if (item.time > this.config.maxVirtualTime) continue
      this.monitor.register(item.request)
      this.scheduler.scheduleAt(item.time, "client.request", { request: item.request } satisfies ClientEvent)
    }
  }

  private scheduleFaults(): void {
    for (const fault of this.faults) {
      this.scheduler.scheduleAt(fault.time, "fault", fault)
    }
  }

  private dispatch(type: string, payload: unknown): void {
    this.eventNumber += 1
    if (type === "network.delivery") {
      this.network.deliver(payload as Packet<ProtocolMessage>)
    } else if (type === "storage.complete") {
      this.completeStorage(payload as StorageOperation)
    } else if (type === "timer") {
      this.deliverTimer(payload as TimerEvent)
    } else if (type === "client.request") {
      this.dispatchClient((payload as ClientEvent).request)
    } else if (type === "input") {
      this.deliverInput(payload as InputEvent)
    } else if (type === "fault") {
      this.applyFault(payload as FaultEvent)
    } else if (type === "settle") {
      this.settle()
    }
    this.observeNodes()
  }

  private applyEffects(id: NodeId, effects: NodeEffect[]): void {
    for (const effect of effects) {
      if (effect.type === "send") {
        this.network.send(id, effect.to, effect.message)
      } else if (effect.type === "read") {
        this.storage.read(id, effect.key, effect.id, this.generations.get(id)!)
      } else if (effect.type === "write") {
        this.storage.write(id, effect.key, effect.value, effect.id, this.generations.get(id)!)
      } else if (effect.type === "schedule") {
        this.scheduler.schedule(effect.delay, "timer", {
          node: id,
          generation: this.generations.get(id)!,
          name: effect.name,
          token: effect.token
        } satisfies TimerEvent)
      } else if (effect.type === "client") {
        this.clientResult(id, effect.result)
      } else {
        this.trace.record(this.scheduler.now, id, effect.action, effect.data, effect.target ?? null)
        if (effect.action === "BECOME_LEADER") {
          const snapshot = this.snapshot(id)
          if (snapshot) this.monitor.checkLeader(snapshot, this.scheduler.now, this.eventNumber)
        }
      }
    }
  }

  private clientResult(id: NodeId, result: ClientResult): void {
    this.trace.record(this.scheduler.now, id, "CLIENT_RESULT", {
      id: result.id,
      status: result.status,
      version: result.version ?? -1,
      value: result.value ?? "null"
    }, "client")
    const node = this.nodes.get(id)
    const snapshot = node ? this.snapshot(id) : undefined
    if (snapshot) this.monitor.checkRead(result, snapshot, this.scheduler.now, this.eventNumber)
    this.monitor.clientResult(result, this.scheduler.now, this.eventNumber)
    if (result.status === "ok") this.completed += 1
    else this.failures += 1

    if (result.status === "ok") {
      const leader = this.currentLeader()
      const leaderSnapshot = leader ? this.snapshot(leader.id) : undefined
      if (leaderSnapshot) this.monitor.checkLeader(leaderSnapshot, this.scheduler.now, this.eventNumber)
    }
  }

  private completeStorage(operation: StorageOperation): void {
    const id = operation.node as NodeId
    const node = this.nodes.get(id)
    if (!node || this.generations.get(id) !== operation.generation) {
      this.storage.cancel(operation)
      return
    }
    const result = this.storage.complete(operation)
    try {
      this.applyEffects(operation.node as NodeId, node.handle({
        type: "storage",
        id: result.id,
        value: result.value,
        error: result.error
      }))
    } catch (error) {
      if (operation.kind === "read" && (error instanceof SyntaxError || error instanceof TypeError)) {
        throw new InvariantViolation("STORAGE_CORRUPTION", this.config.seed, this.scheduler.now, this.eventNumber, {
          node: operation.node as NodeId,
          key: operation.key,
          actual: result.value
        })
      }
      throw error
    }
  }

  private deliverTimer(timer: TimerEvent): void {
    if (this.generations.get(timer.node) !== timer.generation) return
    const node = this.nodes.get(timer.node)
    if (node) this.applyEffects(timer.node, node.handle({ type: "timer", name: timer.name, token: timer.token }))
  }

  private deliverInput(event: InputEvent): void {
    if (this.generations.get(event.node) !== event.generation) return
    const node = this.nodes.get(event.node)
    if (node) this.applyEffects(event.node, node.handle(event.input))
  }

  private deliver(packet: Packet<ProtocolMessage>): void {
    const id = packet.to as NodeId
    const node = this.nodes.get(id)
    if (!node) {
      this.trace.record(this.scheduler.now, id, "NODE_UNAVAILABLE", { packet: packet.id }, packet.from)
      return
    }
    this.applyEffects(id, node.handle({ type: "message", from: packet.from as NodeId, message: packet.message }))
  }

  private dispatchClient(request: ClientRequest): void {
    const leader = this.currentLeader()
    const node = leader?.id ?? (this.nodes.has("node-a") ? "node-a" : nodeIds.find(id => this.nodes.has(id)))
    if (!node) {
      this.failures += 1
      this.trace.record(this.scheduler.now, "client", "UNAVAILABLE", { id: request.id })
      return
    }
    this.trace.record(this.scheduler.now, "client", request.operation.type.toUpperCase(), {
      id: request.id,
      key: request.operation.key,
      value: request.operation.type === "put" ? request.operation.value : ""
    }, node)
    const instance = this.nodes.get(node)
    if (instance) this.applyEffects(node, instance.handle({ type: "client", request }))
  }

  private applyFault(fault: FaultEvent): void {
    if (fault.type === "node_crash") {
      if (this.nodes.delete(fault.node)) this.trace.record(this.scheduler.now, fault.node, "CRASH")
      else this.trace.record(this.scheduler.now, fault.node, "CRASH_IGNORED")
      return
    }
    if (fault.type === "node_restart") {
      if (this.nodes.has(fault.node)) {
        this.trace.record(this.scheduler.now, fault.node, "RESTART_IGNORED")
      } else {
        this.boot(fault.node)
        this.trace.record(this.scheduler.now, fault.node, "RESTART")
      }
      return
    }
    if (fault.type === "partition") {
      this.network.partition(fault.groups)
      return
    }
    if (fault.type === "storage_corruption") {
      this.storage.corrupt(fault.node, "state")
      return
    }
    this.network.heal()
  }

  private settle(): void {
    this.network.heal()
    for (const id of nodeIds) if (!this.nodes.has(id)) this.boot(id)
    this.trace.record(this.scheduler.now, "simulation", "SETTLE")
  }

  private observeNodes(): void {
    for (const [id, node] of this.nodes) {
      if (!node.isReady) continue
      const snapshot = this.snapshot(id)
      if (snapshot) this.monitor.observe(snapshot, this.scheduler.now, this.eventNumber)
      if (node.currentRole === "leader") this.monitor.checkLeader(snapshot!, this.scheduler.now, this.eventNumber)
    }
  }

  private snapshot(id: NodeId): NodeSnapshot | undefined {
    const node = this.nodes.get(id)
    if (!node || !node.isReady) return undefined
    return {
      id,
      generation: this.generations.get(id)!,
      commitIndex: node.currentCommitIndex,
      log: node.logEntries,
      values: node.committedValues
    }
  }

  private snapshots(): NodeSnapshot[] {
    return nodeIds.map(id => this.snapshot(id)).filter((item): item is NodeSnapshot => item !== undefined)
  }

  private currentLeader(): DatabaseNode | undefined {
    return [...this.nodes.values()]
      .filter(node => node.currentRole === "leader")
      .sort((left, right) => right.currentTerm - left.currentTerm)[0]
  }

  private maxElectionTimeout(): number {
    return 48
  }
}
