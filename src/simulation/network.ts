import { Random } from "./random"
import { TraceLog } from "./trace"

export type Packet<M> = {
  id: number
  copy: number
  from: string
  to: string
  message: M
}

export type NetworkConfig = {
  minLatency: number
  maxLatency: number
  dropProbability: number
  duplicateProbability: number
  extraDelayProbability: number
  maxExtraDelay: number
}

export type Schedule = (delay: number, type: string, payload: unknown) => void

const defaults: NetworkConfig = {
  minLatency: 1,
  maxLatency: 4,
  dropProbability: 0,
  duplicateProbability: 0,
  extraDelayProbability: 0,
  maxExtraDelay: 10
}

export class SimNetwork<M> {
  private readonly config: NetworkConfig
  private partitions = new Map<string, number>()
  private nextPacketId = 1

  constructor(
    private readonly nodes: readonly string[],
    private readonly random: Random,
    private readonly trace: TraceLog,
    private readonly now: () => number,
    private readonly schedule: Schedule,
    private readonly deliverPacket: (packet: Packet<M>) => void,
    config: Partial<NetworkConfig> = {}
  ) {
    this.config = { ...defaults, ...config }
    this.validateConfig()
  }

  send(from: string, to: string, message: M): number {
    if (from === to) throw new RangeError("Network messages require distinct nodes")
    if (!this.nodes.includes(from) || !this.nodes.includes(to)) throw new RangeError("Unknown network node")

    const id = this.nextPacketId
    this.nextPacketId += 1

    if (this.isPartitioned(from, to)) {
      this.trace.record(this.now(), from, "PARTITION_BLOCK", { id }, to)
      return id
    }

    if (this.random.bool(this.config.dropProbability)) {
      this.trace.record(this.now(), from, "PACKET_DROP", { id }, to)
      return id
    }

    const latency = this.latency()
    const packet = { id, copy: 0, from, to, message }
    this.schedule(latency, "network.delivery", packet)
    this.trace.record(this.now(), from, "PACKET_SEND", { id, latency }, to)

    if (this.random.bool(this.config.duplicateProbability)) {
      const duplicateDelay = this.latency()
      this.schedule(latency + duplicateDelay, "network.delivery", { ...packet, copy: 1 })
      this.trace.record(this.now(), from, "PACKET_DUPLICATE", { id, delay: latency + duplicateDelay }, to)
    }

    return id
  }

  deliver(packet: Packet<M>): void {
    this.trace.record(this.now(), packet.from, "PACKET_DELIVER", { id: packet.id, copy: packet.copy }, packet.to)
    this.deliverPacket(packet)
  }

  partition(groups: readonly (readonly string[])[]): void {
    const next = new Map<string, number>()
    groups.forEach((group, index) => {
      if (group.length === 0) throw new RangeError("Partition groups cannot be empty")
      for (const node of group) {
        if (!this.nodes.includes(node) || next.has(node)) throw new RangeError("Invalid partition membership")
        next.set(node, index)
      }
    })
    if (next.size !== this.nodes.length) throw new RangeError("Partition must include every network node")
    this.partitions = next
    this.trace.record(this.now(), "network", "PARTITION", { groups: groups.length })
  }

  heal(): void {
    this.partitions.clear()
    this.trace.record(this.now(), "network", "HEAL")
  }

  private isPartitioned(from: string, to: string): boolean {
    if (this.partitions.size === 0) return false
    return this.partitions.get(from) !== this.partitions.get(to)
  }

  private latency(): number {
    let delay = this.random.int(this.config.minLatency, this.config.maxLatency)
    if (this.random.bool(this.config.extraDelayProbability)) {
      delay += this.random.int(1, this.config.maxExtraDelay)
    }
    return delay
  }

  private validateConfig(): void {
    const { minLatency, maxLatency, dropProbability, duplicateProbability, extraDelayProbability, maxExtraDelay } = this.config
    if (!Number.isSafeInteger(minLatency) || minLatency < 0) throw new RangeError("Minimum latency must be nonnegative")
    if (!Number.isSafeInteger(maxLatency) || maxLatency < minLatency) throw new RangeError("Invalid maximum latency")
    if (!Number.isSafeInteger(maxExtraDelay) || maxExtraDelay < 1) throw new RangeError("Maximum extra delay must be positive")
    for (const probability of [dropProbability, duplicateProbability, extraDelayProbability]) {
      if (!Number.isFinite(probability) || probability < 0 || probability > 1) {
        throw new RangeError("Network probabilities must be between zero and one")
      }
    }
  }
}
