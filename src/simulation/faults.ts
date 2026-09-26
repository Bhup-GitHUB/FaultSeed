import type { NodeId } from "../db/types"
import { Random } from "./random"
import type { WorkloadItem } from "./workload"

export type FaultEvent =
  | { time: number; order: number; type: "node_crash"; node: NodeId }
  | { time: number; order: number; type: "node_restart"; node: NodeId }
  | { time: number; order: number; type: "partition"; groups: NodeId[][] }
  | { time: number; order: number; type: "heal" }

const nodes: readonly NodeId[] = ["node-a", "node-b", "node-c"]

export function generateFaults(random: Random, workload: readonly WorkloadItem[], rate: number, maxTime: number): FaultEvent[] {
  if (!Number.isFinite(rate) || rate < 0 || rate > 1) throw new RangeError("Fault rate must be between zero and one")
  if (!Number.isSafeInteger(maxTime) || maxTime < 0) throw new RangeError("Maximum virtual time must be nonnegative")

  const faults: FaultEvent[] = []
  let order = 0

  for (const item of workload) {
    if (item.time > maxTime) continue
    if (!random.bool(rate)) continue
    const time = Math.min(item.time + random.int(0, 3), maxTime)
    const duration = random.int(8, 24)

    if (random.bool(0.5)) {
      const node = random.pick(nodes)
      faults.push({ time, order, type: "node_crash", node })
      order += 1
      faults.push({ time: Math.min(time + duration, maxTime), order, type: "node_restart", node })
      order += 1
    } else {
      const isolated = random.pick(nodes)
      const others = nodes.filter(node => node !== isolated)
      faults.push({ time, order, type: "partition", groups: [[isolated], others] })
      order += 1
      faults.push({ time: Math.min(time + duration, maxTime), order, type: "heal" })
      order += 1
    }
  }

  return faults.sort((left, right) => left.time - right.time || left.order - right.order)
}
