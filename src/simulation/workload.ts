import type { ClientRequest } from "../db/types"
import { Random } from "./random"

export type WorkloadItem = {
  time: number
  request: ClientRequest
}

export function generateWorkload(random: Random, operations: number, startTime = 60): WorkloadItem[] {
  if (!Number.isSafeInteger(operations) || operations < 0) throw new RangeError("Operation count must be nonnegative")
  if (!Number.isSafeInteger(startTime) || startTime < 0) throw new RangeError("Start time must be nonnegative")

  const workload: WorkloadItem[] = []
  let time = startTime

  for (let index = 0; index < operations; index += 1) {
    time += random.int(1, 5)
    const key = `key-${random.int(0, 15)}`
    const choice = random.next()
    const operation = choice < 0.45
      ? { type: "put" as const, key, value: `value-${random.int(0, 0xffffff).toString(16)}` }
      : choice < 0.75
        ? { type: "get" as const, key }
        : { type: "delete" as const, key }

    workload.push({ time, request: { id: `request-${index}`, operation } })
  }

  return workload
}
