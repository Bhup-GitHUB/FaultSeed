export type ScheduledEvent<T> = {
  time: number
  sequence: number
  type: string
  payload: T
}

export class EventScheduler<T> {
  private heap: ScheduledEvent<T>[] = []
  private time = 0
  private sequence = 0

  get now(): number {
    return this.time
  }

  get size(): number {
    return this.heap.length
  }

  peek(): ScheduledEvent<T> | undefined {
    return this.heap[0]
  }

  schedule(delay: number, type: string, payload: T): ScheduledEvent<T> {
    if (!Number.isFinite(delay) || delay < 0) throw new RangeError("Delay must be nonnegative")
    return this.scheduleAt(this.time + delay, type, payload)
  }

  scheduleAt(time: number, type: string, payload: T): ScheduledEvent<T> {
    if (!Number.isFinite(time) || time < this.time) throw new RangeError("Event time cannot move backwards")
    const event = { time, sequence: this.sequence, type, payload }
    this.sequence += 1
    this.push(event)
    return event
  }

  next(): ScheduledEvent<T> | undefined {
    const event = this.pop()
    if (event) this.time = event.time
    return event
  }

  runUntil(until: number, dispatch: (event: ScheduledEvent<T>) => void, maxEvents = Infinity): number {
    if (!Number.isFinite(until) || until < this.time) throw new RangeError("Cannot move virtual time backwards")
    if (maxEvents <= 0) throw new RangeError("Event limit must be positive")

    let count = 0
    while (this.peek() && this.peek()!.time <= until) {
      if (count >= maxEvents) throw new RangeError("Event limit reached")
      const event = this.next()!
      dispatch(event)
      count += 1
    }
    this.time = until
    return count
  }

  private push(event: ScheduledEvent<T>): void {
    this.heap.push(event)
    let index = this.heap.length - 1
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2)
      if (!this.less(this.heap[index], this.heap[parent])) break
      this.swap(index, parent)
      index = parent
    }
  }

  private pop(): ScheduledEvent<T> | undefined {
    if (this.heap.length === 0) return undefined
    const first = this.heap[0]
    const last = this.heap.pop()!
    if (this.heap.length > 0) {
      this.heap[0] = last
      this.siftDown(0)
    }
    return first
  }

  private siftDown(start: number): void {
    let index = start
    while (true) {
      const left = index * 2 + 1
      const right = left + 1
      let smallest = index
      if (left < this.heap.length && this.less(this.heap[left], this.heap[smallest])) smallest = left
      if (right < this.heap.length && this.less(this.heap[right], this.heap[smallest])) smallest = right
      if (smallest === index) return
      this.swap(index, smallest)
      index = smallest
    }
  }

  private less(left: ScheduledEvent<T>, right: ScheduledEvent<T>): boolean {
    return left.time < right.time || (left.time === right.time && left.sequence < right.sequence)
  }

  private swap(left: number, right: number): void {
    const value = this.heap[left]
    this.heap[left] = this.heap[right]
    this.heap[right] = value
  }
}

export class VirtualClock<T> {
  constructor(private readonly scheduler: EventScheduler<T>) {}

  now(): number {
    return this.scheduler.now
  }
}
