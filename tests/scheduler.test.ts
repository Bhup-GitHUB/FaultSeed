import { describe, expect, test } from "bun:test"
import { EventScheduler, VirtualClock } from "../src/simulation/scheduler"

describe("event scheduler", () => {
  test("orders events by virtual time and insertion sequence", () => {
    const scheduler = new EventScheduler<string>()
    scheduler.scheduleAt(5, "work", "first")
    scheduler.scheduleAt(2, "work", "early")
    scheduler.scheduleAt(5, "work", "second")

    const handled: string[] = []
    scheduler.runUntil(8, event => handled.push(event.payload))

    expect(handled).toEqual(["early", "first", "second"])
    expect(scheduler.now).toBe(8)
  })

  test("assigns later sequences to events scheduled during dispatch", () => {
    const scheduler = new EventScheduler<string>()
    scheduler.scheduleAt(1, "work", "first")
    scheduler.scheduleAt(1, "work", "second")

    const handled: string[] = []
    scheduler.runUntil(1, event => {
      handled.push(event.payload)
      if (event.payload === "first") scheduler.schedule(0, "work", "third")
    })

    expect(handled).toEqual(["first", "second", "third"])
  })

  test("advances time without waiting and rejects backward scheduling", () => {
    const scheduler = new EventScheduler<string>()
    const clock = new VirtualClock(scheduler)
    scheduler.schedule(50, "work", "later")
    expect(clock.now()).toBe(0)
    scheduler.next()
    expect(clock.now()).toBe(50)
    expect(() => scheduler.scheduleAt(49, "work", "past")).toThrow(RangeError)
  })
})
