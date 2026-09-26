import { describe, expect, test } from "bun:test"
import { parseCli } from "../src/cli"

describe("CLI arguments", () => {
  test("parses options and positional arguments", () => {
    const result = parseCli(["replay", "48291", "--trace", "--ops", "50"])

    expect(result.command).toBe("replay")
    expect(result.positionals).toEqual(["48291"])
    expect(result.options.get("trace")).toBe(true)
    expect(result.options.get("ops")).toBe("50")
  })

  test("rejects an empty option name", () => {
    expect(() => parseCli(["sim", "--"])).toThrow("Empty option name")
  })
})
