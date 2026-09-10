import { describe, expect, test } from "bun:test"
import path from "node:path"
import { isApplePlatform } from "../src/renderer/src/components/titlebar"

const styles = path.join(import.meta.dirname, "..", "src", "renderer", "src", "styles", "global")

describe("macOS traffic lights overlap", () => {
  test("detects Apple platforms for native window controls", () => {
    expect(isApplePlatform("MacIntel", "")).toBe(true)
    expect(isApplePlatform("", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")).toBe(true)
    expect(isApplePlatform("iPhone", "")).toBe(true)
    expect(isApplePlatform("iPad", "")).toBe(true)
    expect(isApplePlatform("Win32", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")).toBe(false)
    expect(isApplePlatform("Linux x86_64", "Mozilla/5.0 (X11; Linux x86_64)")).toBe(false)
  })

  test("reserves the native traffic-light inset on darwin", async () => {
    const titlebar = await Bun.file(path.join(styles, "shell", "titlebar.css")).text()
    const menu = await Bun.file(path.join(styles, "overlays", "titlebar-menu.css")).text()
    expect(titlebar).toContain('[data-platform="darwin"]')
    expect(titlebar).toContain("height: 38px")
    expect(titlebar).toContain("--titlebar-traffic-inset: 78px")
    expect(titlebar).toContain("padding-left: var(--titlebar-traffic-inset)")
    expect(menu).toContain('titlebar[data-platform="darwin"] .titlebar-menu')
    expect(menu).toContain("padding-left: var(--titlebar-traffic-inset)")
  })

  test("keeps the darwin titlebar draggable without trapping menu clicks", async () => {
    const titlebar = await Bun.file(path.join(styles, "shell", "titlebar.css")).text()
    const menu = await Bun.file(path.join(styles, "overlays", "titlebar-menu.css")).text()
    expect(titlebar).toMatch(/\[data-platform="darwin"\][\s\S]*?-webkit-app-region: drag/)
    expect(menu).not.toMatch(/\.titlebar-menu \{[^}]*-webkit-app-region: no-drag/)
    expect(menu).toMatch(/\.titlebar-menu-trigger[\s\S]*?-webkit-app-region: no-drag/)
  })
})
