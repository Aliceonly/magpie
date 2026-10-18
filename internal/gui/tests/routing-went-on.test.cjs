// Run with Node's test runner and Playwright on the module path; see README.md.
// What came of a failed try is told from what the gateway did next (#2281,
// xishuchkui-code): an Antigravity account's 502 empty reply, which doesn't
// rest it, went on to the next account, which answered. The Routing page's
// story said "an error another account wouldn't fix, so Claude Code gets
// it", then told of the next account answering, and the account's row said
// the error was passed to Claude Code. A later try is the request going on;
// with none, the agent got the error. And the last one left that rests
// (a key, #1772), or was slow to begin, says nobody is left, not that the
// request goes on; the last one out of its allowance after another failed
// says the agent got the earlier error (Try.Earlier).
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const { test } = require("node:test");
const { chromium, webkit } = require("playwright");

const assets = path.resolve(__dirname, "../assets");
const now = new Date();
const day = [now.getFullYear(), now.getMonth() + 1, now.getDate()].map((n) => String(n).padStart(2, "0")).join("-");
const at = (i) => new Date(now.getTime() - (i + 1) * 60e3).toISOString();
const until = new Date(now.getTime() + 3600e3).toISOString();
const empty = "Antigravity: the model returned an empty reply";
const agA = { id: "antigravity@a@example.com", provider: "antigravity", name: "Antigravity", kind: "account", who: "a@example.com", model: "claude-sonnet-4-6" };
const agB = { id: "antigravity@b@example.com", provider: "antigravity", name: "Antigravity", kind: "account", who: "b@example.com", model: "claude-sonnet-4-6" };
const k1 = { id: "relay#1", provider: "relay", name: "Relay", kind: "key", who: "key one", model: "claude-sonnet-4-6" };
const k2 = { id: "relay#2", provider: "relay", name: "Relay", kind: "key", who: "key two", model: "claude-sonnet-4-6" };
const slow = { id: "slowpoke", provider: "slowpoke", name: "Slowpoke", kind: "provider", model: "claude-sonnet-4-6" };
const grok = { id: "xai", provider: "xai", name: "Grok", kind: "provider", model: "grok-4" };
const codex = { id: "codex@c@example.com", provider: "codex", name: "Codex", kind: "account", who: "c@example.com", model: "gpt-5.5" };
const rest = (why, status) => ({ status, why, until });
const routes = [{
  // the reporter's: A's 502 empty reply, no rest, then B answers
  id: 104, seq: 104, time: at(0), agent: "claude", model: "group/g", provider: "antigravity", order: [agA, agB],
  tries: [
    { id: agA.id, model: agA.model, start: at(0), done: true, status: 502, ms: 900, fail: "other", error: empty },
    { id: agB.id, model: agB.model, start: at(0), done: true, status: 200, ms: 4000 },
  ],
  done: true, status: 200, ms: 5000, fallback: empty,
}, {
  // the last key left rests, and the agent gets its 429
  id: 103, seq: 103, time: at(1), agent: "claude", model: "relay/claude-sonnet-4-6", provider: "relay", order: [k1, k2],
  tries: [
    { id: k1.id, model: k1.model, start: at(1), done: true, status: 429, ms: 100, fail: "quota", rest: rest("quota", 429) },
    { id: k2.id, model: k2.model, start: at(1), done: true, status: 429, ms: 100, fail: "quota", rest: rest("quota", 429) },
  ],
  done: true, status: 429, error: "Relay: quota", ms: 300,
}, {
  // the only member left hadn't begun answering
  id: 102, seq: 102, time: at(2), agent: "claude", model: "slowpoke/claude-sonnet-4-6", provider: "slowpoke", order: [slow],
  tries: [{ id: slow.id, model: slow.model, start: at(2), done: true, status: 504, ms: 30000, fail: "slow", error: "Slowpoke: no first token in 30s" }],
  done: true, status: 504, error: "Slowpoke: no first token in 30s", ms: 30000,
}, {
  // Grok busy, then the Codex account out of its allowance: Codex is told Grok's error
  id: 101, seq: 101, time: at(3), agent: "claude", model: "group/h", provider: "codex", order: [grok, codex],
  tries: [
    { id: grok.id, model: grok.model, start: at(3), done: true, status: 503, ms: 200, fail: "busy", error: "Grok: upstream busy", rest: rest("busy", 503) },
    { id: codex.id, model: codex.model, start: at(3), done: true, status: 429, ms: 200, fail: "quota", earlier: true },
  ],
  done: true, status: 503, error: "Grok: upstream busy", ms: 500,
}];
const heldCases = [
  { reason: "buffer", status: 502, next: true },
  { reason: "time", status: 504, next: false },
  { reason: "request-idle", status: 504, next: false },
];
routes.push(...heldCases.map((c, i) => {
  const error = `magpie: local limit for holding search output (${c.reason})`;
  return {
    id: 100 - i, seq: 100 - i, time: at(4 + i), agent: "codex", model: "group/search", provider: c.next ? "xai" : "codex",
    // An unasked member in the order is no evidence of fallback.
    order: [codex, grok],
    tries: [
      { id: codex.id, model: codex.model, start: at(4 + i), done: true, status: c.status, ms: 900, fail: "held_limit", error },
      ...(c.next ? [{ id: grok.id, model: grok.model, start: at(4 + i), done: true, status: 200, ms: 1000 }] : []),
    ],
    done: true, status: c.next ? 200 : c.status, ms: c.next ? 1900 : 900,
    ...(c.next ? { fallback: error } : { error }),
  };
}));

function serve(lang, live) {
  const state = { agents: [
    { id: "claude", name: "Claude Code", path: "/test/settings.json", fields: [] },
    { id: "codex", name: "Codex", path: "/test/config.toml", fields: [] },
  ], profiles: [], settings: { lang, theme: "light" } };
  return async (route) => {
    const url = new URL(route.request().url());
    const json = (data) => route.fulfill({ json: data });
    if (url.pathname === "/boot.js") return route.fulfill({ contentType: "text/javascript", body: `window.bootPrefs = {lang:"${lang}",theme:"light",web:true};` });
    if (url.pathname === "/wails/runtime.js") return route.fulfill({ contentType: "text/javascript", body: "export const Window = {};" });
    if (url.pathname === "/api/state") return json(state);
    if (url.pathname === "/api/plugins") return json({ plugins: [] });
    if (url.pathname === "/api/gateway/trace") {
      if (Number(url.searchParams.get("after")) >= live.seq) await new Promise((r) => { live.wake = r; });
      return json({ mine: true, now: new Date().toISOString(), seq: live.seq, totals: { requests: 0, rerouted: 0, errors: 0 }, routes: live.routes });
    }
    if (url.pathname === "/api/gateway/history") {
      const d = url.searchParams.get("day");
      return json({ cut: false, days: [{ day, requests: routes.length }], routes: d ? routes : [] });
    }
    if (url.pathname === "/api/groups") return json({ groups: [] });
    if (url.pathname === "/api/providers") return json({ providers: [], presets: [], excluded: [], gateway: { running: true, window: true } });
    if (url.pathname.startsWith("/api/")) return json({});
    const file = path.join(assets, url.pathname === "/" ? "index.html" : url.pathname);
    const contentType = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" }[path.extname(file)];
    await route.fulfill({ body: await fs.readFile(file), contentType });
  };
}

// went: A went on to B; gets: the old "gets it" for A; passed: A's row
// saying the error was passed on; left: nobody is left; on: going on to
// the next; earlier: the agent got the earlier error
const want = {
  en: { went: "and the request goes on to b@example.com before any of the reply reaches Claude Code", gets: ["wouldn't fix", "so Claude Code gets"], passed: "passed to", row: "went on to the next",
    left: "nobody is left to try, so Claude Code gets the error", on: "goes on to", earlier: "Claude Code gets the error an earlier try gave",
    held: ["magpie canceled", "local limit for holding search output", "No search output reached Codex", "doesn't rest for this"],
    heldNext: "The request went to Grok", heldError: "Codex gets the error", heldPending: "The request is still in progress", heldLabel: "local search hold limit", success: "answered in", answered: "answered " },
  zh: { went: "请求在回复到达 Claude Code 前转给 b@example.com", gets: ["换账号也无法解决", "Claude Code 收到此错误"], passed: "已交给", row: "已转给下一个",
    left: "且无其他可试，Claude Code 收到此错误", on: "转给", earlier: "Claude Code 收到的是此前某次尝试的错误",
    held: ["magpie", "本地搜索暂存上限", "取消了 c@example.com", "搜索内容未交付给 Codex", "不因此休息"],
    heldNext: "请求已转给 Grok", heldError: "Codex 收到此错误", heldPending: "请求仍在进行中", heldLabel: "本地搜索暂存上限", success: "完成回答", answered: "返回 " },
  "zh-TW": { went: "請求在回覆到達 Claude Code 前轉給 b@example.com", gets: ["換帳號也無法解決", "Claude Code 收到此錯誤"], passed: "已交給", row: "已轉給下一個",
    left: "且無其他可試，Claude Code 收到此錯誤", on: "轉給", earlier: "Claude Code 收到的是先前某次嘗試的錯誤",
    held: ["magpie", "本機搜尋暫存上限", "取消了 c@example.com", "搜尋內容未交付給 Codex", "不因此休息"],
    heldNext: "請求已轉給 Grok", heldError: "Codex 收到此錯誤", heldPending: "請求仍在進行中", heldLabel: "本機搜尋暫存上限", success: "完成回答", answered: "返回 " },
  ja: { went: "リクエストは b@example.com に回ります", gets: ["別のアカウントでも解決しない", "Claude Code に届きます"], passed: "渡しました", row: "次に回しました",
    left: "他に試せる相手がいないため、Claude Code にエラーが届きます", on: "に回ります", earlier: "それより前の試行のエラー",
    held: ["magpie", "検索出力を保持するローカル上限", "試行を中止しました", "検索出力は Codex に届いていません", "これで休止しません"],
    heldNext: "リクエストは Grok に引き継がれました", heldError: "Codex にこのエラーが返ります", heldPending: "リクエストはまだ進行中です", heldLabel: "検索出力の保持上限（ローカル）", success: "で応答しました", answered: "が " },
  de: { went: "die Anfrage geht an b@example.com, bevor etwas von der Antwort Claude Code erreicht", gets: ["nicht beheben würde", "daher erhält Claude Code ihn"], passed: "übergeben", row: "an den nächsten weitergegeben",
    left: "es bleibt niemand zum Versuchen, daher erhält Claude Code den Fehler", on: "geht an", earlier: "Claude Code erhält den Fehler eines früheren Versuchs",
    held: ["magpie hat den Versuch", "lokalen Limit für das Zurückhalten von Suchausgaben", "abgebrochen", "Keine Suchausgabe hat Codex erreicht", "pausiert deshalb nicht"],
    heldNext: "Die Anfrage ging an Grok", heldError: "Codex erhält den Fehler", heldPending: "Die Anfrage läuft noch", heldLabel: "lokales Limit für zurückgehaltene Suchausgabe", success: "antwortete in", answered: "antwortete mit " },
};

for (const engine of (process.env.BROWSER ? [process.env.BROWSER] : ["chromium", "webkit"])) {
  for (const lang of Object.keys(want)) {
    for (const width of [1100, 420]) {
      test(`${engine} ${lang} ${width}: a try the gateway went on from says so, not that the agent got it`, async (t) => {
        const browser = await (engine === "webkit" ? webkit.launch() : chromium.launch({ channel: "chromium" }));
        const context = await browser.newContext({ viewport: { width, height: 760 }, reducedMotion: "reduce" });
        const page = await context.newPage();
        page.setDefaultTimeout(5000);
        const errors = [];
        page.on("pageerror", (e) => errors.push(e.message));
        const live = { seq: 1, routes: [], wake: null };
        const publish = (r) => {
          live.routes = [r];
          live.seq++;
          live.wake?.();
          live.wake = null;
        };
        await page.route("**/*", serve(lang, live));
        t.after(async () => {
          if (process.env.ARTIFACT_DIR) {
            await fs.mkdir(process.env.ARTIFACT_DIR, { recursive: true });
            await page.screenshot({ path: path.join(process.env.ARTIFACT_DIR, `${engine}-${lang}-${width}-went-on.png`), fullPage: true });
          }
          await browser.close();
        });
        await page.goto("http://magpie.test/?view=routing");
        await page.locator(".rt-days .rt-day").nth(1).click();
        await page.locator(".rt-req").nth(routes.length - 1).waitFor();
        const w = want[lang];
        const stepsOf = async (n) => {
          // at 420 in WebKit the footer stands over the list's foot, where
          // a reader scrolls the list first: the row's own click is enough
          await page.locator(".rt-req").nth(n).dispatchEvent("click");
          await page.waitForTimeout(300);
          return page.locator(".rt-steps li").allTextContents();
        };

        // the reporter's: A went on to B, and B answered
        let steps = await stepsOf(0);
        const a = steps.find((s) => s.includes("a@example.com (") && s.includes("502"));
        assert(a, JSON.stringify(steps));
        assert(a.includes(w.went), a);
        for (const g of w.gets) assert(!a.includes(g), a);
        assert(!steps.some((s) => w.gets.some((g) => s.includes(g))), JSON.stringify(steps));
        const rowA = page.locator(".rt-accts li").filter({ hasText: "a@example.com" }).first();
        const said = await rowA.locator("em").first().textContent();
        assert(!said.includes(w.passed) && said.includes("502") && said.includes(w.row), said);

        // the last key left rests: nobody is left, it doesn't go on
        steps = await stepsOf(1);
        const k = steps.find((s) => s.includes("key two (") && s.indexOf("key two") < s.indexOf("429"));
        assert(k, JSON.stringify(steps));
        assert(k.includes(w.left) && !k.includes(w.on), k);

        // the only one, slow to begin: the agent gets the error
        steps = await stepsOf(2);
        const sl = steps.find((s) => s.includes("Slowpoke (") && s.includes("30"));
        assert(sl, JSON.stringify(steps));
        assert(sl.includes(w.left) && !sl.includes(w.on), sl);

        // the last one out of its allowance: the agent got Grok's error
        steps = await stepsOf(3);
        const c = steps.find((s) => s.includes("c@example.com (") && s.includes("429"));
        assert(c, JSON.stringify(steps));
        assert(c.includes(w.earlier), c);

        for (const [i, held] of heldCases.entries()) {
          steps = await stepsOf(4 + i);
          const local = steps.find((s) => s.includes("c@example.com (") && s.includes(String(held.status)));
          assert(local, JSON.stringify(steps));
          for (const text of w.held) assert(local.includes(text), local);
          assert(local.includes(held.next ? w.heldNext : w.heldError), local);
          assert(!local.includes(held.next ? w.heldError : w.heldNext), local);
          assert(!local.includes(w.answered + held.status) && !local.includes(w.gets[0]), local);
          const row = page.locator(".rt-accts li").filter({ hasText: "c@example.com" }).first();
          const summary = await row.locator("em").first().textContent();
          assert(summary.includes(w.heldLabel) && summary.includes(String(held.status)), summary);
          assert(summary.includes(held.next ? w.row : w.passed), summary);
        }

        await page.locator(".rt-days .rt-day").first().click();
        // A completed try is not a completed request. Exercise both later
        // outcomes through the same live trace updates the page consumes.
        for (const next of [true, false]) {
          const pending = { ...routes[4], id: next ? 201 : 202, time: new Date().toISOString(),
            done: false, status: 0, tries: [routes[4].tries[0]] };
          publish(pending);
          const local = page.locator(".rt-steps li").filter({ hasText: "c@example.com (" }).filter({ hasText: "502" });
          await local.waitFor();
          if (next) assert(!(await local.textContent()).includes(w.heldError), "an unfinished request must not say the client gets the error");
          await local.filter({ hasText: w.heldPending }).waitFor();
          let said = await local.textContent();
          for (const text of w.held) assert(said.includes(text), said);
          assert(!said.includes(w.heldError) && !said.includes(w.heldNext), said);
          const summary = await page.locator(".rt-accts li").filter({ hasText: "c@example.com" }).first().locator("em").first().textContent();
          assert(!summary.includes(w.passed) && !summary.includes(w.row), summary);

          publish(next
            ? { ...pending, tries: [...pending.tries, { ...routes[4].tries[1], done: false, status: 0 }] }
            : { ...pending, done: true, status: 502, error: pending.tries[0].error });
          await local.filter({ hasText: next ? w.heldNext : w.heldError }).waitFor();
          said = await local.textContent();
          assert(!said.includes(w.heldPending) && !said.includes(next ? w.heldError : w.heldNext), said);
          if (next) {
            publish({ ...pending, done: true, status: 200, tries: routes[4].tries });
            await page.locator(".rt-steps li").filter({ hasText: "Grok (" }).filter({ hasText: w.success }).waitFor();
          }
        }

        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, "the page scrolls sideways");
        assert.deepEqual(errors, []);
      });
    }
  }
}
