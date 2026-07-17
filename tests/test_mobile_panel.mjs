import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("../mobile_panel.js", import.meta.url), "utf8");
const panel = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

class FakeElement {
  constructor(tagName) {
    this.tagName = tagName;
    this.children = [];
    this.attributes = new Map();
    this.listeners = new Map();
    this.className = "";
    this.textContent = "";
    this.type = "";
    this.classList = {
      add: (...names) => this.#setClasses(names, true),
      remove: (...names) => this.#setClasses(names, false),
      contains: (name) => this.className.split(" ").includes(name),
      toggle: (name) => {
        const names = new Set(this.className.split(" ").filter(Boolean));
        const enabled = !names.has(name);
        if (enabled) names.add(name);
        else names.delete(name);
        this.className = Array.from(names).join(" ");
        return enabled;
      },
    };
  }

  #setClasses(names, enabled) {
    const current = new Set(this.className.split(" ").filter(Boolean));
    for (const name of names) enabled ? current.add(name) : current.delete(name);
    this.className = Array.from(current).join(" ");
  }

  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  remove() { this.removed = true; }
  setAttribute(name, value) { this.attributes.set(name, value); }
  getAttribute(name) { return this.attributes.get(name); }
  addEventListener(name, listener) { this.listeners.set(name, listener); }
  click() { this.listeners.get("click")?.(); }
}

class MetricElement extends FakeElement {
  constructor() {
    super("div");
    this.strong = new FakeElement("strong");
    this.span = new FakeElement("span");
  }

  querySelector(selector) { return selector === "strong" ? this.strong : this.span; }
}

class DashboardHost extends FakeElement {
  constructor() {
    super("host");
    this.loading = new FakeElement("div");
    this.content = new FakeElement("div");
    this.content.hidden = true;
    this.rate = new MetricElement();
    this.quotes = new MetricElement();
    this.confidence = new MetricElement();
    this.events = new FakeElement("div");
    this.total = new FakeElement("span");
    this.filters = ["", "topic_follow", "explicit_quote"].map((type) => {
      const button = new FakeElement("button");
      button.dataset = { type };
      button.setAttribute("aria-pressed", String(type === ""));
      return button;
    });
  }

  set innerHTML(_value) {}

  querySelector(selector) {
    return {
      ".proactive-feedback-loading": this.loading,
      ".proactive-feedback-content": this.content,
      ".proactive-feedback-rate strong": this.rate.strong,
      ".proactive-feedback-quotes": this.quotes,
      ".proactive-feedback-confidence": this.confidence,
      ".proactive-feedback-events": this.events,
      ".proactive-feedback-total": this.total,
    }[selector];
  }

  querySelectorAll(selector) {
    return selector === ".proactive-feedback-filter button" ? this.filters : [];
  }
}

test("mobile navigation describes the proactive feedback task", () => {
  assert.equal(panel.default.navigation.label, "主动反馈");
  assert.match(panel.default.navigation.description, /是否被继续/);
  assert.equal(typeof panel.default.dashboard.mount, "function");
  assert.doesNotMatch(source, /context\.request/);
  assert.match(source, /context\.query\("feedback\.overview"\)/);
});

test("feedback types keep stable semantic labels and tones", () => {
  assert.equal(panel.feedbackLabel("explicit_quote"), "明确引用");
  assert.equal(panel.feedbackTone("explicit_quote"), "quote");
  assert.equal(panel.feedbackLabel("topic_follow"), "继续话题");
  assert.equal(panel.feedbackTone("topic_follow"), "follow");
  assert.equal(panel.feedbackLabel("no_topic_follow"), "没有继续");
  assert.equal(panel.feedbackTone("no_topic_follow"), "neutral");
  assert.equal(panel.feedbackLabel("unscored"), "未能判断");
  assert.equal(panel.feedbackTone("unscored"), "uncertain");
});

test("mobile panel keeps content in plugin-owned module", () => {
  assert.match(source, /主动发出/);
  assert.match(source, /用户回应/);
  assert.match(source, /助手继续/);
  assert.doesNotMatch(source, /window\.AkashicDashboard/);
});

test("feedback row expands its relation and keeps aria state in sync", () => {
  globalThis.document = {
    createElement(tagName) { return new FakeElement(tagName); },
  };
  const row = panel.eventRow({
    feedback_type: "topic_follow",
    created_at: "2026-07-17T08:30:00Z",
    proactive_preview: "先前的主动消息",
    user_reply_preview: "用户继续了这个话题",
    assistant_preview: "助手接着回答",
  });
  const trigger = row.children[0];
  const detail = row.children[1];

  assert.equal(trigger.getAttribute("aria-expanded"), "false");
  assert.equal(detail.getAttribute("aria-hidden"), "true");
  assert.equal(detail.inert, true);
  assert.equal(detail.children[0].children[0].children.length, 3);
  trigger.click();
  assert.equal(trigger.getAttribute("aria-expanded"), "true");
  assert.equal(detail.getAttribute("aria-hidden"), "false");
  assert.equal(detail.inert, false);
  assert.match(row.className, /is-expanded/);
  trigger.click();
  assert.equal(trigger.getAttribute("aria-expanded"), "false");
  assert.equal(detail.getAttribute("aria-hidden"), "true");
  assert.equal(detail.inert, true);
  assert.doesNotMatch(row.className, /is-expanded/);
});

test("dashboard clears a failed filter state after the next filter succeeds", async () => {
  globalThis.document = {
    createElement(tagName) { return new FakeElement(tagName); },
  };
  const host = new DashboardHost();
  const context = {
    query(method, payload = {}) {
      if (method === "feedback.overview") {
        return Promise.resolve({ follow_rate: 0.4, explicit_quote: 2, high_confidence: 3 });
      }
      if (payload.feedback_type === "topic_follow") return Promise.reject(new Error("temporary"));
      return Promise.resolve({ items: [], total: 0 });
    },
  };
  panel.default.dashboard.mount(host, context);
  await new Promise((resolve) => setImmediate(resolve));

  host.filters[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(host.events.classList.contains("error"), true);

  host.filters[2].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(host.events.classList.contains("error"), false);
  assert.equal(host.events.classList.contains("is-loading"), false);
});
