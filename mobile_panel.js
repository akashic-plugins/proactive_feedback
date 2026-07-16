function number(value) {
  return new Intl.NumberFormat("zh-CN").format(Number(value || 0));
}

function rate(value) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "—";
}

function shortTime(value) {
  const date = new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) return String(value || "—");
  return new Intl.DateTimeFormat("zh-CN", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

export function feedbackLabel(value) {
  if (value === "explicit_quote") return "明确引用";
  if (value === "topic_follow") return "继续话题";
  if (value === "no_topic_follow") return "没有继续";
  if (value === "unscored") return "未能判断";
  return "其他反馈";
}

export function feedbackTone(value) {
  if (value === "explicit_quote") return "quote";
  if (value === "topic_follow") return "follow";
  if (value === "unscored") return "uncertain";
  return "neutral";
}

function relationStep(label, text) {
  const item = document.createElement("li");
  const title = document.createElement("span");
  title.textContent = label;
  const content = document.createElement("p");
  content.textContent = text || "没有可显示的内容";
  item.append(title, content);
  return item;
}

export function eventRow(event) {
  const item = document.createElement("article");
  item.className = `proactive-feedback-event proactive-feedback-event--${feedbackTone(event.feedback_type)}`;

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "proactive-feedback-event__trigger";
  trigger.setAttribute("aria-expanded", "false");

  const signal = document.createElement("span");
  signal.className = "proactive-feedback-event__signal";
  signal.textContent = feedbackLabel(event.feedback_type);
  const time = document.createElement("time");
  time.textContent = shortTime(event.created_at);
  const preview = document.createElement("strong");
  preview.textContent = event.user_reply_preview || event.user_preview || "（没有用户回复摘要）";
  const hint = document.createElement("span");
  hint.className = "proactive-feedback-event__hint";
  hint.textContent = "查看关联链路";
  trigger.append(signal, time, preview, hint);

  const detail = document.createElement("div");
  detail.className = "proactive-feedback-event__detail";
  detail.inert = true;
  detail.setAttribute("aria-hidden", "true");
  const detailInner = document.createElement("div");
  detailInner.className = "proactive-feedback-event__detail-inner";
  const relation = document.createElement("ol");
  relation.className = "proactive-feedback-relation";
  relation.append(
    relationStep("主动发出", event.proactive_preview),
    relationStep("用户回应", event.user_reply_preview || event.user_preview),
    relationStep("助手继续", event.assistant_preview),
  );
  detailInner.append(relation);
  detail.append(detailInner);

  trigger.addEventListener("click", () => {
    const expanded = item.classList.toggle("is-expanded");
    trigger.setAttribute("aria-expanded", String(expanded));
    detail.inert = !expanded;
    detail.setAttribute("aria-hidden", String(!expanded));
    hint.textContent = expanded ? "收起关联链路" : "查看关联链路";
  });
  item.append(trigger, detail);
  return item;
}

function setMetric(host, selector, value, label) {
  const metric = host.querySelector(selector);
  metric.querySelector("strong").textContent = number(value);
  metric.querySelector("span").textContent = label;
}

const dashboard = {
  mount(host, context) {
    let active = true;
    let selectedType = "";
    host.className += " proactive-feedback";
    host.innerHTML = `
      <div class="proactive-feedback-loading" role="status">正在读取主动反馈…</div>
      <div class="proactive-feedback-content" hidden>
        <section class="proactive-feedback-overview" aria-label="主动反馈概览">
          <div class="proactive-feedback-rate">
            <strong>—</strong>
            <span>主动消息被继续</span>
          </div>
          <div class="proactive-feedback-signals">
            <div class="proactive-feedback-quotes"><strong>0</strong><span>明确引用</span></div>
            <div class="proactive-feedback-confidence"><strong>0</strong><span>高可信信号</span></div>
          </div>
        </section>
        <section class="proactive-feedback-feed" aria-labelledby="proactive-feedback-title">
          <header>
            <div><h2 id="proactive-feedback-title">最近回应</h2><span class="proactive-feedback-total"></span></div>
            <div class="proactive-feedback-filter" role="group" aria-label="筛选反馈">
              <button type="button" data-type="" aria-pressed="true">全部</button>
              <button type="button" data-type="topic_follow" aria-pressed="false">继续</button>
              <button type="button" data-type="explicit_quote" aria-pressed="false">引用</button>
            </div>
          </header>
          <div class="proactive-feedback-events"></div>
        </section>
      </div>`;
    const loading = host.querySelector(".proactive-feedback-loading");
    const content = host.querySelector(".proactive-feedback-content");
    const events = host.querySelector(".proactive-feedback-events");
    const total = host.querySelector(".proactive-feedback-total");

    const loadEvents = (feedbackType) => {
      events.classList.remove("error");
      events.classList.add("is-loading");
      return context.request("feedback.events", {
        page: 1,
        page_size: 30,
        feedback_type: feedbackType,
      }).then((page) => {
        if (!active || feedbackType !== selectedType) return;
        const items = Array.isArray(page.items) ? page.items : [];
        events.replaceChildren();
        events.classList.remove("error");
        total.textContent = `${number(page.total)} 条`;
        if (items.length === 0) {
          const empty = document.createElement("p");
          empty.className = "proactive-feedback-empty";
          empty.textContent = feedbackType ? "这个筛选下还没有反馈。" : "还没有可分析的主动反馈。";
          events.append(empty);
        } else {
          events.append(...items.map(eventRow));
        }
        events.classList.remove("is-loading");
      });
    };

    Promise.all([
      context.request("feedback.overview"),
      loadEvents(""),
    ]).then(([overview]) => {
      if (!active) return;
      host.querySelector(".proactive-feedback-rate strong").textContent = rate(overview.follow_rate);
      setMetric(host, ".proactive-feedback-quotes", overview.explicit_quote, "明确引用");
      setMetric(host, ".proactive-feedback-confidence", overview.high_confidence, "高可信信号");
      loading.remove();
      content.hidden = false;
    }).catch((error) => {
      if (!active) return;
      loading.className = "proactive-feedback-loading error";
      loading.textContent = error instanceof Error
        ? `主动反馈读取失败：${error.message}`
        : "主动反馈读取失败";
    });

    for (const button of host.querySelectorAll(".proactive-feedback-filter button")) {
      button.addEventListener("click", () => {
        const nextType = button.dataset.type || "";
        if (nextType === selectedType) return;
        selectedType = nextType;
        for (const peer of host.querySelectorAll(".proactive-feedback-filter button")) {
          peer.setAttribute("aria-pressed", String(peer === button));
        }
        loadEvents(nextType).catch((error) => {
          if (!active || nextType !== selectedType) return;
          events.classList.remove("is-loading");
          events.textContent = error instanceof Error ? `筛选失败：${error.message}` : "筛选失败";
          events.classList.add("error");
        });
      });
    }
    return () => { active = false; };
  },
};

export default {
  slots: {},
  navigation: {
    label: "主动反馈",
    description: "主动消息是否被继续，以及对应的回应链路",
  },
  dashboard,
};
