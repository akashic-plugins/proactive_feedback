export function activate(ctx) {
  return ctx.ui.inject("workbench.panels.v1", (mount) => mount.register({
    id: "proactive-feedback",
    label: "主动反馈",
    order: 50,
    render(host) {
      const panel = document.createElement("section");
      panel.className = "proactive-feedback-workbench-panel";
      panel.innerHTML = `<header><div><h1>主动反馈</h1><p>查看主动消息是否被继续，以及对应的回应链路。</p></div><button type="button" data-refresh>刷新</button></header><section class="proactive-feedback-overview" data-overview aria-label="反馈总览"><p>正在读取反馈总览…</p></section><div class="proactive-feedback-toolbar"><label>反馈类型<select data-filter><option value="">全部</option><option value="topic_follow">话题延续</option><option value="explicit_quote">明确引用</option><option value="no_topic_follow">未延续话题</option><option value="unscored">待评分</option></select></label><p data-status role="status" aria-live="polite"></p></div><div class="proactive-feedback-panel-grid"><div><div data-list></div><footer><button type="button" data-previous>上一页</button><span data-page></span><button type="button" data-next>下一页</button></footer></div><article data-detail><p>选择一条反馈查看完整回应链路。</p></article></div>`;
      host.replaceChildren(panel);
      const overview = panel.querySelector("[data-overview]");
      const refresh = panel.querySelector("[data-refresh]");
      const filter = panel.querySelector("[data-filter]");
      const status = panel.querySelector("[data-status]");
      const list = panel.querySelector("[data-list]");
      const detail = panel.querySelector("[data-detail]");
      const pageText = panel.querySelector("[data-page]");
      const previous = panel.querySelector("[data-previous]");
      const next = panel.querySelector("[data-next]");
      let page = 1;
      let total = 0;
      let disposed = false;
      let overviewRequest = new AbortController();
      let listRequest = new AbortController();
      let detailRequest = new AbortController();

      const loadOverview = async () => {
        overviewRequest.abort();
        overviewRequest = new AbortController();
        const request = overviewRequest;
        overview.textContent = "正在读取反馈总览…";
        try {
          const data = await json(ctx, "/api/dashboard/proactive-feedback/overview", request.signal);
          if (disposed || request.signal.aborted) return;
          overview.innerHTML = renderOverview(data);
        } catch (reason) {
          if (!disposed && !request.signal.aborted) showError(overview, reason);
        }
      };

      const loadList = async () => {
        listRequest.abort();
        listRequest = new AbortController();
        const request = listRequest;
        const requestedPage = page;
        const params = new URLSearchParams({page: String(requestedPage), page_size: "25"});
        if (filter.value) params.set("feedback_type", filter.value);
        status.textContent = "正在读取反馈记录…";
        try {
          const data = await json(ctx, `/api/dashboard/proactive-feedback/events?${params}`, request.signal);
          if (disposed || request.signal.aborted) return;
          total = finiteNumber(data.total);
          renderRows(list, data.items, openDetail);
          const pages = Math.max(1, Math.ceil(total / 25));
          pageText.textContent = `${requestedPage} / ${pages}`;
          previous.disabled = requestedPage <= 1;
          next.disabled = requestedPage >= pages;
          status.textContent = total ? `共 ${total} 条反馈` : "没有符合条件的反馈。";
        } catch (reason) {
          if (!disposed && !request.signal.aborted) showError(status, reason);
        }
      };

      const openDetail = async (eventId) => {
        detailRequest.abort();
        detailRequest = new AbortController();
        const request = detailRequest;
        detail.innerHTML = "<p>正在读取详情…</p>";
        try {
          const item = await json(ctx, `/api/dashboard/proactive-feedback/events/${encodeURIComponent(eventId)}`, request.signal);
          if (disposed || request.signal.aborted) return;
          detail.innerHTML = renderDetail(item);
        } catch (reason) {
          if (!disposed && !request.signal.aborted) showError(detail, reason);
        }
      };

      refresh.addEventListener("click", () => {
        void loadOverview();
        void loadList();
      });
      filter.addEventListener("change", () => {
        page = 1;
        void loadList();
      });
      previous.addEventListener("click", () => {
        if (page > 1) {
          page -= 1;
          void loadList();
        }
      });
      next.addEventListener("click", () => {
        if (page * 25 < total) {
          page += 1;
          void loadList();
        }
      });
      void loadOverview();
      void loadList();
      return () => {
        disposed = true;
        overviewRequest.abort();
        listRequest.abort();
        detailRequest.abort();
        host.replaceChildren();
      };
    },
  }));
}

function renderOverview(data) {
  return `<div><span>总反馈</span><strong>${finiteNumber(data && data.total)}</strong></div><div><span>话题延续率</span><strong>${percent(data && data.follow_rate)}</strong></div><div><span>明确引用</span><strong>${finiteNumber(data && data.explicit_quote)}</strong></div><div><span>高置信度</span><strong>${finiteNumber(data && data.high_confidence)}</strong></div>`;
}

function renderRows(target, items, openDetail) {
  target.replaceChildren();
  if (!Array.isArray(items) || !items.length) {
    target.innerHTML = "<p>没有可展示的反馈记录。</p>";
    return;
  }
  for (const item of items) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "proactive-feedback-panel-row";
    button.innerHTML = `<strong>${escapeHtml(feedbackLabel(item.feedback_type))}</strong><span>${escapeHtml(shortTime(item.created_at))} · ${escapeHtml(confidenceLabel(item.confidence))} · ${escapeHtml(lagText(item.lag_seconds))}</span><small>${escapeHtml(String(item.user_reply_preview || item.user_preview || "没有可展示的用户反馈"))}</small>`;
    button.addEventListener("click", () => void openDetail(item.id));
    target.append(button);
  }
}

function renderDetail(item) {
  return `<header><div><p>主动消息效果</p><h2>用户如何回应这次主动触达</h2><span>${escapeHtml(String(item.session_key || "未关联会话"))}</span></div><strong>${escapeHtml(feedbackLabel(item.feedback_type))}</strong></header><dl class="proactive-feedback-detail-metrics"><div><dt>置信度</dt><dd>${escapeHtml(confidenceLabel(item.confidence))}</dd></div><div><dt>响应延迟</dt><dd>${escapeHtml(lagText(item.lag_seconds))}</dd></div><div><dt>PUA 分数</dt><dd>${escapeHtml(score(item.pua_score))}</dd></div></dl><section class="proactive-feedback-timeline"><h3>对话链路</h3>${timelineStep("1", "主动消息", item.proactive_preview || item.quoted_preview)}${timelineStep("2", "用户反馈", item.user_reply_preview || item.user_preview, true)}${timelineStep("3", "助手后续", item.assistant_preview)}</section><details><summary>查看匹配依据与消息标识</summary><dl class="proactive-feedback-technical"><div><dt>匹配方式</dt><dd>${escapeHtml(String(item.matched_by || "-"))}</dd></div><div><dt>用户消息 ID</dt><dd>${escapeHtml(String(item.user_message_id || "-"))}</dd></div><div><dt>主动消息 ID</dt><dd>${escapeHtml(String(item.proactive_message_id || "-"))}</dd></div></dl></details>`;
}

function timelineStep(index, title, text, emphasis = false) {
  return `<div class="proactive-feedback-step${emphasis ? " is-emphasis" : ""}"><span>${index}</span><div><h4>${title}</h4><p>${escapeHtml(String(text || "没有可展示的内容"))}</p></div></div>`;
}

async function json(ctx, path, signal) {
  const response = await ctx.http.request(path, {method: "GET", signal});
  const body = await response.json();
  if (!response.ok) throw new Error(body?.detail || body?.message || `HTTP ${response.status}`);
  return body;
}

function feedbackLabel(value) {
  return ({explicit_quote: "明确引用", topic_follow: "话题延续", no_topic_follow: "未延续话题", unscored: "待评分"})[value] || String(value || "-");
}

function confidenceLabel(value) {
  return ({gold: "金标", high: "高", medium: "中", low: "低"})[value] || String(value || "-");
}

function score(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(3) : "-";
}

function lagText(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  if (number < 60) return `${number}s`;
  if (number < 3600) return `${Math.round(number / 60)}m`;
  return `${(number / 3600).toFixed(1)}h`;
}

function percent(value) {
  const number = Number(value);
  return Number.isFinite(number) ? `${Math.round(number * 100)}%` : "-";
}

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function shortTime(value) {
  const date = new Date(String(value || ""));
  return Number.isNaN(date.getTime()) ? "-" : new Intl.DateTimeFormat("zh-CN", {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false}).format(date);
}

function showError(target, reason) {
  target.setAttribute("role", "alert");
  target.textContent = reason instanceof Error ? reason.message : String(reason);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"})[character]);
}
