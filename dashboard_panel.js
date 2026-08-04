// ../proactive_feedback/dashboard_panel.tsx
import { Chip, api } from "@akashic/dashboard-ui";
import { jsx, jsxs } from "react/jsx-runtime";
function _score(value) {
  return typeof value === "number" ? value.toFixed(3) : "-";
}
function _shortTs(value) {
  const text = String(value || "");
  if (!text) return "-";
  const d = new Date(text);
  if (Number.isNaN(d.getTime())) return text;
  return `${d.getMonth() + 1}-${String(d.getDate()).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}
function _lag(value) {
  if (typeof value !== "number") return "-";
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  return `${(value / 3600).toFixed(1)}h`;
}
function _tone(type) {
  if (type === "explicit_quote") return "accent";
  if (type === "topic_follow") return "success";
  if (type === "unscored") return "warning";
  if (type === "no_topic_follow") return "muted";
  return "muted";
}
function _feedbackTypeLabel(value) {
  const type = String(value || "");
  if (type === "explicit_quote") return "\u660E\u786E\u5F15\u7528";
  if (type === "topic_follow") return "\u8BDD\u9898\u5EF6\u7EED";
  if (type === "no_topic_follow") return "\u672A\u5EF6\u7EED\u8BDD\u9898";
  if (type === "unscored") return "\u5F85\u8BC4\u5206";
  return type || "-";
}
function _confidenceLabel(value) {
  const confidence = String(value || "");
  if (confidence === "gold") return "\u91D1\u6807";
  if (confidence === "high") return "\u9AD8";
  if (confidence === "medium") return "\u4E2D";
  if (confidence === "low") return "\u4F4E";
  return confidence || "-";
}
function _escape(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
function _cellText(value) {
  const text = String(value || "").trim();
  return _escape(text || "-");
}
function _typeCell(value) {
  const type = String(value || "");
  const tone = type === "explicit_quote" ? "accent" : type === "topic_follow" ? "success" : type === "unscored" ? "warning" : "muted";
  return `<span class="${window.AkashicDashboard.ui.cx.badge(tone)}">${_escape(_feedbackTypeLabel(type))}</span>`;
}
function _confidenceTone(value) {
  const confidence = String(value || "");
  if (confidence === "gold" || confidence === "high") return "success";
  if (confidence === "medium") return "warning";
  return "muted";
}
function _confidenceCell(value) {
  const confidence = String(value || "-");
  return `<span class="${window.AkashicDashboard.ui.cx.badge(_confidenceTone(confidence))}">${_escape(_confidenceLabel(confidence))}</span>`;
}
function FeedbackDetail(props) {
  const item = props.item;
  if (!item) {
    return /* @__PURE__ */ jsxs("div", { className: "detail-empty", children: [
      /* @__PURE__ */ jsx("div", { className: "detail-empty-title", children: "\u53CD\u9988\u8BE6\u60C5" }),
      /* @__PURE__ */ jsx("div", { className: "detail-empty-text", children: "\u70B9\u5F00\u4E00\u6761\u8BB0\u5F55\u540E\uFF0C\u8FD9\u91CC\u4F1A\u663E\u793A\u7528\u6237\u56DE\u590D\u3001\u547D\u4E2D\u7684 proactive \u548C\u52A9\u624B\u540E\u7EED\u56DE\u590D\u3002" })
    ] });
  }
  const type = String(item.feedback_type || "");
  return /* @__PURE__ */ jsxs("main", { className: "feedback-detail", "aria-labelledby": "feedback-detail-title", children: [
    /* @__PURE__ */ jsxs("header", { className: "feedback-detail__header", children: [
      /* @__PURE__ */ jsxs("div", { children: [
        /* @__PURE__ */ jsx("p", { children: "\u4E3B\u52A8\u6D88\u606F\u6548\u679C" }),
        /* @__PURE__ */ jsx("h2", { id: "feedback-detail-title", children: "\u7528\u6237\u5982\u4F55\u56DE\u5E94\u8FD9\u6B21\u4E3B\u52A8\u89E6\u8FBE" }),
        /* @__PURE__ */ jsx("span", { children: String(item.session_key || "\u672A\u5173\u8054\u4F1A\u8BDD") })
      ] }),
      /* @__PURE__ */ jsx(Chip, { tone: _tone(type), children: _feedbackTypeLabel(type) })
    ] }),
    /* @__PURE__ */ jsxs("section", { className: "feedback-summary", "aria-label": "\u53CD\u9988\u5224\u65AD\u6458\u8981", children: [
      /* @__PURE__ */ jsx(SummaryMetric, { label: "\u7F6E\u4FE1\u5EA6", value: _confidenceLabel(item.confidence) }),
      /* @__PURE__ */ jsx(SummaryMetric, { label: "\u54CD\u5E94\u5EF6\u8FDF", value: _lag(item.lag_seconds) }),
      /* @__PURE__ */ jsx(SummaryMetric, { label: "PUA \u5206\u6570", value: _score(item.pua_score) })
    ] }),
    /* @__PURE__ */ jsxs("section", { className: "feedback-timeline", "aria-labelledby": "feedback-timeline-title", children: [
      /* @__PURE__ */ jsx("h3", { id: "feedback-timeline-title", children: "\u5BF9\u8BDD\u94FE\u8DEF" }),
      /* @__PURE__ */ jsx(TimelineStep, { index: "1", title: "\u4E3B\u52A8\u6D88\u606F", text: String(item.proactive_preview || item.quoted_preview || "") }),
      /* @__PURE__ */ jsx(TimelineStep, { index: "2", title: "\u7528\u6237\u53CD\u9988", text: String(item.user_reply_preview || item.user_preview || ""), emphasis: true }),
      /* @__PURE__ */ jsx(TimelineStep, { index: "3", title: "\u52A9\u624B\u540E\u7EED", text: String(item.assistant_preview || "") })
    ] }),
    /* @__PURE__ */ jsxs("details", { className: "feedback-technical", children: [
      /* @__PURE__ */ jsx("summary", { children: "\u67E5\u770B\u5339\u914D\u4F9D\u636E\u4E0E\u6D88\u606F\u6807\u8BC6" }),
      /* @__PURE__ */ jsxs("dl", { children: [
        /* @__PURE__ */ jsxs("div", { children: [
          /* @__PURE__ */ jsx("dt", { children: "\u5339\u914D\u65B9\u5F0F" }),
          /* @__PURE__ */ jsx("dd", { children: String(item.matched_by || "-") })
        ] }),
        /* @__PURE__ */ jsxs("div", { children: [
          /* @__PURE__ */ jsx("dt", { children: "\u7528\u6237\u6D88\u606F ID" }),
          /* @__PURE__ */ jsx("dd", { children: String(item.user_message_id || "-") })
        ] }),
        /* @__PURE__ */ jsxs("div", { children: [
          /* @__PURE__ */ jsx("dt", { children: "\u4E3B\u52A8\u6D88\u606F ID" }),
          /* @__PURE__ */ jsx("dd", { children: String(item.proactive_message_id || "-") })
        ] })
      ] })
    ] })
  ] });
}
function SummaryMetric(props) {
  return /* @__PURE__ */ jsxs("div", { children: [
    /* @__PURE__ */ jsx("span", { children: props.label }),
    /* @__PURE__ */ jsx("strong", { children: props.value })
  ] });
}
function TimelineStep(props) {
  return /* @__PURE__ */ jsxs("div", { className: `feedback-step${props.emphasis ? " is-emphasis" : ""}`, children: [
    /* @__PURE__ */ jsx("span", { className: "feedback-step__index", children: props.index }),
    /* @__PURE__ */ jsxs("div", { children: [
      /* @__PURE__ */ jsx("h4", { children: props.title }),
      /* @__PURE__ */ jsx("p", { children: props.text || "\u6CA1\u6709\u53EF\u5C55\u793A\u7684\u5185\u5BB9" })
    ] })
  ] });
}
window.AkashicDashboard.registerPlugin({
  id: "proactive_feedback",
  label: "\u4E3B\u52A8\u53CD\u9988",
  viewLabel: "\u4E3B\u52A8\u53CD\u9988",
  pageSize: 50,
  rowKey: "id",
  countTitle(total) {
    return `\u5171 ${total} \u6761\u53CD\u9988`;
  },
  columns: [
    { key: "created_at", label: "\u65F6\u95F4", width: 96, fmt: "mono-time", cellClass: "mono cell-time", rawTitle: true },
    { key: "feedback_type", label: "\u7C7B\u578B", width: 126, renderCell: _typeCell },
    { key: "confidence", label: "\u7F6E\u4FE1\u5EA6", width: 84, renderCell: _confidenceCell },
    { key: "lag_seconds", label: "\u5EF6\u8FDF", width: 68, fmt: "lag", cellClass: "mono cell-metric", align: "right" },
    { key: "user_reply_preview", label: "\u7528\u6237\u56DE\u590D", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true },
    { key: "proactive_preview", label: "\u547D\u4E2D\u5185\u5BB9", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true }
  ],
  async getCount() {
    try {
      const overview = await api("/api/dashboard/proactive-feedback/overview");
      return overview.total || 0;
    } catch {
      return null;
    }
  },
  async fetchPage({ page, pageSize }) {
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await api(`/api/dashboard/proactive-feedback/events?${params.toString()}`);
    return { items: data.items || [], total: data.total || 0 };
  },
  async fetchDetail(item) {
    return api(`/api/dashboard/proactive-feedback/events/${item.id}`);
  },
  Detail: FeedbackDetail,
  formatters: {
    score: (value) => _score(value),
    lag: (value) => _lag(value),
    "mono-time": (value) => _shortTs(value)
  }
});
