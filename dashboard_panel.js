// ../akashic-plugin/proactive_feedback/dashboard_panel.tsx
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
  return `<span class="${window.AkashicDashboard.ui.cx.badge(tone)}">${_escape(type || "-")}</span>`;
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
  return /* @__PURE__ */ jsxs("div", { className: "detail-wrap", children: [
    /* @__PURE__ */ jsx("div", { className: "detail-toolbar", children: /* @__PURE__ */ jsxs("div", { children: [
      /* @__PURE__ */ jsx("div", { className: "detail-title", children: "\u53CD\u9988\u94FE\u8DEF" }),
      /* @__PURE__ */ jsxs("div", { className: "detail-subtext", children: [
        String(item.session_key || ""),
        " \xB7 ",
        String(item.user_message_id || "")
      ] })
    ] }) }),
    /* @__PURE__ */ jsxs("div", { className: "detail-grid", children: [
      /* @__PURE__ */ jsx(DetailRow, { label: "type", value: /* @__PURE__ */ jsx(Chip, { tone: _tone(type), children: type }) }),
      /* @__PURE__ */ jsx(DetailRow, { label: "confidence", value: /* @__PURE__ */ jsx("code", { children: String(item.confidence || "-") }) }),
      /* @__PURE__ */ jsx(DetailRow, { label: "matched_by", value: /* @__PURE__ */ jsx("code", { children: String(item.matched_by || "-") }) }),
      /* @__PURE__ */ jsx(DetailRow, { label: "pua", value: /* @__PURE__ */ jsx("code", { children: _score(item.pua_score) }) }),
      /* @__PURE__ */ jsx(DetailRow, { label: "lag", value: /* @__PURE__ */ jsx("code", { children: _lag(item.lag_seconds) }) }),
      /* @__PURE__ */ jsx(DetailRow, { label: "proactive_id", value: /* @__PURE__ */ jsx("code", { children: String(item.proactive_message_id || "-") }) })
    ] }),
    /* @__PURE__ */ jsx(TextBlock, { title: "User Reply", text: String(item.user_reply_preview || item.user_preview || "") }),
    item.quoted_preview ? /* @__PURE__ */ jsx(TextBlock, { title: "Quoted Proactive", text: String(item.quoted_preview) }) : null,
    /* @__PURE__ */ jsx(TextBlock, { title: "Matched Proactive", text: String(item.proactive_preview || "") }),
    /* @__PURE__ */ jsx(TextBlock, { title: "Assistant Reply", text: String(item.assistant_preview || "") })
  ] });
}
function DetailRow(props) {
  return /* @__PURE__ */ jsxs("div", { className: "detail-row", children: [
    /* @__PURE__ */ jsx("div", { className: "detail-row-label", children: props.label }),
    /* @__PURE__ */ jsx("div", { className: "detail-row-val", children: props.value })
  ] });
}
function TextBlock(props) {
  return /* @__PURE__ */ jsxs("div", { className: "detail-block", children: [
    /* @__PURE__ */ jsx("div", { className: "detail-label", children: props.title }),
    /* @__PURE__ */ jsx("div", { className: "detail-content ak-plugin-pre-wrap", children: props.text || "-" })
  ] });
}
window.AkashicDashboard.registerPlugin({
  id: "proactive_feedback",
  label: "Feedback",
  viewLabel: "feedback",
  pageSize: 50,
  rowKey: "id",
  countTitle(total) {
    return `\u5171 ${total} \u6761\u53CD\u9988`;
  },
  columns: [
    { key: "created_at", label: "Time", width: 96, fmt: "mono-time", cellClass: "mono cell-time", rawTitle: true },
    { key: "feedback_type", label: "Type", width: 126, renderCell: _typeCell },
    { key: "confidence", label: "Conf", width: 72, cellClass: "mono cell-metric" },
    { key: "pua_score", label: "PUA", width: 66, fmt: "score", cellClass: "mono cell-metric", align: "right" },
    { key: "lag_seconds", label: "Lag", width: 68, fmt: "lag", cellClass: "mono cell-metric", align: "right" },
    { key: "user_reply_preview", label: "User Reply", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true },
    { key: "proactive_preview", label: "Matched Proactive", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true }
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
