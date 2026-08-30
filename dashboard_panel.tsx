import { type ReactElement } from "react";
import { createRoot } from "react-dom/client";
import "./dashboard_panel.css";
import type { WebHostContextV1, WebUiDisposer } from "@akashic/web-ui-v1";
import type { WorkbenchDispatch, WorkbenchPanelEntry, WorkbenchUi } from "@akashic/workbench-ui-v2";

let dashboardRequest: WebHostContextV1["http"]["request"] | null = null;

async function api<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  if (!dashboardRequest) throw new Error("主动反馈工作台面板未激活");
  const response = await dashboardRequest(path, init);
  const body = await response.json() as T & { detail?: unknown; message?: unknown };
  if (!response.ok) throw new Error(String(body.detail ?? body.message ?? `HTTP ${response.status}`));
  return body;
}

interface Overview {
  total: number;
}

interface FetchPage {
  items: Record<string, unknown>[];
  total: number;
}

function _score(value: unknown): string {
  return typeof value === "number" ? value.toFixed(3) : "-";
}

function _shortTs(value: unknown): string {
  const text = String(value || "");
  if (!text) return "-";
  const d = new Date(text);
  if (Number.isNaN(d.getTime())) return text;
  return `${d.getMonth() + 1}-${String(d.getDate()).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function _lag(value: unknown): string {
  if (typeof value !== "number") return "-";
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  return `${(value / 3600).toFixed(1)}h`;
}

function _tone(type: string): "success" | "warning" | "danger" | "accent" | "muted" {
  if (type === "explicit_quote") return "accent";
  if (type === "topic_follow") return "success";
  if (type === "unscored") return "warning";
  if (type === "no_topic_follow") return "muted";
  return "muted";
}

function _feedbackTypeLabel(value: unknown): string {
  const type = String(value || "");
  if (type === "explicit_quote") return "明确引用";
  if (type === "topic_follow") return "话题延续";
  if (type === "no_topic_follow") return "未延续话题";
  if (type === "unscored") return "待评分";
  return type || "-";
}

function _confidenceLabel(value: unknown): string {
  const confidence = String(value || "");
  if (confidence === "gold") return "金标";
  if (confidence === "high") return "高";
  if (confidence === "medium") return "中";
  if (confidence === "low") return "低";
  return confidence || "-";
}

function _escape(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function _cellText(value: unknown): string {
  const text = String(value || "").trim();
  return _escape(text || "-");
}

function _typeCell(value: unknown): string {
  const type = String(value || "");
  const tone = type === "explicit_quote" ? "accent" : type === "topic_follow" ? "success" : type === "unscored" ? "warning" : "muted";
  return `<span class="ak-chip ak-chip--${tone} inline-flex items-center gap-1.5 px-2.5 py-1 font-sans text-[11px] tabular-nums">${_escape(_feedbackTypeLabel(type))}</span>`;
}

function _confidenceTone(value: unknown): "success" | "warning" | "muted" {
  const confidence = String(value || "");
  if (confidence === "gold" || confidence === "high") return "success";
  if (confidence === "medium") return "warning";
  return "muted";
}

function _confidenceCell(value: unknown): string {
  const confidence = String(value || "-");
  const tone = _confidenceTone(confidence);
  return `<span class="ak-chip ak-chip--${tone} inline-flex items-center gap-1.5 px-2.5 py-1 font-sans text-[11px] tabular-nums">${_escape(_confidenceLabel(confidence))}</span>`;
}

function FeedbackDetail(props: { item: Record<string, unknown> | null; ui: WorkbenchUi }): ReactElement {
  const item = props.item;
  const Chip = props.ui.Chip;
  if (!item) {
    return <div className="feedback-empty"><div className="feedback-empty__title">反馈详情</div><div className="feedback-empty__text">点开一条记录后，这里会显示用户回复、命中的 proactive 和助手后续回复。</div></div>;
  }
  const type = String(item.feedback_type || "");
  return (
    <main className="feedback-detail" aria-labelledby="feedback-detail-title">
      <header className="feedback-detail__header">
        <div>
          <p>主动消息效果</p>
          <h2 id="feedback-detail-title">用户如何回应这次主动触达</h2>
          <span>{String(item.session_key || "未关联会话")}</span>
        </div>
        <Chip tone={_tone(type)}>{_feedbackTypeLabel(type)}</Chip>
      </header>

      <section className="feedback-summary" aria-label="反馈判断摘要">
        <SummaryMetric label="置信度" value={_confidenceLabel(item.confidence)} />
        <SummaryMetric label="响应延迟" value={_lag(item.lag_seconds)} />
        <SummaryMetric label="PUA 分数" value={_score(item.pua_score)} />
      </section>

      <section className="feedback-timeline" aria-labelledby="feedback-timeline-title">
        <h3 id="feedback-timeline-title">对话链路</h3>
        <TimelineStep index="1" title="主动消息" text={String(item.proactive_preview || item.quoted_preview || "")} />
        <TimelineStep index="2" title="用户反馈" text={String(item.user_reply_preview || item.user_preview || "")} emphasis />
        <TimelineStep index="3" title="助手后续" text={String(item.assistant_preview || "")} />
      </section>

      <details className="feedback-technical">
        <summary>查看匹配依据与消息标识</summary>
        <dl>
          <div><dt>匹配方式</dt><dd>{String(item.matched_by || "-")}</dd></div>
          <div><dt>用户消息 ID</dt><dd>{String(item.user_message_id || "-")}</dd></div>
          <div><dt>主动消息 ID</dt><dd>{String(item.proactive_message_id || "-")}</dd></div>
        </dl>
      </details>
    </main>
  );
}

function SummaryMetric(props: { label: string; value: string }): ReactElement {
  return <div><span>{props.label}</span><strong>{props.value}</strong></div>;
}

function TimelineStep(props: { index: string; title: string; text: string; emphasis?: boolean }): ReactElement {
  return (
    <div className={`feedback-step${props.emphasis ? " is-emphasis" : ""}`}>
      <span className="feedback-step__index">{props.index}</span>
      <div>
        <h4>{props.title}</h4>
        <p>{props.text || "没有可展示的内容"}</p>
      </div>
    </div>
  );
}

const panel = {
  id: "proactive-feedback",
  label: "主动反馈",
  viewLabel: "主动反馈",
  order: 70,
  pageSize: 50,
  rowKey: "id",

  countTitle(total: number): string {
    return `共 ${total} 条反馈`;
  },

  columns: [
    { key: "created_at", label: "时间", width: 96, fmt: "mono-time", cellClass: "mono cell-time", rawTitle: true },
    { key: "feedback_type", label: "类型", width: 126, renderCell: _typeCell },
    { key: "confidence", label: "置信度", width: 84, renderCell: _confidenceCell },
    { key: "lag_seconds", label: "延迟", width: 68, fmt: "lag", cellClass: "mono cell-metric", align: "right" },
    { key: "user_reply_preview", label: "用户回复", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true },
    { key: "proactive_preview", label: "命中内容", flex: true, renderCell: _cellText, cellClass: "content-preview", rawTitle: true },
  ],

  async getCount({ signal }: { signal: AbortSignal }): Promise<number | null> {
    try {
      const overview = await api<Overview>("/api/dashboard/proactive-feedback/overview", { signal });
      return overview.total || 0;
    } catch (error) {
      if (signal.aborted) throw error;
      return null;
    }
  },

  async fetchPage({ page, pageSize, signal }: { page: number; pageSize: number; signal: AbortSignal }) {
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await api<FetchPage>(`/api/dashboard/proactive-feedback/events?${params.toString()}`, { signal });
    return { items: data.items || [], total: data.total || 0 };
  },

  async fetchDetail(item: Record<string, unknown>, { signal }: { signal: AbortSignal }) {
    return api<Record<string, unknown>>(`/api/dashboard/proactive-feedback/events/${item.id}`, { signal });
  },

  renderDetail(item: Record<string, unknown> | null, container: HTMLElement, dispatch: WorkbenchDispatch): WebUiDisposer {
    const root = createRoot(container);
    root.render(<FeedbackDetail item={item} ui={dispatch.ui} />);
    return () => root.unmount();
  },

  formatters: {
    score: (value: unknown) => _score(value),
    lag: (value: unknown) => _lag(value),
    "mono-time": (value: unknown) => _shortTs(value),
  },
} satisfies WorkbenchPanelEntry;

export function activate(ctx: WebHostContextV1): WebUiDisposer {
  dashboardRequest = ctx.http.request;
  const release = ctx.ui.inject("workbench.panels.v2", (mount) => mount.register(panel));
  return () => {
    release();
    dashboardRequest = null;
  };
}
