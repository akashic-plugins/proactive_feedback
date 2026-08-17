# proactive_feedback

Akashic proactive feedback plugin.

## v3 接入

插件入口是 module-level `api_version = 3` 与 `apply(ctx, config)`：

- 通过 Core `AFTER_TURN_COMMITTED` 串行接入点观察已提交 Turn；
- 通过 `SESSION_READ` 读取脱离持久化 owner 的 Session 快照；
- 反馈数据库由 Core 分配的 `ctx.data_root` 独占，Dashboard 与 Mobile 只读同一投影；
- `apply` 不读取或写入正式 `sessions.db`，候选期不会访问正式 Session；候选没有
  反馈 DB 时也不会为了重放而创建文件。

### Durable typed event

Core exact `20062a715d2c5822228b327863b51c8d036119b3` 提供唯一的
`agent.turn_events.proactive_feedback.PROACTIVE_FEEDBACK_COMMITTED` Observe seam。
每次评分结果都在一次 SQLite commit 中同时写入 `proactive_feedback_events` 和
`proactive_feedback_outbox`；commit 返回后才调用
`ctx.observe(PROACTIVE_FEEDBACK_COMMITTED, ProactiveFeedbackCommitted(...))`。

事件 `event_id` 固定为 `proactive_feedback:<row_id>`，DTO 使用 Core 的
`session_key`、user/assistant/proactive message identity、评分、`reason` 和最多
2400 字符的 user/assistant/proactive preview。全文不进入事件。发布成功后同库的
`proactive_feedback_published_cursor` 与 outbox receipt 一起推进；发布失败、进程内
取消或 Core 重启都会保留 pending 行，正式 generation 启动时按 row 顺序重放。消费方
必须按 `event_id` 幂等；本插件不再向 `TurnCommitted.extra` 写入反馈，也不提供
marker fallback。

非引用评分使用 Core 正式运行时的共享 HTTP resources。嵌入配置从
`AKASHIC_CONFIG` 指向的 Core 配置加载，不从插件 checkout 的当前目录猜测配置。

插件加载不会自动移动旧数据库。首次从 v2 切换时，先停用旧 runtime，再显式执行
SQLite 一致性迁移；旧源始终保留：

```bash
python scripts/migrate_v2_data.py \
  --workspace <workspace> \
  --marketplace github
```

迁移完成后，可用下面的独立命令补齐历史事件的文本预览；它只更新插件自己的可选投影列，不删除消息：

```bash
python scripts/migrate_feedback_previews.py \
  --sessions-db <workspace>/sessions.db \
  --feedback-db <workspace>/plugin-data/<proactive-feedback-data-root>/proactive_feedback.db
```

插件不再声明 v2 `Plugin` class、EventBus listener、`ProactiveFeedbackRecorded` 或 tool
ABI；v3 运行路径只观察 Core 的 committed Turn，并通过上述 typed event 发布已持久化
反馈。

## 移动端看板

插件通过 Akashic 的通用移动 UI 生命周期注册“主动反馈”入口，不要求 Agent 核心识别
插件业务。看板聚焦主动消息是否被继续、明确引用和高可信信号；事件展开后按“主动发出、
用户回应、助手继续”显示关联链路。桌面 Dashboard 保留完整审计字段，移动端不复制桌面
表格。
