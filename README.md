# proactive_feedback

Akashic proactive feedback plugin.

## v3 接入

插件入口是 module-level `api_version = 3` 与 `apply(ctx, config)`：

- 通过 Core `AFTER_TURN_COMMITTED` 串行接入点观察已提交 Turn；
- 通过 `SESSION_READ` 读取脱离持久化 owner 的 Session 快照；
- 反馈数据库由 Core 分配的 `ctx.data_root` 独占，Dashboard 与 Mobile 只读同一投影；
- `apply` 不读取或写入正式 `sessions.db`，候选期不会访问正式 Session；候选没有
  反馈 DB 时也不会为了重放而创建文件；已提交 Turn 的 inbox 只保存
  session/turn/message identity，不保存 user/assistant 正文。Core 正式 generation
  启动时会在最多 64 个既有 session、最多 256 个 Turn 的边界内用 `SESSION_READ`
  重新发现已提交但尚未进入 inbox 的 eligible Turn；只把 ordered user IDs 和
  assistant ID 写入 inbox，正文只在评分内存中重建。候选 generation 不执行 discovery，
  因而不会写正式 DB 或事件。

### Durable typed event

Core exact `20062a715d2c5822228b327863b51c8d036119b3` 提供唯一的
`agent.turn_events.proactive_feedback.PROACTIVE_FEEDBACK_COMMITTED` Observe seam。
每次评分结果都在一次 SQLite commit 中同时写入 `proactive_feedback_events` 和
`proactive_feedback_outbox`；commit 返回后才调用
`ctx.observe(PROACTIVE_FEEDBACK_COMMITTED, ProactiveFeedbackCommitted(...))`。

事件 `event_id` 固定为 `proactive_feedback:<row_id>`，DTO 使用 Core 的
`session_key`、ordered user message identity（DTO 使用该 Turn 最后一条 user ID）、
assistant/proactive message identity、评分、`reason` 和最多
2400 字符的 user/assistant/proactive preview。全文不进入事件。发布成功后同库的
`proactive_feedback_published_cursor` 与 outbox receipt 一起推进；发布失败、进程内
取消或 Core 重启都会保留 pending 行，正式 generation 启动时按 row 顺序重放。消费方
必须按 `event_id` 幂等。已发布的 projection/outbox receipt 是不可变事实；重复的
同一 identity 即使评分不同，也不会改写已发布 DTO。待发布行才允许在同一 identity
内更新；若关联 proactive identity 改变，则保留旧 published row 并创建新的 row/event。
本插件不再向 `TurnCommitted.extra` 写入反馈，也不提供 marker fallback。

非引用评分使用 Core 正式运行时的共享 HTTP resources。嵌入配置从
`AKASHIC_CONFIG` 指向的 Core 配置加载，不从插件 checkout 的当前目录猜测配置。
embedding 继续使用既有 Core provider 数据流；API key 只作为运行时认证，不进入
inbox、projection 或 typed event，完整正文也不进入这些持久/发布边界。

旧 v2 `scripts/backfill_proactive_feedback.py` 已移除：它直接操作
`workspace/proactive_feedback/proactive_feedback.db`，而 `--clear` 会删除旧 DB、WAL
和 SHM。需要处理旧数据时，先保留可恢复备份，再使用下面的非破坏迁移；迁移会保留
旧源并写入可校验 receipt，不提供旧脚本的清空入口。

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
