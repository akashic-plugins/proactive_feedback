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

### Durable history pull

插件提供普通服务 `proactive-feedback.history.v1`。消费者按单调 `cursor` 调用
`page(after_cursor, max_items)`；每条 accepted feedback 的 `event_id` 固定为
`proactive_feedback:<cursor>`，`payload_hash` 是稳定字段 canonical JSON 的 SHA-256。
分页最多 100 条，严格按 SQLite row id 递增。

`proactive_feedback_events` 是 accepted feedback 的唯一 owner。第一次 accepted payload
写入后不可 UPDATE 或 DELETE；相同 Turn 的完全相同 payload 返回原 identity，任何字段
漂移都 fail-loud。评分重试的中间计算不是新的领域事实，不另造 history。input inbox
保留 Turn identity 与处理状态。旧 outbox/cursor schema 和既有行冻结保留，新链不再写
outbox，也不调用 Core Observe event。

history reader 始终使用 SQLite `mode=ro`。数据库不存在表示合法空历史且不会创建目录；
数据库存在但损坏、schema 异构或字段类型无效会 fail-loud，不能伪装成空页。插件不向
`TurnCommitted.extra` 回写结果，也不依赖 Wake、Content 或消费者数据库。

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
ABI；v3 运行路径只观察 Core 的 committed Turn，并通过上述只读 history service 暴露
已持久化反馈。

## 移动端看板

插件通过 Akashic 的通用移动 UI 生命周期注册“主动反馈”入口，不要求 Agent 核心识别
插件业务。看板聚焦主动消息是否被继续、明确引用和高可信信号；事件展开后按“主动发出、
用户回应、助手继续”显示关联链路。桌面 Dashboard 保留完整审计字段，移动端不复制桌面
表格。
