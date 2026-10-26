# proactive_feedback

Akashic proactive feedback plugin.

## Message runtime 接入

插件入口是 module-level `api_version = 3` 与 `apply(ctx)`：

- 通过 `core.message_catalog` 追赶已提交 Message head，并用 `turn.projection.v1`
  从各来源的闭合 Turn 边界增量识别已完成回复；
- 反馈数据库由 Core 分配的 `ctx.data_root` 独占，Dashboard 只读同一投影；
- `apply` 不读取或写入正式 Message 数据；候选期不会启动 watcher。正式 generation
  启动后按保存的发现续读点追赶列出的 Session，只把 ordered input IDs、ending output ID
  和 Session identity 写入 inbox，正文只在评分内存中从 Message 重建。
- 发现续读点（`proactive_feedback_discovery_cursor`）按 Session 记录已扫描 head 与每个来源的
  闭合边界、未闭合尾段起点。head 前进时只解码新消息，以及有新消息的来源自己的未闭合尾段；
  head 变化时若消息前缀已被改写（Core `prefix_revision` 变化）、head 回退或 Core 不提供改写标记，
  该 Session 从头发现，inbox 写入幂等；只改写旧消息而 head 不变时不触发（与原实现相同）。
  首次升级每个 Session 全量发现一次。同一轮写入 inbox 的次序与全量重扫一致：按各来源首条 Input
  的先后，续读点保存这一次序。
- 候选只需要 Turn 首条输入之前的有界历史：普通追问读到上一个用户输入，引用回复读到最近
  256 条非空助手消息；评分按 inbox 身份读取本 Turn 成员，不再整段解码 Session。
- 历史发现与 inbox 评分独立推进：评分失败不会重扫已发现的 Message 前缀，
  也不阻止后续 head 进入 inbox。每轮发现只建一次候选索引，逐 Turn 让出执行时间，
  避免历史追赶阻塞聊天与运行时心跳。未完成评分每秒重试，原错误仍可见。

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
`TurnCommitted.extra` 回写结果，也不依赖 Wake、Content 或消费者数据库。已知正式旧表
若尚无三个 preview 列，history 会把这三个稳定字段投影为 `null` 后参与 canonical hash；
旧表经既有 `open_db` 逐列 `ALTER` 后形成的 SQLite schema 也属于同一已知 lineage。

非引用评分只注入公共 `EMBEDDINGS` service，不读取 Core 配置、provider 或凭据。
后台评分每次都在当前 generation 的 `runtime_scope` 内执行 `bind → embed`，因此热更新
前后的任务不会混用模型快照。向量和完整正文不进入 inbox、projection 或 history。

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

插件不声明 EventBus listener、`TurnCommitted` 或旧 Session snapshot ABI；运行路径只消费
Message 与无状态 Turn 投影，并通过上述只读 history service 暴露已持久化反馈。

## 界面与后台处理

Web 独立插件面板已随 Core 退役。插件保留 Dashboard 只读路由、反馈历史服务和后台评分；
不再登记无入口的聊天面板。升级不删除反馈库、inbox 或既有记录。

历史消息的读取、解码与 Turn 投影在工作线程执行，避免启动追赶阻塞聊天和心跳。已有 accepted feedback 的身份直接补齐 inbox 回执，不重新评分或改写历史反馈。

后台发现、评分和结算的 SQLite 工作通过 Core 的有界文件执行路径完成。每次连接、完整事务和关闭都在同一物理工作内；连接不跨 await 或线程传递。发现仍逐 Turn 接纳，取消先等当前物理工作结束，后续 Turn 不再开始；已提交 inbox/accepted feedback 按原身份恢复。除发现续读点表外没有新增 schema、队列、反馈改写或数据清理。

同一 Runtime 的数据库准入保持串行；不同 Runtime 共用数据库时，反馈归属核对、原 payload 校验和首次 accepted 写入在同一 SQLite 写事务内完成，不能各自接受第二份 payload。

隔离数据库锁、取消与重开场景可运行 `PYTHONPATH=/path/to/core python scripts/check_store_io.py`；脚本只使用临时 MessageLog 与反馈库，不调用付费模型或正式 workspace。
