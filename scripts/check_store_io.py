"""用临时真实 SQLite 核对反馈锁等待、取消、连接关闭与耐久恢复。"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import sqlite3
import sys
import threading
import time
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any

from plugins.content.plugin import check_text
from plugins.turn_projection.plugin import TurnProjection
from session.log import MessageCatalog, MessageLog
from session.message import ContentPart, ContentReferences, Input, Output


def load_source(source: Path):
    """加载明确选中的插件源码；缺少真实依赖时直接报错。"""
    package = ModuleType("feedback_io_source")
    package.__path__ = [str(source)]
    sys.modules[package.__name__] = package
    return importlib.import_module(package.__name__ + ".plugin")


def seed(log: MessageLog, count: int = 1) -> None:
    """保存真实的主动输出、明确引用输入与最终回复。"""
    def check_ref(_part: ContentPart) -> ContentReferences:
        return ContentReferences()

    proactive = log.writer("s", author="wake", source="wake", body_types=(Output,),
                           content={"text": check_text})
    users = log.writer("s", author="user", source="conversation", body_types=(Input,),
                       content={"text": check_text, "reply_ref": check_ref})
    replies = log.writer("s", author="assistant", source="conversation", body_types=(Output,),
                         content={"text": check_text})
    for index in range(count):
        proactive.append(f"p-{index}", Output((ContentPart("text", "主动提醒一个明确主题"),), "complete"))
        users.append(f"u-{index}", Input((ContentPart("text", "继续这个明确主题"),
                                         ContentPart("reply_ref", f"p-{index}"))))
        replies.append(f"a-{index}", Output((ContentPart("text", "继续回答这个主题"),), "complete"))


def runtime(plugin, log: MessageLog, path: Path):
    """评分夹具只允许明确引用路径；不得调用模型或嵌入服务。"""
    async def reject_embeddings(_texts):
        raise AssertionError("explicit quote scenario must not request embeddings")

    return plugin.ProactiveFeedbackRuntime(
        catalog=MessageCatalog(log), projection=TurnProjection(),
        embed_batch=reject_embeddings, db_path=path,
    )


def message_digest(path: Path) -> str:
    """按原始 SQLite 行核对 Message 事实不变。"""
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        rows = connection.execute("SELECT * FROM messages ORDER BY rowid").fetchall()
    return hashlib.sha256(json.dumps(rows).encode()).hexdigest()


def leaf_errors(error: BaseException) -> list[BaseException]:
    """保留取消与真实 SQLite 错误，不把异常组当成功。"""
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in leaf_errors(child)]
    return [error]


async def scenario(plugin, kind: str, expect_blocking: bool = False) -> dict[str, Any]:
    """用真实写锁或实际提交后的屏障证明物理 owner 的寿命。"""
    with TemporaryDirectory(prefix="feedback-store-io-") as directory:
        root = Path(directory)
        message_path, store_path = root / "messages.db", root / "feedback.db"
        with closing(MessageLog(message_path)) as log:
            seed(log)
            owner = runtime(plugin, log, store_path)
            with closing(plugin.open_db(store_path)):
                pass
            if kind != "discovery_lock":
                await owner._discover_changed(dict(owner._catalog.snapshot_heads()))
            before = message_digest(message_path)
            loop = asyncio.get_running_loop()
            loop_thread = threading.get_ident()
            entered, checkpoint, release = threading.Event(), threading.Event(), threading.Event()
            writer_ready = threading.Event()
            receipts: dict[str, Any] = {"kind": kind, "connections": []}
            caller: asyncio.Task[Any] | None = None
            observer: asyncio.Task[None] | None = None
            connect = sqlite3.connect

            class TrackedConnection(sqlite3.Connection):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    self.record = {"opened_thread": threading.get_ident(), "closed": False}
                    self.accepted = False
                    receipts["connections"].append(self.record)

                def executescript(self, sql: str, /):
                    if kind == "discovery_lock" and not entered.is_set():
                        receipts["entered_at"] = time.monotonic()
                        entered.set()
                    return super().executescript(sql)

                def execute(self, sql: str, parameters=(), /):
                    if "INSERT INTO PROACTIVE_FEEDBACK_EVENTS" in " ".join(sql.upper().split()):
                        self.accepted = True
                        if kind == "write_error_cancel":
                            receipts["entered_at"] = time.monotonic()
                            entered.set()
                            if not release.wait(10):
                                raise TimeoutError("SQLite error barrier was not released")
                            super().execute("PRAGMA query_only=ON")
                    return super().execute(sql, parameters)

                def commit(self):
                    super().commit()
                    if self.accepted and kind in {"accepted_cancel", "queued_cancel"}:
                        self.accepted = False
                        receipts["entered_at"] = time.monotonic()
                        entered.set()
                        if not release.wait(10):
                            raise TimeoutError("actual commit barrier was not released")

                def close(self):
                    super().close()
                    self.record.update(closed=True, closed_thread=threading.get_ident())

            def tracked_connect(database, *args, **kwargs):
                if str(store_path) in str(database):
                    kwargs["factory"] = TrackedConnection
                return connect(database, *args, **kwargs)

            async def observe() -> None:
                nonlocal caller
                try:
                    assert caller is not None
                    receipts["checkpoint_at"] = time.monotonic()
                    receipts["caller_pending"] = not caller.done()
                    count = len(receipts["connections"])
                    if kind == "queued_cancel":
                        queued = asyncio.create_task(owner._complete(1))
                        await asyncio.sleep(0)
                        queued.cancel()
                        try:
                            await queued
                        except asyncio.CancelledError:
                            receipts["queued_cancelled"] = True
                        else:
                            raise AssertionError("cancelled queued receipt executed")
                        assert len(receipts["connections"]) == count
                    elif kind in {"accepted_cancel", "write_error_cancel"}:
                        caller.cancel()
                        await asyncio.sleep(0)
                        caller.cancel()
                        await asyncio.sleep(0)
                        assert not caller.done()
                        receipts["cancel_draining"] = True
                    checkpoint.set()
                except BaseException as error:
                    receipts["observer_failure"] = repr(error)
                    checkpoint.set()

            def schedule_observer() -> None:
                nonlocal observer
                observer = asyncio.create_task(observe())

            def controller() -> None:
                try:
                    with closing(connect(store_path)) as writer:
                        if kind == "discovery_lock":
                            writer.execute("BEGIN IMMEDIATE")
                        writer_ready.set()
                        if not entered.wait(10):
                            raise TimeoutError("operation never reached real SQLite")
                        if release.is_set():
                            return
                        loop.call_soon_threadsafe(schedule_observer)
                        receipts["checkpoint_before_release"] = checkpoint.wait(1)
                        receipts["released_at"] = time.monotonic()
                        writer.rollback()
                        release.set()
                except BaseException as error:
                    receipts["controller_failure"] = repr(error)
                    release.set()

            # 1. 控制连接在独立线程持锁；被测连接仍执行原始 SQL/commit/close。
            thread = threading.Thread(target=controller, daemon=True)
            sqlite3.connect = tracked_connect
            try:
                thread.start()
                assert await asyncio.to_thread(writer_ready.wait, 10)
                if kind == "discovery_lock":
                    operation = owner._discover_changed(dict(owner._catalog.snapshot_heads()))
                else:
                    with closing(connect(store_path)) as connection:
                        connection.row_factory = sqlite3.Row
                        record = plugin.pending_feedback_inputs(connection)[0]
                    by_id = {row.id: row for row in plugin.message_rows_from_messages(log.reader("s").snapshot())}
                    operation = owner._persist_feedback(
                        record, by_id["u-0"], by_id["a-0"], by_id["p-0"],
                        feedback_type="explicit_quote", confidence="high", pa_score=None,
                        pua_score=None, lag_seconds=0, candidate_count=1,
                        matched_by="reply_to_id", reason="controlled_commit",
                    )
                caller = asyncio.create_task(operation)
                try:
                    await asyncio.wait_for(asyncio.shield(caller), 15)
                except BaseException as error:
                    errors = leaf_errors(error)
                    if kind == "accepted_cancel":
                        assert len(errors) == 1 and isinstance(errors[0], asyncio.CancelledError)
                    elif kind == "write_error_cancel":
                        assert any(isinstance(x, asyncio.CancelledError) for x in errors)
                        assert any(isinstance(x, sqlite3.OperationalError) and "readonly" in str(x) for x in errors)
                    else:
                        raise
                    receipts["errors"] = [type(x).__name__ for x in errors]
                if observer is not None:
                    await observer
                thread.join(2)
                assert not thread.is_alive()
                assert "controller_failure" not in receipts and "observer_failure" not in receipts
                assert receipts["checkpoint_before_release"] is not expect_blocking
                assert all(x["closed"] and x["opened_thread"] == x["closed_thread"]
                           for x in receipts["connections"])
                if not expect_blocking:
                    assert all(x["opened_thread"] != loop_thread for x in receipts["connections"])
                    assert receipts["caller_pending"] is True
                receipts["checkpoint_lag_seconds"] = receipts["checkpoint_at"] - receipts["entered_at"]
            finally:
                release.set()
                entered.set()
                sqlite3.connect = connect
                thread.join(2)
                if caller is not None and not caller.done():
                    await asyncio.gather(caller, return_exceptions=True)

            # 2. 重开读取的是实际耐久行；accepted 后取消只补 inbox，不再评分。
            with closing(connect(store_path)) as connection:
                accepted_before = connection.execute("SELECT * FROM proactive_feedback_events").fetchall()
                assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
                assert connection.execute("SELECT count(*) FROM proactive_feedback_input_inbox").fetchone() == (1,)
                receipts["accepted_count"] = len(accepted_before)
                pending = connection.execute("SELECT processed_at FROM proactive_feedback_input_inbox").fetchone()
                assert pending == (None,)
            if kind in {"accepted_cancel", "queued_cancel"}:
                assert len(accepted_before) == 1
                async def reject_rescore(**_kwargs):
                    raise AssertionError("durable accepted fact was scored again")
                score = plugin.score_followup
                plugin.score_followup = reject_rescore
                try:
                    reopened = runtime(plugin, log, store_path)
                    assert await reopened._process_pending_inputs() is False
                finally:
                    plugin.score_followup = score
                with closing(connect(store_path)) as connection:
                    assert connection.execute("SELECT * FROM proactive_feedback_events").fetchall() == accepted_before
                    assert connection.execute("SELECT processed_at FROM proactive_feedback_input_inbox").fetchone()[0] is not None
                receipts["reopen_without_rescore"] = True
            else:
                assert not accepted_before
            assert message_digest(message_path) == before
            receipts["messages_unchanged"] = True
            return receipts


async def throughput(plugin) -> dict[str, Any]:
    """测量实际逐 Turn 接纳成本，不能把连接重建的开销藏起来。"""
    with TemporaryDirectory(prefix="feedback-discovery-cost-") as directory:
        root = Path(directory)
        with closing(MessageLog(root / "messages.db")) as log:
            seed(log, 256)
            owner = runtime(plugin, log, root / "feedback.db")
            start = time.monotonic()
            await owner._discover_changed(dict(owner._catalog.snapshot_heads()))
            elapsed = time.monotonic() - start
            with closing(sqlite3.connect(root / "feedback.db")) as connection:
                count = connection.execute("SELECT count(*) FROM proactive_feedback_input_inbox").fetchone()[0]
                assert count == 256
                assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            return {"turns": count, "message_rows": 768, "elapsed_seconds": elapsed,
                    "scope": "temporary local discovery; no scoring/provider/production throughput claim"}


async def check_initialize_error(plugin) -> dict[str, Any]:
    """真实初始化 SQL 失败后，检查所有实际连接已经关闭。"""
    with TemporaryDirectory(prefix="feedback-init-error-") as directory:
        path = Path(directory) / "invalid.db"
        connect = sqlite3.connect
        with closing(connect(path)) as connection:
            connection.execute("PRAGMA user_version=1")
            connection.execute("CREATE TABLE proactive_feedback_published_cursor(wrong_column TEXT)")
        connections: list[sqlite3.Connection] = []

        def track(database, *args, **kwargs):
            connection = connect(database, *args, **kwargs)
            connections.append(connection)
            return connection

        def check() -> dict[str, Any]:
            sqlite3.connect = track
            try:
                try:
                    plugin.open_db(path)
                except sqlite3.OperationalError as error:
                    assert "name" in str(error)
                else:
                    raise AssertionError("invalid schema initialization succeeded")
                assert len(connections) == 2
                for connection in connections:
                    try:
                        connection.execute("SELECT 1")
                    except sqlite3.ProgrammingError as error:
                        assert "closed" in str(error)
                    else:
                        raise AssertionError("failed initialization left an open connection")
            finally:
                sqlite3.connect = connect
                for connection in connections:
                    connection.close()
            return {"initialization_error": "OperationalError", "actual_closed_connections": len(connections)}

        return await asyncio.to_thread(check)

async def check_two_owners(plugin, expect_two: bool = False) -> dict[str, Any]:
    """不同 Runtime 共用真实数据库时，用户身份只能接受第一份 payload。"""
    with TemporaryDirectory(prefix="feedback-two-owner-") as directory:
        root = Path(directory)
        with closing(MessageLog(root / "messages.db")) as log:
            # 1. 两份合法先前主动消息允许不同评分决定，但不允许两份 accepted 事实。
            proactive = log.writer("s", author="wake", source="wake", body_types=(Output,),
                                   content={"text": check_text})
            for identity in ("p-0", "p-alt"):
                proactive.append(identity, Output((ContentPart("text", "主动提醒一个明确主题"),), "complete"))
            users = log.writer("s", author="user", source="conversation", body_types=(Input,),
                               content={"text": check_text})
            users.append("u-0", Input((ContentPart("text", "继续这个主题"),)))
            replies = log.writer("s", author="assistant", source="conversation", body_types=(Output,),
                                 content={"text": check_text})
            replies.append("a-0", Output((ContentPart("text", "继续回答这个主题"),), "complete"))
            path = root / "feedback.db"
            first, second = (runtime(plugin, log, path) for _ in range(2))
            await first._discover_changed(dict(first._catalog.snapshot_heads()))
            with closing(plugin.open_db(path)) as connection:
                record = plugin.pending_feedback_inputs(connection)[0]
            rows = {row.id: row for row in plugin.message_rows_from_messages(log.reader("s").snapshot())}
            module: Any = sys.modules[plugin.__package__ + ".db"]
            original = module._existing_feedback
            entered, release, second_done = threading.Event(), threading.Event(), threading.Event()
            second_start = asyncio.Event()
            mutex = threading.Lock()
            checks = 0

            def paused(connection, event):
                nonlocal checks
                result = original(connection, event)
                with mutex:
                    checks += 1
                    own = checks == 1
                if own:
                    assert result is None
                    entered.set()
                    if not release.wait(10):
                        raise TimeoutError("first owner query was not released")
                return result

            loop = asyncio.get_running_loop()
            receipt: dict[str, Any] = {}

            def controller() -> None:
                if not entered.wait(10):
                    receipt["controller_failure"] = "first query did not run"
                    loop.call_soon_threadsafe(second_start.set)
                    release.set()
                    return
                loop.call_soon_threadsafe(second_start.set)
                receipt["second_completed_before_release"] = second_done.wait(1)
                release.set()

            def persist(owner, identity):
                return owner._persist_feedback(
                    record, rows["u-0"], rows["a-0"], rows[identity],
                    feedback_type="strong_continuation", confidence="high", pa_score=1.0,
                    pua_score=None, lag_seconds=0, candidate_count=2,
                    matched_by="semantic", reason="controlled_owner_race",
                )

            # 2. 首次查询暂停后允许第二个真实 worker 提交；SQLite 自身决定归属。
            thread = threading.Thread(target=controller, daemon=True)
            module._existing_feedback = paused
            tasks: list[asyncio.Task[Any]] = []
            try:
                thread.start()
                tasks.append(asyncio.create_task(persist(first, "p-0")))
                await asyncio.wait_for(second_start.wait(), 15)
                tasks.append(asyncio.create_task(persist(second, "p-alt")))
                tasks[1].add_done_callback(lambda _task: second_done.set())
                outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
                with closing(sqlite3.connect(path)) as connection:
                    count = connection.execute(
                        "SELECT count(*) FROM proactive_feedback_events WHERE user_message_id=?", ("u-0",),
                    ).fetchone()[0]
                    assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
                receipt.update(accepted_facts=count, outcomes=[type(value).__name__ for value in outcomes])
                assert "controller_failure" not in receipt
                if expect_two:
                    assert count == 2 and all(value is None for value in outcomes)
                else:
                    assert count == 1 and outcomes[0] is None
                    assert isinstance(outcomes[1], RuntimeError) and "漂移" in str(outcomes[1])
                return receipt
            finally:
                release.set()
                entered.set()
                thread.join(2)
                await asyncio.gather(*tasks, return_exceptions=True)
                module._existing_feedback = original


async def main() -> None:
    """输出实际回执，任何不满足条件的路径返回非零。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--expect-blocking", action="store_true")
    parser.add_argument("--init-error-only", action="store_true")
    parser.add_argument("--atomic-only", action="store_true")
    parser.add_argument("--expect-two", action="store_true")
    args = parser.parse_args()
    plugin = load_source(args.source.resolve())
    if args.init_error_only:
        print(json.dumps(await check_initialize_error(plugin), ensure_ascii=False, indent=2))
        return
    if args.atomic_only:
        print(json.dumps(await check_two_owners(plugin, args.expect_two), ensure_ascii=False, indent=2))
        return
    kinds = ("discovery_lock",) if args.expect_blocking else (
        "discovery_lock", "accepted_cancel", "queued_cancel", "write_error_cancel",
    )
    receipts = [await scenario(plugin, kind, args.expect_blocking) for kind in kinds]
    initialize = None if args.expect_blocking else await check_initialize_error(plugin)
    atomic = None if args.expect_blocking else await check_two_owners(plugin)
    print(json.dumps({"scenarios": receipts, "initialize_error": initialize,
                      "two_owners": atomic, "discovery_cost": await throughput(plugin)},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
