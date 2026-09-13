"""Proactive Feedback 自有迁移边界支持。"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import IO


def validate_workspace_plugin_data_path(path: Path, workspace: Path) -> None:
    """确认插件数据路径属于 workspace，且现有路径不穿过符号链接。"""

    root = workspace.resolve(strict=False)
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"插件数据目录越界: {path}") from error
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError(f"插件数据目录不能穿过符号链接: {current}")


def ensure_workspace_plugin_data_dir(path: Path, workspace: Path) -> None:
    """安全创建插件自有数据目录，并再次核对创建结果。"""

    validate_workspace_plugin_data_path(path, workspace)
    path.mkdir(parents=True, exist_ok=True)
    validate_workspace_plugin_data_path(path, workspace)


class WorkspaceInstanceLock:
    """保证迁移与 runtime 不能同时写入同一 workspace。"""

    def __init__(self, workspace: Path) -> None:
        self.path = workspace / ".instance.lock"
        self._stream: IO[str] | None = None

    def acquire(self) -> None:
        """非阻塞取得 workspace 锁，冲突时保留 owner 诊断。"""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            stream.seek(0)
            owner = stream.read().strip() or "unknown"
            stream.close()
            raise RuntimeError(
                f"workspace 已由其他 runtime 占用: {self.path} owner={owner}"
            ) from exc
        stream.seek(0)
        stream.truncate()
        stream.write(str(os.getpid()))
        stream.flush()
        self._stream = stream

    def release(self) -> None:
        """释放内核锁并保留可复用的诊断文件。"""

        stream = self._stream
        self._stream = None
        if stream is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()


_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def validate_marketplace(value: str) -> None:
    """校验 marketplace 可安全形成插件数据目录名。"""

    if not _SAFE_SEGMENT.fullmatch(value):
        raise ValueError(f"Proactive Feedback marketplace 无效: {value}")
