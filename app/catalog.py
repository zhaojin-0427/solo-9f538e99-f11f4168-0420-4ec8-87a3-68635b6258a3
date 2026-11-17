"""字段目录：指标身份、字段版与字段变更裁决。

所有写操作都要求调用方持有进程内写锁，并在一个立即事务内执行，
从而保证“字段变更与上传交错时按同一已提交字段状态裁决”。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

from .config import MAX_INDICATORS, RESERVED_NAMES
from .models import ConflictError, NotFound


# ---------------------------------------------------------------------------
# 读取辅助
# ---------------------------------------------------------------------------

def latest_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version_id) AS v FROM schema_versions").fetchone()
    return int(row["v"])


def active_indicators(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """当前存续指标，按指标 ID 排序。"""
    return conn.execute(
        "SELECT indicator_id, name, active FROM indicators "
        "WHERE active = 1 ORDER BY indicator_id"
    ).fetchall()


def active_names(conn: sqlite3.Connection) -> List[str]:
    return [r["name"] for r in active_indicators(conn)]


def find_active_by_name(conn: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT indicator_id, name, active FROM indicators "
        "WHERE active = 1 AND name = ? ORDER BY indicator_id",
        (name,),
    ).fetchone()


def version_note(conn: sqlite3.Connection, version_id: int) -> Optional[str]:
    row = conn.execute(
        "SELECT change_note FROM schema_versions WHERE version_id = ?",
        (version_id,),
    ).fetchone()
    return row["change_note"] if row else None


def version_mapping(conn: sqlite3.Connection, version_id: int) -> List[sqlite3.Row]:
    """某字段版下的全部映射记录（含已删指标，active=0），按指标 ID 排序。"""
    rows = conn.execute(
        "SELECT vi.indicator_id, vi.name, vi.active, i.name AS current_name, i.active AS current_active "
        "FROM version_indicators vi "
        "JOIN indicators i ON i.indicator_id = vi.indicator_id "
        "WHERE vi.version_id = ? ORDER BY vi.indicator_id",
        (version_id,),
    ).fetchall()
    if not rows:
        # 字段版 1 是空版，查不到映射属正常；不存在的版本号应报错
        if version_note(conn, version_id) is None:
            raise NotFound(f"字段版 {version_id} 不存在")
    return rows


def _validate_name(name: str) -> str:
    name = name.strip()
    if not name:
        raise ConflictError("指标名不能为空")
    if name in RESERVED_NAMES:
        raise ConflictError(f"指标名 {name!r} 是保留的固定字段名，不允许使用")
    return name


# ---------------------------------------------------------------------------
# 字段版写入：每次变更把*当前全部指标*连同新名字整体快照进新版本
# ---------------------------------------------------------------------------

def _snapshot_new_version(
    conn: sqlite3.Connection,
    change_note: str,
    overrides: Optional[dict[int, str]] = None,
    deactivate: Optional[set[int]] = None,
) -> int:
    """基于当前指标集合创建新字段版。

    :param overrides:   需要在新版中改名的指标 {indicator_id: new_name}
    :param deactivate:  需要在新版中标记删除的指标 ID 集合
    """
    overrides = overrides or {}
    deactivate = deactivate or set()

    cur = conn.execute(
        "INSERT INTO schema_versions (change_note) VALUES (?)", (change_note,)
    )
    new_version = int(cur.lastrowid)
    for row in conn.execute(
        "SELECT indicator_id, name, active FROM indicators ORDER BY indicator_id"
    ).fetchall():
        ind_id = row["indicator_id"]
        new_active = 0 if ind_id in deactivate else row["active"]
        new_name = overrides.get(ind_id, row["name"])
        conn.execute(
            "INSERT INTO version_indicators (version_id, indicator_id, name, active) "
            "VALUES (?, ?, ?, ?)",
            (new_version, ind_id, new_name, new_active),
        )
    return new_version


def create_indicator(conn: sqlite3.Connection, raw_name: str) -> tuple[int, str, int]:
    """新增指标。同名复用（含已删除的历史名）一律分配新身份，不继承旧值。"""
    name = _validate_name(raw_name)

    existing = find_active_by_name(conn, name)
    if existing is not None:
        raise ConflictError(f"指标名 {name!r} 已存在（indicator_id={existing['indicator_id']}），不能重复新增")

    count = conn.execute("SELECT COUNT(*) AS n FROM indicators WHERE active = 1").fetchone()["n"]
    if count >= MAX_INDICATORS:
        raise ConflictError(f"存续指标已达上限 {MAX_INDICATORS} 个，请先删除指标再新增")

    cur = conn.execute(
        "INSERT INTO indicators (name, active) VALUES (?, 1)", (name,)
    )
    indicator_id = int(cur.lastrowid)
    new_version = _snapshot_new_version(conn, f"新增指标 {name!r}（indicator_id={indicator_id}）")
    return indicator_id, name, new_version


def rename_indicator(conn: sqlite3.Connection, old_name: str, new_name_raw: str) -> tuple[int, str, int]:
    """指标改名：身份不变，仅产生新字段版；旧版映射保留旧名，保证历史不串列。"""
    old_name = _validate_name(old_name)
    new_name = _validate_name(new_name_raw)

    target = find_active_by_name(conn, old_name)
    if target is None:
        raise NotFound(f"当前不存在名为 {old_name!r} 的存续指标，无法改名")

    if new_name == old_name:
        # 幂等：新旧同名不产生新版本
        return int(target["indicator_id"]), old_name, latest_version(conn)

    clash = find_active_by_name(conn, new_name)
    if clash is not None:
        raise ConflictError(
            f"新指标名 {new_name!r} 已被 indicator_id={clash['indicator_id']} 占用，改名会造成重名"
        )

    indicator_id = int(target["indicator_id"])
    conn.execute("UPDATE indicators SET name = ? WHERE indicator_id = ?", (new_name, indicator_id))
    new_version = _snapshot_new_version(
        conn,
        f"指标改名：{old_name!r} -> {new_name!r}（indicator_id={indicator_id}，身份不变）",
        overrides={indicator_id: new_name},
    )
    return indicator_id, new_name, new_version


def delete_indicator(conn: sqlite3.Connection, raw_name: str) -> tuple[int, int]:
    """删除指标：身份保留但置为 inactive，产生新字段版。历史分片数据不删除。"""
    name = _validate_name(raw_name)
    target = find_active_by_name(conn, name)
    if target is None:
        raise NotFound(f"当前不存在名为 {name!r} 的存续指标，无法删除")

    indicator_id = int(target["indicator_id"])
    conn.execute("UPDATE indicators SET active = 0 WHERE indicator_id = ?", (indicator_id,))
    new_version = _snapshot_new_version(
        conn,
        f"删除指标 {name!r}（indicator_id={indicator_id}，历史分片保留）",
        deactivate={indicator_id},
    )
    return indicator_id, new_version
