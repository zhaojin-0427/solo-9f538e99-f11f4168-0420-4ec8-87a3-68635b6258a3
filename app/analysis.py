"""分析读取入口：只提供已发布的核对记录及其来源。

* 数据源仅限 ``publications`` / ``publication_pairs``，未发布的原始实测
  不会出现在任何分析接口中（原始值仍由 ``/rows`` 读取，互不影响）。
* 指标名在发布时固化在发布记录里；改名、删除、同名复用新身份都不改变
  已发布结果。
* 全部为只读查询，不创建快照、不写入任何数据。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

from .models import NotFound


def _row_to_pairs(conn: sqlite3.Connection, publication_id: int) -> List[dict]:
    rows = conn.execute(
        "SELECT pair_position, measured_value, measured_shard_id, measured_row_index, "
        "       reference_value, tolerance, abs_diff, reference_source "
        "FROM publication_pairs WHERE publication_id = ? ORDER BY pair_position",
        (publication_id,),
    ).fetchall()
    return [
        {
            "pair_position": r["pair_position"],
            "measured_value": r["measured_value"],
            "measured_shard_id": r["measured_shard_id"],
            "measured_row_index": r["measured_row_index"],
            "reference_value": r["reference_value"],
            "tolerance": r["tolerance"],
            "abs_diff": r["abs_diff"],
            "reference_source": r["reference_source"],
        }
        for r in rows
    ]


def list_publications(
    conn: sqlite3.Connection,
    snapshot_id: Optional[int] = None,
    station: Optional[str] = None,
    indicator_id: Optional[int] = None,
) -> dict:
    """按可选条件列出已发布记录的摘要（不含配对明细）。"""
    sql = (
        "SELECT publication_id, snapshot_id, station, sampled_at, indicator_id, "
        "       indicator_name, pair_count, created_at "
        "FROM publications WHERE 1 = 1"
    )
    params: list = []
    if snapshot_id is not None:
        sql += " AND snapshot_id = ?"
        params.append(snapshot_id)
    if station is not None:
        sql += " AND station = ?"
        params.append(station)
    if indicator_id is not None:
        sql += " AND indicator_id = ?"
        params.append(indicator_id)
    sql += " ORDER BY publication_id"

    rows = conn.execute(sql, params).fetchall()
    publications: List[dict] = []
    for r in rows:
        total = conn.execute(
            "SELECT COALESCE(SUM(abs_diff), 0) AS total FROM publication_pairs "
            "WHERE publication_id = ?",
            (r["publication_id"],),
        ).fetchone()["total"]
        publications.append({
            "publication_id": r["publication_id"],
            "snapshot_id": r["snapshot_id"],
            "station": r["station"],
            "sampled_at": r["sampled_at"],
            "indicator_id": r["indicator_id"],
            "indicator_name": r["indicator_name"],
            "pair_count": r["pair_count"],
            "total_abs_diff": total,
            "created_at": r["created_at"],
        })
    return {"count": len(publications), "publications": publications}


def get_publication_detail(conn: sqlite3.Connection, publication_id: int) -> dict:
    """读取单条已发布记录的完整配对明细与双方来源；不存在返回 404。"""
    row = conn.execute(
        "SELECT publication_id, snapshot_id, station, sampled_at, indicator_id, "
        "       indicator_name, pair_count, created_at "
        "FROM publications WHERE publication_id = ?",
        (publication_id,),
    ).fetchone()
    if row is None:
        raise NotFound(f"已发布记录 {publication_id} 不存在")

    pairs = _row_to_pairs(conn, publication_id)
    total_abs_diff = sum(p["abs_diff"] for p in pairs)
    return {
        "publication_id": row["publication_id"],
        "snapshot_id": row["snapshot_id"],
        "station": row["station"],
        "sampled_at": row["sampled_at"],
        "indicator_id": row["indicator_id"],
        "indicator_name": row["indicator_name"],
        "pair_count": row["pair_count"],
        "total_abs_diff": total_abs_diff,
        "created_at": row["created_at"],
        "pairs": pairs,
    }
