"""核对发布：在指定快照内按同键提取实测，与交接对照值一对一配对，全配上才发布。

语义要点
========
* 实测只从**该快照固化的分片**（``snapshot_shards``）中提取，且只认同键
  （station + UTC sampled_at + indicator_id 身份）；快照之后的新上传分片
  不会混入，同名复用得到的新指标身份也不会混入。
* 指标值为 NULL 的同键实测不可配对，单独回报；非 NULL 实测超过 20 条直接报错。
* 配对规则：先使容差内配对数最多，再使绝对差总和最小（见 :mod:`app.matching`）。
* 只有两侧全部配上（无 NULL、无多余、无缺失且全部在容差内）才整组写入
  ``publications`` / ``publication_pairs``；任何失败都抛异常由调用方事务回滚，
  不开放半组。发布与字段变更/上传共用进程写锁 + ``BEGIN IMMEDIATE``。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import List, Optional

import pyarrow.parquet as pq

from .config import INDICATOR_COLUMN_PREFIX, MAX_CHECK_VALUES, SAMPLED_AT_COLUMN, STATION_COLUMN
from .matching import MeasuredPoint, Pairing, ReferencePoint, solve_pairings
from .models import CheckPublishRequest, ConflictError, NotFound

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def format_utc(dt: datetime) -> str:
    """统一输出带微秒与 Z 后缀的 UTC ISO-8601 字符串。"""
    return dt.strftime(TS_FORMAT)


def _snapshot_indicator(conn: sqlite3.Connection, snapshot_id: int, indicator_id: int) -> sqlite3.Row:
    """校验快照存在且固化了该指标身份，返回快照字段行（含固化指标名）。"""
    snap = conn.execute(
        "SELECT snapshot_id FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()
    if snap is None:
        raise NotFound(f"快照 {snapshot_id} 不存在")

    indicator = conn.execute(
        "SELECT indicator_id FROM indicators WHERE indicator_id = ?", (indicator_id,)
    ).fetchone()
    if indicator is None:
        raise NotFound(f"指标身份 indicator_id={indicator_id} 不存在")

    field = conn.execute(
        "SELECT indicator_id, name FROM snapshot_fields "
        "WHERE snapshot_id = ? AND indicator_id = ?",
        (snapshot_id, indicator_id),
    ).fetchone()
    if field is None:
        raise ConflictError(
            f"快照 {snapshot_id} 未固化指标身份 indicator_id={indicator_id}；"
            "该指标可能是快照创建之后才新增的（同名新指标不得混入本次核对）"
        )
    return field


def extract_measured(
    conn: sqlite3.Connection,
    snapshot_id: int,
    station: str,
    sampled_at_utc: datetime,
    indicator_id: int,
) -> tuple[List[MeasuredPoint], list[dict]]:
    """扫描快照全部分片，提取同键实测。

    :return: (非 NULL 实测列表, NULL 同键记录 [{shard_id, row_index}])
    """
    canonical = f"{INDICATOR_COLUMN_PREFIX}{indicator_id}"
    measured: List[MeasuredPoint] = []
    null_rows: list[dict] = []

    shards = conn.execute(
        "SELECT ss.shard_id, s.file_path "
        "FROM snapshot_shards ss JOIN shards s ON s.shard_id = ss.shard_id "
        "WHERE ss.snapshot_id = ? ORDER BY ss.position",
        (snapshot_id,),
    ).fetchall()

    for shard in shards:
        shard_id = int(shard["shard_id"])
        path = shard["file_path"]
        # 分片不包含该身份物理列 -> 该指标在本片“没有实测”（不是 NULL 实测），
        # 与快照中“旧分片没有新身份列即为 NULL”的合读语义区分开，避免把无关
        # 分片的行误报成无法配对的 NULL。
        has_column = conn.execute(
            "SELECT 1 FROM shard_indicators WHERE shard_id = ? AND indicator_id = ?",
            (shard_id, indicator_id),
        ).fetchone() is not None
        if not has_column:
            continue

        table = pq.read_table(path, columns=[STATION_COLUMN, SAMPLED_AT_COLUMN, canonical])
        stations = table.column(STATION_COLUMN).combine_chunks().to_pylist()
        times = table.column(SAMPLED_AT_COLUMN).combine_chunks().to_pylist()
        values = table.column(canonical).combine_chunks().to_pylist()

        for row_index, (st, ts, val) in enumerate(zip(stations, times, values)):
            if st != station or ts != sampled_at_utc:
                continue
            if val is None:
                # 同键、同身份列存在但值为 NULL：NULL 不可配对，单独回报
                null_rows.append({"shard_id": shard_id, "row_index": row_index})
            else:
                measured.append(MeasuredPoint(value=float(val), shard_id=shard_id, row_index=row_index))

    return measured, null_rows


def _best_pairs_payload(pairings: List[Pairing]) -> tuple[int, list[dict], list[dict], list[dict]]:
    """把配对结果拆成：可行配对、多余实测、缺失对照值。"""
    best_pairs: list[dict] = []
    extra_measured: list[dict] = []
    missing_references: list[dict] = []
    position = 0
    for p in pairings:
        if p.feasible:
            position += 1
            best_pairs.append({
                "pair_position": position,
                "measured_value": p.measured.value,
                "measured_shard_id": p.measured.shard_id,
                "measured_row_index": p.measured.row_index,
                "reference_value": p.reference.value,
                "tolerance": p.reference.tolerance,
                "abs_diff": p.abs_diff,
                "reference_source": p.reference.source,
            })
        else:
            if p.measured is not None:
                extra_measured.append({
                    "value": p.measured.value,
                    "shard_id": p.measured.shard_id,
                    "row_index": p.measured.row_index,
                })
            if p.reference is not None:
                missing_references.append({
                    "value": p.reference.value,
                    "tolerance": p.reference.tolerance,
                    "source": p.reference.source,
                })
    # 确定性顺序：多余实测按来源分片/行号，缺失对照按值与来源
    extra_measured.sort(key=lambda x: (x["shard_id"], x["row_index"]))
    missing_references.sort(key=lambda x: (x["value"], x["source"] or ""))
    return len(best_pairs), best_pairs, extra_measured, missing_references


def check_and_publish(conn: sqlite3.Connection, payload: CheckPublishRequest) -> dict:
    """在调用方写事务内执行核对；全配上则发布，否则抛 :class:`ConflictError`。"""
    snapshot_id = payload.snapshot_id
    station = payload.station.strip()
    sampled_at_utc = payload.normalized_sampled_at()
    sampled_at_text = format_utc(sampled_at_utc)
    indicator_id = payload.indicator_id

    field = _snapshot_indicator(conn, snapshot_id, indicator_id)
    indicator_name = field["name"]

    measured, null_rows = extract_measured(
        conn, snapshot_id, station, sampled_at_utc, indicator_id
    )

    if len(measured) > MAX_CHECK_VALUES:
        raise ConflictError(
            f"快照 {snapshot_id} 中站点 {station!r} 时刻 {sampled_at_text} 指标 "
            f"indicator_id={indicator_id} 的同键非 NULL 实测为 {len(measured)} 条，"
            f"超过上限 {MAX_CHECK_VALUES} 条，无法核对"
        )

    references = [
        ReferencePoint(value=item.value, tolerance=item.tolerance, source=item.source)
        for item in payload.references
    ]

    pairings = solve_pairings(measured, references)
    matched, best_pairs, extra_measured, missing_references = _best_pairs_payload(pairings)
    null_payload = [
        {"shard_id": row["shard_id"], "row_index": row["row_index"]} for row in null_rows
    ]

    fully_paired = (
        not null_rows
        and len(measured) == len(references)
        and len(measured) >= 1
        and matched == len(measured)
    )

    if not fully_paired:
        if null_rows:
            reason = "存在 NULL 实测，NULL 不可配对，整组不予发布"
        elif len(measured) < len(references):
            reason = "交接对照值多于同键实测，存在缺失实测，整组不予发布"
        elif len(measured) > len(references):
            reason = "同键实测多于交接对照值，存在多余实测，整组不予发布"
        else:
            reason = "存在超出绝对容差无法配对的值，整组不予发布"
        raise ConflictError({
            "reason": reason,
            "snapshot_id": snapshot_id,
            "station": station,
            "sampled_at": sampled_at_text,
            "indicator_id": indicator_id,
            "measured_count": len(measured),
            "null_measured_count": len(null_payload),
            "reference_count": len(references),
            "matched_pairs": matched,
            "extra_measured": extra_measured,
            "null_measured": null_payload,
            "missing_references": missing_references,
            "best_pairs": best_pairs,
        })

    # 全配上：查重后整组原子发布
    existing = conn.execute(
        "SELECT publication_id FROM publications "
        "WHERE snapshot_id = ? AND station = ? AND sampled_at = ? AND indicator_id = ?",
        (snapshot_id, station, sampled_at_text, indicator_id),
    ).fetchone()
    if existing is not None:
        raise ConflictError(
            f"该组核对结果已发布（publication_id={existing['publication_id']}），"
            "原有发布不可更改"
        )

    cur = conn.execute(
        "INSERT INTO publications "
        "(snapshot_id, station, sampled_at, indicator_id, indicator_name, pair_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (snapshot_id, station, sampled_at_text, indicator_id, indicator_name, len(best_pairs)),
    )
    publication_id = int(cur.lastrowid)
    total_abs_diff = 0.0
    # 发布明细按实测来源（分片、物理行号）稳定排序后落库
    best_pairs.sort(key=lambda p: (p["measured_shard_id"], p["measured_row_index"]))
    for position, pair in enumerate(best_pairs, start=1):
        pair["pair_position"] = position
        total_abs_diff += pair["abs_diff"]
        conn.execute(
            "INSERT INTO publication_pairs "
            "(publication_id, pair_position, measured_value, measured_shard_id, "
            " measured_row_index, reference_value, tolerance, abs_diff, reference_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                publication_id,
                position,
                pair["measured_value"],
                pair["measured_shard_id"],
                pair["measured_row_index"],
                pair["reference_value"],
                pair["tolerance"],
                pair["abs_diff"],
                pair["reference_source"],
            ),
        )

    return {
        "published": True,
        "publication_id": publication_id,
        "snapshot_id": snapshot_id,
        "station": station,
        "sampled_at": sampled_at_text,
        "indicator_id": indicator_id,
        "indicator_name": indicator_name,
        "pair_count": len(best_pairs),
        "total_abs_diff": total_abs_diff,
        "pairs": best_pairs,
    }
