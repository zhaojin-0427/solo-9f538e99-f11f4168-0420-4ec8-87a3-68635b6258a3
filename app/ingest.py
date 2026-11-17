"""Parquet 接入：按指定字段版解释列名、校验并原子落盘为分片。

裁决规则
========
* 上传必须显式指定字段版；该版本下的“字段名 -> 指标身份”映射是不可变的。
* 旧版中仍存续的指标按**原身份**接入：改名后用旧版上传，列仍归旧指标，
  与新指标各走各的身份列，历史值不会串列。
* 列映射到已删除指标（active=0）→ 整次拒绝；出现版本映射之外的未知列 → 整次拒绝。
* 固定字段 ``station`` / ``sampled_at`` 必填；至多 8 个可空浮点指标列。
* 校验失败只抛异常，由事务整体回滚，不写入分片、不改任何已有数据。
"""
from __future__ import annotations

import io
import os
import sqlite3
import uuid
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from . import catalog
from .config import (
    FIXED_COLUMNS,
    INDICATOR_COLUMN_PREFIX,
    MAX_INDICATORS,
    SAMPLED_AT_COLUMN,
    SHARD_DIR,
    STATION_COLUMN,
)
from .models import ConflictError


def _require_columns(table: pa.Table) -> None:
    missing = [c for c in FIXED_COLUMNS if c not in table.column_names]
    if missing:
        raise ConflictError(f"Parquet 缺少固定字段列：{missing}；固定字段为 {list(FIXED_COLUMNS)}")
    if len(table.column_names) != len(set(table.column_names)):
        raise ConflictError("Parquet 中存在重复列名")


def _station_array(table: pa.Table, n_rows: int) -> pa.Array:
    col = table.column(STATION_COLUMN)
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        arr = col
    else:
        try:
            arr = col.cast(pa.string())
        except pa.ArrowInvalid as exc:
            raise ConflictError(f"固定字段 {STATION_COLUMN!r} 必须是字符串：{exc}") from exc
    py_list = arr.to_pylist()
    for v in py_list:
        if v is None or not str(v).strip():
            raise ConflictError(f"固定字段 {STATION_COLUMN!r} 不允许为空")
    return pa.array([str(v) for v in py_list], type=pa.string())


def _parse_iso8601_utc(values: list) -> pa.Array:
    """用标准库解析 ISO-8601 字符串（支持 Z 后缀与时区偏移），统一转 UTC。"""
    from datetime import datetime, timezone

    parsed: list[datetime] = []
    for raw in values:
        if raw is None:
            raise ConflictError(f"固定字段 {SAMPLED_AT_COLUMN!r} 不允许为空")
        text = str(raw).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ConflictError(
                f"固定字段 {SAMPLED_AT_COLUMN!r} 必须是 ISO-8601 时间戳字符串，非法值：{raw!r}"
            ) from exc
        if dt.tzinfo is None:
            # 约定无时区值即 UTC
            dt = dt.replace(tzinfo=timezone.utc)
        parsed.append(dt.astimezone(timezone.utc))
    return pa.array(parsed, type=pa.timestamp("us", tz="UTC"))


def _sampled_at_array(table: pa.Table, n_rows: int) -> pa.Array:
    """站点采样时刻：统一为 UTC 无时区微秒精度时间戳。"""
    col = table.column(SAMPLED_AT_COLUMN)
    t = col.type

    if pa.types.is_timestamp(t):
        if t.tz is None:
            # 约定无时区值即 UTC
            arr = col.cast(pa.timestamp("us", tz="UTC"))
        else:
            arr = col.cast(pa.timestamp("us", tz="UTC"))
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        arr = _parse_iso8601_utc(col.to_pylist())
    elif pa.types.is_date(t):
        arr = col.cast(pa.timestamp("us", tz="UTC"))
    else:
        raise ConflictError(
            f"固定字段 {SAMPLED_AT_COLUMN!r} 必须是 timestamp / ISO-8601 字符串 / date，实际为 {t}"
        )

    if arr.null_count:
        raise ConflictError(f"固定字段 {SAMPLED_AT_COLUMN!r} 不允许为空")
    return arr


def _indicator_array(col: pa.ChunkedArray, column_name: str) -> pa.Array:
    """指标列统一转 float64；全部为 null 的列也允许（可空浮点指标）。"""
    if pa.types.is_boolean(col.type):
        raise ConflictError(f"指标列 {column_name!r} 不接受布尔类型，请提供浮点数值")
    if pa.types.is_floating(col.type) or pa.types.is_integer(col.type) or pa.types.is_null(col.type):
        try:
            return col.cast(pa.float64()).combine_chunks()
        except pa.ArrowInvalid as exc:
            raise ConflictError(f"指标列 {column_name!r} 无法转换为 float64：{exc}") from exc
    raise ConflictError(
        f"指标列 {column_name!r} 必须是可空浮点（或整数）类型，实际为 {col.type}"
    )


def _temp_path() -> str:
    token = uuid.uuid4().hex
    return str(SHARD_DIR / f".tmp-{token}.parquet")


def ingest_upload(
    conn: sqlite3.Connection,
    file_bytes: bytes,
    version_id: int,
    original_name: str,
) -> dict:
    """在调用方事务内完成一次上传的全部裁决与落盘。

    :raises ConflictError/NotFound: 任何校验失败，调用方回滚事务即可，无副作用残留。
    """
    # 1) 字段版必须存在；取该版不可变映射
    mapping_rows = catalog.version_mapping(conn, version_id)
    name_to_row = {r["name"]: r for r in mapping_rows}

    # 2) 读 Parquet（未受信任的上传内容，只做结构解析，不执行任何代码）
    try:
        table = pq.read_table(io.BytesIO(file_bytes))
    except Exception as exc:  # noqa: BLE001 - Arrow 异常类型较多，统一转 409
        raise ConflictError(f"无法解析 Parquet 文件：{exc}") from exc

    n_rows = table.num_rows
    if n_rows == 0:
        raise ConflictError("Parquet 不允许为空分片（0 行），请提供至少一行化验数据")

    _require_columns(table)
    if len(table.column_names) - len(FIXED_COLUMNS) > MAX_INDICATORS:
        raise ConflictError(f"一次上传至多包含 {MAX_INDICATORS} 个指标列")

    # 3) 固定字段
    arrays: dict[str, pa.Array] = {
        STATION_COLUMN: _station_array(table, n_rows),
        SAMPLED_AT_COLUMN: _sampled_at_array(table, n_rows),
    }

    # 4) 指标列：按字段版映射到指标身份；未知列 / 已删指标整次拒绝
    present_indicator_ids: List[int] = []
    for column_name in table.column_names:
        if column_name in FIXED_COLUMNS:
            continue
        mapped = name_to_row.get(column_name)
        if mapped is None:
            known = sorted(name_to_row.keys())
            raise ConflictError(
                f"列 {column_name!r} 在字段版 {version_id} 中不存在，整次拒绝；"
                f"该版已知列：{known}"
            )
        if mapped["active"] == 0 or mapped["current_active"] == 0:
            raise ConflictError(
                f"列 {column_name!r} 对应指标 indicator_id={mapped['indicator_id']} "
                f"已被删除，含已删指标的上传整次拒绝"
            )
        indicator_id = int(mapped["indicator_id"])
        canonical = f"{INDICATOR_COLUMN_PREFIX}{indicator_id}"
        if canonical in arrays:
            raise ConflictError(f"字段版 {version_id} 中存在重名列 {column_name!r}，无法裁决")
        arrays[canonical] = _indicator_array(table.column(column_name), column_name)
        present_indicator_ids.append(indicator_id)

    # 5) 规范化分片表（固定列 + 身份列），原子落盘
    canonical_table = pa.table(arrays)
    tmp_path = _temp_path()
    try:
        pq.write_table(canonical_table, tmp_path, compression="zstd")
        # fsync 保证 rename 后内容可持久化
        with open(tmp_path, "rb") as fh:
            os.fsync(fh.fileno())

        cur = conn.execute(
            "INSERT INTO shards (version_id, file_path, row_count, original_name) "
            "VALUES (?, ?, ?, ?)",
            (version_id, tmp_path, n_rows, original_name),
        )
        shard_id = int(cur.lastrowid)
        final_path = str(SHARD_DIR / f"shard-{shard_id:010d}.parquet")
        os.rename(tmp_path, final_path)
        # 文件已改名，更新元数据
        conn.execute("UPDATE shards SET file_path = ? WHERE shard_id = ?", (final_path, shard_id))
        for indicator_id in present_indicator_ids:
            conn.execute(
                "INSERT INTO shard_indicators (shard_id, indicator_id) VALUES (?, ?)",
                (shard_id, indicator_id),
            )
    except BaseException:
        # 任何意外：清理临时文件，交由外层事务回滚
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    return {
        "shard_id": shard_id,
        "schema_version": version_id,
        "row_count": n_rows,
        "stored_columns": [f"{INDICATOR_COLUMN_PREFIX}{i}" for i in present_indicator_ids],
        "file_path": final_path,
    }
