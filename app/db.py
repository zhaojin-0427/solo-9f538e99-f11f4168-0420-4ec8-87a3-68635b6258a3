"""SQLite 元数据库：建表语句、连接管理与事务辅助。

设计要点
========
1. 指标有稳定的*身份*（``indicators.indicator_id``），改名只改名字不改身份；
   新指标（含同名复用）一律分配新的身份，旧分片里不会继承旧指标的值。
2. 每次字段变更产生一个不可变的*字段版*（``schema_versions``），
   ``version_indicators`` 记录该版下“字段名 -> 指标身份”的映射。
   上传 Parquet 时按指定字段版解释列名，从而做到改名不串列、删名即拒绝。
3. 上传一份 Parquet 产生一个*分片*，分片与指标身份的关联记录在
   ``shard_indicators``；快照只固化分片集合与字段集合，读取按身份合读。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import DB_PATH, DB_URL, SHARD_DIR

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 指标身份（改名不换 ID；删除后 active=0，ID 永不复用）
CREATE TABLE IF NOT EXISTS indicators (
    indicator_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    active       INTEGER NOT NULL CHECK (active IN (0, 1))
);

-- 字段版：每次新增/改名/删除产生一个新版本，版本不可变
CREATE TABLE IF NOT EXISTS schema_versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    change_note  TEXT NOT NULL
);

-- 某字段版下，字段名 -> 指标身份 的映射（含当时已存在的全部指标）
CREATE TABLE IF NOT EXISTS version_indicators (
    version_id    INTEGER NOT NULL REFERENCES schema_versions(version_id),
    indicator_id  INTEGER NOT NULL REFERENCES indicators(indicator_id),
    name          TEXT NOT NULL,
    active        INTEGER NOT NULL CHECK (active IN (0, 1)),
    PRIMARY KEY (version_id, indicator_id)
);

-- Parquet 分片
CREATE TABLE IF NOT EXISTS shards (
    shard_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES schema_versions(version_id),
    file_path       TEXT NOT NULL UNIQUE,
    row_count       INTEGER NOT NULL CHECK (row_count >= 0),
    original_name   TEXT NOT NULL,
    uploaded_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 分片实际包含哪些指标身份（用于按快照合读）
CREATE TABLE IF NOT EXISTS shard_indicators (
    shard_id       INTEGER NOT NULL REFERENCES shards(shard_id),
    indicator_id   INTEGER NOT NULL REFERENCES indicators(indicator_id),
    PRIMARY KEY (shard_id, indicator_id)
);

-- 查询快照：首次查询固化字段集合与分片集合
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS snapshot_fields (
    snapshot_id   INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
    indicator_id  INTEGER NOT NULL REFERENCES indicators(indicator_id),
    name          TEXT NOT NULL,
    position      INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, indicator_id)
);

CREATE TABLE IF NOT EXISTS snapshot_shards (
    snapshot_id  INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
    shard_id     INTEGER NOT NULL REFERENCES shards(shard_id),
    position     INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, shard_id)
);

CREATE INDEX IF NOT EXISTS idx_indicators_active ON indicators(active);
CREATE INDEX IF NOT EXISTS idx_version_indicators_version
    ON version_indicators(version_id);
CREATE INDEX IF NOT EXISTS idx_shard_indicators_indicator
    ON shard_indicators(indicator_id);
"""


def connect() -> sqlite3.Connection:
    """打开一个启用外键约束的 SQLite 连接（行工厂为 dict 风格 Row）。

    每个请求使用独立连接；``check_same_thread=False`` 允许连接在 uvicorn
    工作线程内随依赖创建/关闭，写串行化由进程内写锁 + BEGIN IMMEDIATE 保证。
    """
    conn = sqlite3.connect(DB_URL, uri=True, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def immediate_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """以 ``BEGIN IMMEDIATE`` 开启写事务。

    所有字段变更与上传都在同一把进程锁 + 同一个立即事务内裁决，
    保证交错操作看到的是同一个已提交字段状态；异常时整体回滚，
    不会留下半份字段、数据或快照。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


def init_db() -> None:
    """初始化目录与表结构，并写入空的字段版 1。"""
    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # 清理上次进程崩溃可能残留的临时分片
    for stale in SHARD_DIR.glob(".tmp-*.parquet"):
        stale.unlink(missing_ok=True)
    conn = connect()
    try:
        conn.executescript(SCHEMA_SQL)
        row = conn.execute("SELECT COUNT(*) AS n FROM schema_versions").fetchone()
        if row["n"] == 0:
            conn.execute(
                "INSERT INTO schema_versions (version_id, change_note) VALUES (1, ?)",
                ("初始化空字段版",),
            )
            conn.commit()
    finally:
        conn.close()
