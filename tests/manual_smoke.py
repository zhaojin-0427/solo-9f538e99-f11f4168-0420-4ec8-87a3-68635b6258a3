"""独立 HTTP 冒烟脚本：对运行中的服务验证完整业务故事（与 pytest 夹具无关）。

用法：先启动 uvicorn，再运行  python tests/manual_smoke.py
"""
from __future__ import annotations

import io
import sys

import os

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")


def parquet_bytes(columns: dict, types: dict | None = None) -> bytes:
    arrays = {name: pa.array(values, type=(types or {}).get(name)) for name, values in columns.items()}
    buf = io.BytesIO()
    pq.write_table(pa.table(arrays), buf)
    return buf.getvalue()


def main() -> int:
    c = httpx.Client(base_url=BASE, timeout=10)
    assert c.get("/health").status_code == 200

    # 字段版 2：新增“浓度”
    v2 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("version with 浓度:", v2)

    # 历史分片
    r = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("h.parquet", parquet_bytes({
            "station": ["S1"],
            "sampled_at": ["2026-01-01T00:00:00Z"],
            "浓度": [11.5],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    r.raise_for_status()
    print("historical shard:", r.json())

    # 改名 -> 硝酸盐（版 3）；再建“浓度”（版 4，新身份=磷酸盐）
    c.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"}).raise_for_status()
    v4 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("current version:", v4)

    # 旧版 2 上传 -> 硝酸盐原身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("old.parquet", parquet_bytes({
            "station": ["S2"], "sampled_at": ["2026-01-02T00:00:00Z"], "浓度": [22.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    # 新版 4 上传 -> 磷酸盐新身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("new.parquet", parquet_bytes({
            "station": ["S3"], "sampled_at": ["2026-01-03T00:00:00Z"], "浓度": [33.25],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    page = c.get("/rows").json()
    snap = page["snapshot_id"]
    print("snapshot:", snap, "names:", page["indicator_names"], "rows:", page["total_rows"])
    for row in page["rows"]:
        print(" ", row)

    by = {r["station"]: r for r in page["rows"]}
    assert by["S1"]["硝酸盐"] == 11.5 and by["S1"]["浓度"] is None
    assert by["S2"]["硝酸盐"] == 22.0 and by["S2"]["浓度"] is None
    assert by["S3"]["浓度"] == 33.25 and by["S3"]["硝酸盐"] is None

    # 快照之后删指标 + 上传，快照不变
    c.delete("/schema/indicators/浓度").raise_for_status()
    frozen = c.get("/rows", params={"snapshot_id": snap}).json()
    assert frozen["indicator_names"] == ["硝酸盐", "浓度"]
    assert frozen["total_rows"] == 3
    print("snapshot still frozen after delete+upload:", frozen["indicator_names"], frozen["total_rows"])

    # 已删指标上传必须 409
    rejected = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("x.parquet", parquet_bytes({
            "station": ["X"], "sampled_at": ["2026-01-04T00:00:00Z"], "浓度": [1.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    assert rejected.status_code == 409, rejected.text
    print("deleted-indicator upload rejected:", rejected.json()["detail"])

    print("\nSMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
