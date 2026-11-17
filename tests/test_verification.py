"""核对发布与分析读取的端到端自测。"""
from __future__ import annotations

import math
import threading

import pyarrow as pa

from app.db import init_db
from app.main import app
from fastapi.testclient import TestClient


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def make_indicator(client, name: str = "氨氮") -> tuple[int, int]:
    r = client.post("/schema/indicators", json={"name": name})
    assert r.status_code == 201, r.text
    body = r.json()
    return body["indicator_id"], body["new_version"]


def make_snapshot(client) -> int:
    return client.get("/rows").json()["snapshot_id"]


def publish_payload(snapshot_id, station, sampled_at, indicator_id, references):
    return {
        "snapshot_id": snapshot_id,
        "station": station,
        "sampled_at": sampled_at,
        "indicator_id": indicator_id,
        "references": references,
    }


# ---------------------------------------------------------------------------
#  happy path：全部配上 -> 发布；分析只读已发布记录且带来源
# ---------------------------------------------------------------------------

def test_publish_happy_path_and_analysis_sources(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("a.parquet", {
        "station": ["S1", "S1", "S1"],
        "sampled_at": ["2026-09-01T00:00:00Z"] * 3,
        "氨氮": [1.0, 2.0, 3.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, version).status_code == 200
    snap = make_snapshot(client)

    # 发布前分析入口为空
    assert client.get("/analysis/published").json() == {"count": 0, "publications": []}

    payload = publish_payload(snap, "S1", "2026-09-01T00:00:00Z", ind_id, [
        {"value": 1.05, "tolerance": 0.1, "source": "交接单-A1"},
        {"value": 2.10, "tolerance": 0.2, "source": "交接单-A2"},
        {"value": 2.95, "tolerance": 0.1, "source": "交接单-A3"},
    ])
    r = client.post("/verification/publish", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["published"] is True
    assert body["pair_count"] == 3
    assert body["indicator_name"] == "氨氮"
    assert body["sampled_at"] == "2026-09-01T00:00:00.000000Z"
    # 第二目标：绝对差总和 = 0.05 + 0.10 + 0.05
    assert math.isclose(body["total_abs_diff"], 0.20, rel_tol=1e-9)
    # 每条配对带双方来源
    for pair in body["pairs"]:
        assert pair["measured_shard_id"] >= 1
        assert pair["measured_row_index"] in (0, 1, 2)
        assert pair["reference_source"].startswith("交接单-")
        assert pair["abs_diff"] <= pair["tolerance"]

    # 分析列表与明细
    listing = client.get("/analysis/published").json()
    assert listing["count"] == 1
    summary = listing["publications"][0]
    assert summary["snapshot_id"] == snap and summary["station"] == "S1"
    assert summary["indicator_id"] == ind_id and summary["pair_count"] == 3

    detail = client.get(f"/analysis/published/{summary['publication_id']}").json()
    assert len(detail["pairs"]) == 3
    measured_rows = sorted(p["measured_row_index"] for p in detail["pairs"])
    assert measured_rows == [0, 1, 2]
    assert {p["reference_source"] for p in detail["pairs"]} == {
        "交接单-A1", "交接单-A2", "交接单-A3"
    }

    # 过滤条件
    assert client.get("/analysis/published", params={"station": "OTHER"}).json()["count"] == 0
    assert client.get(
        "/analysis/published", params={"snapshot_id": snap, "indicator_id": ind_id}
    ).json()["count"] == 1


def test_publish_uses_optimal_pairing(client, make_parquet):
    """容差内配对数优先于就近贪心：必须配出 2 对而不是 1 对。"""
    ind_id, version = make_indicator(client, "总磷")
    f = make_parquet("b.parquet", {
        "station": ["S", "S"], "sampled_at": ["2026-09-02T00:00:00Z", "2026-09-02T00:00:00Z"],
        "总磷": [10.0, 11.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    # 贪心就近只能 1 对；最优交叉配法 2 对 -> 应成功发布
    r = client.post("/verification/publish", json=publish_payload(
        snap, "S", "2026-09-02T00:00:00Z", ind_id, [
            {"value": 9.5, "tolerance": 0.6},
            {"value": 10.5, "tolerance": 0.6},
        ]
    ))
    assert r.status_code == 200, r.text
    assert r.json()["pair_count"] == 2


# ---------------------------------------------------------------------------
# 失败场景：多余实测 / 缺失对照 / 超容差 / NULL / 超 20 条
# ---------------------------------------------------------------------------

def test_failure_extra_measured_reports_sources_and_publishes_nothing(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("c.parquet", {
        "station": ["S1", "S1", "S1"],
        "sampled_at": ["2026-10-01T00:00:00Z"] * 3,
        "氨氮": [1.0, 2.0, 3.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-10-01T00:00:00Z", ind_id, [
            {"value": 1.0, "tolerance": 0.5}, {"value": 2.0, "tolerance": 0.5},
        ]
    ))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["measured_count"] == 3 and detail["reference_count"] == 2
    assert detail["matched_pairs"] == 2
    assert len(detail["extra_measured"]) == 1
    assert detail["extra_measured"][0]["value"] == 3.0
    assert detail["extra_measured"][0]["shard_id"] >= 1
    assert "row_index" in detail["extra_measured"][0]
    assert detail["missing_references"] == []
    assert len(detail["best_pairs"]) == 2
    # 失败不产生任何发布
    assert client.get("/analysis/published").json()["count"] == 0


def test_failure_missing_references(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("d.parquet", {
        "station": ["S1"], "sampled_at": ["2026-10-02T00:00:00Z"], "氨氮": [1.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-10-02T00:00:00Z", ind_id, [
            {"value": 1.0, "tolerance": 0.1, "source": "单-1"},
            {"value": 9.0, "tolerance": 0.1, "source": "单-2"},
        ]
    ))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["matched_pairs"] == 1
    assert len(detail["missing_references"]) == 1
    assert detail["missing_references"][0] == {"value": 9.0, "tolerance": 0.1, "source": "单-2"}
    assert detail["extra_measured"] == []
    assert client.get("/analysis/published").json()["count"] == 0


def test_failure_out_of_tolerance_equal_counts(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("e.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": ["2026-10-03T00:00:00Z"] * 2,
        "氨氮": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-10-03T00:00:00Z", ind_id, [
            {"value": 1.0, "tolerance": 0.0},
            {"value": 5.0, "tolerance": 0.1},  # 与 2.0 差 3.0，无法配
        ]
    ))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["matched_pairs"] == 1
    # 两侧数量相等但有超容差：多余实测与缺失对照各报 1 条
    assert len(detail["extra_measured"]) == 1 and detail["extra_measured"][0]["value"] == 2.0
    assert len(detail["missing_references"]) == 1 and detail["missing_references"][0]["value"] == 5.0


def test_null_measured_cannot_pair(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("f.parquet", {
        "station": ["S1", "S1", "S1"],
        "sampled_at": ["2026-10-04T00:00:00Z"] * 3,
        "氨氮": [1.0, None, 3.0],
    }, types={"sampled_at": pa.string(), "氨氮": pa.float64()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-10-04T00:00:00Z", ind_id, [
            {"value": 1.0, "tolerance": 0.1}, {"value": 3.0, "tolerance": 0.1},
        ]
    ))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["measured_count"] == 2          # NULL 不计入可配对实测
    assert detail["null_measured_count"] == 1
    assert detail["null_measured"][0]["shard_id"] >= 1
    assert isinstance(detail["null_measured"][0]["row_index"], int)
    assert "NULL" in detail["reason"]
    assert client.get("/analysis/published").json()["count"] == 0


def test_more_than_twenty_measured_rejected(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("g.parquet", {
        "station": ["S1"] * 21,
        "sampled_at": ["2026-10-05T00:00:00Z"] * 21,
        "氨氮": [float(i) for i in range(21)],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    refs = [{"value": float(i), "tolerance": 1.0} for i in range(20)]
    r = client.post("/verification/publish",
                    json=publish_payload(snap, "S1", "2026-10-05T00:00:00Z", ind_id, refs))
    assert r.status_code == 409
    assert "20" in r.json()["detail"]
    assert client.get("/analysis/published").json()["count"] == 0


# ---------------------------------------------------------------------------
# 快照隔离、同名新指标不混入、改名冻结、后续上传不影响已发布
# ---------------------------------------------------------------------------

def test_snapshot_isolation_and_indicator_identity(client, make_parquet):
    nitrate_id, v2 = make_indicator(client, "浓度")
    f = make_parquet("h.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": ["2026-11-01T00:00:00Z"] * 2,
        "浓度": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, v2)
    snap_old = make_snapshot(client)

    # 快照之后：改名 + 同名新指标（新身份）+ 再上传
    client.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"})
    r = client.post("/schema/indicators", json={"name": "浓度"})
    phosphate_id = r.json()["indicator_id"]
    v4 = r.json()["new_version"]
    f2 = make_parquet("h2.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": ["2026-11-01T00:00:00Z"] * 2,
        "浓度": [100.0, 200.0],  # 新身份（磷酸盐）的值
    }, types={"sampled_at": pa.string()})
    upload_file(client, f2, v4)
    snap_new = make_snapshot(client)

    # 旧快照核对旧身份：只看到 2 条旧实测，新分片的新身份值不混入；
    # 固化指标名是快照当时的“浓度”
    r = client.post("/verification/publish", json=publish_payload(
        snap_old, "S1", "2026-11-01T00:00:00Z", nitrate_id, [
            {"value": 1.0, "tolerance": 0.0}, {"value": 2.0, "tolerance": 0.0},
        ]
    ))
    assert r.status_code == 200, r.text
    pub = r.json()
    assert pub["indicator_name"] == "浓度"          # 发布时固化的名字
    assert {p["measured_value"] for p in pub["pairs"]} == {1.0, 2.0}

    # 新身份不在旧快照里 -> 409，同名新指标不得混入
    r = client.post("/verification/publish", json=publish_payload(
        snap_old, "S1", "2026-11-01T00:00:00Z", phosphate_id, [
            {"value": 100.0, "tolerance": 1.0},
        ]
    ))
    assert r.status_code == 409

    # 新快照可核对新身份
    r = client.post("/verification/publish", json=publish_payload(
        snap_new, "S1", "2026-11-01T00:00:00Z", phosphate_id, [
            {"value": 100.0, "tolerance": 0.0}, {"value": 200.0, "tolerance": 0.0},
        ]
    ))
    assert r.status_code == 200, r.text

    # 改名不改变已发布结果：分析里旧发布仍叫“浓度”
    listing = client.get("/analysis/published", params={"indicator_id": nitrate_id}).json()
    assert listing["publications"][0]["indicator_name"] == "浓度"
    detail = client.get(f"/analysis/published/{pub['publication_id']}").json()
    assert detail["indicator_name"] == "浓度"

    # 原 /rows 仍可读原始值
    rows = client.get("/rows", params={"snapshot_id": snap_old}).json()
    assert {row["浓度"] for row in rows["rows"]} == {1.0, 2.0}


def test_duplicate_publish_rejected_and_original_unchanged(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("i.parquet", {
        "station": ["S1"], "sampled_at": ["2026-11-02T00:00:00Z"], "氨氮": [1.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    body = publish_payload(snap, "S1", "2026-11-02T00:00:00Z", ind_id,
                           [{"value": 1.0, "tolerance": 0.0}])
    assert client.post("/verification/publish", json=body).status_code == 200
    r = client.post("/verification/publish", json=body)
    assert r.status_code == 409 and "已发布" in r.json()["detail"]
    assert client.get("/analysis/published").json()["count"] == 1


def test_key_filtering_station_and_time_do_not_mix(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("j.parquet", {
        "station": ["S1", "S2", "S1"],
        "sampled_at": [
            "2026-11-03T00:00:00Z",
            "2026-11-03T00:00:00Z",
            "2026-11-03T01:00:00Z",
        ],
        "氨氮": [1.0, 2.0, 3.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    # 只匹配 S1 + 00:00 这一条；S2 与 01:00 的实测不混入
    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-11-03T00:00:00Z", ind_id, [{"value": 1.0, "tolerance": 0.0}]
    ))
    assert r.status_code == 200, r.text
    assert r.json()["pairs"][0]["measured_value"] == 1.0

    # 带时区偏移的同一 UTC 时刻也能匹配
    r2 = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-11-03T02:00:00+01:00", ind_id, [{"value": 3.0, "tolerance": 0.0}]
    ))
    assert r2.status_code == 200, r2.text
    assert r2.json()["sampled_at"] == "2026-11-03T01:00:00.000000Z"


# ---------------------------------------------------------------------------
# 404 / 422 / 重启持久化 / 并发
# ---------------------------------------------------------------------------

def test_not_found_and_request_validation(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("k.parquet", {
        "station": ["S1"], "sampled_at": ["2026-12-01T00:00:00Z"], "氨氮": [1.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    # 快照不存在
    r = client.post("/verification/publish", json=publish_payload(
        9999, "S1", "2026-12-01T00:00:00Z", ind_id, [{"value": 1.0, "tolerance": 0.0}]
    ))
    assert r.status_code == 404

    # 指标身份不存在
    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-12-01T00:00:00Z", 9999, [{"value": 1.0, "tolerance": 0.0}]
    ))
    assert r.status_code == 404

    # 对照值数量越界、容差为负、NaN
    import json as _json

    base = {"snapshot_id": snap, "station": "S1",
            "sampled_at": "2026-12-01T00:00:00Z", "indicator_id": ind_id}
    assert client.post("/verification/publish", json={**base, "references": []}).status_code == 422
    assert client.post("/verification/publish", json={
        **base, "references": [{"value": 1.0, "tolerance": -0.1}]
    }).status_code == 422
    # NaN：绕过客户端 JSON 限制，发送非标准 JSON 原文，服务端 Pydantic 拒绝（422 且响应仍是合法 JSON）
    nan_body = _json.dumps({**base, "references": [{"value": float("nan"), "tolerance": 0.1}]},
                           allow_nan=True)
    nan_resp = client.post(
        "/verification/publish", content=nan_body,
        headers={"content-type": "application/json"},
    )
    assert nan_resp.status_code == 422
    assert nan_resp.json()  # 错误体可被标准 JSON 解析
    assert client.post("/verification/publish", json={
        **base, "references": [{"value": float(i), "tolerance": 0.0} for i in range(21)]
    }).status_code == 422

    # 实测超 20 的 409 路径见 test_more_than_twenty_measured_rejected（对照值本身合法，20 个）
    # 不存在的发布记录 -> 404
    assert client.get("/analysis/published/9999").status_code == 404


def test_publication_survives_restart(client, make_parquet):
    ind_id, version = make_indicator(client)
    f = make_parquet("m.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": ["2026-12-02T00:00:00Z"] * 2,
        "氨氮": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)
    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2026-12-02T00:00:00Z", ind_id, [
            {"value": 1.0, "tolerance": 0.0, "source": "留档-1"},
            {"value": 2.0, "tolerance": 0.0, "source": "留档-2"},
        ]
    ))
    publication_id = r.json()["publication_id"]

    del client
    init_db()  # 模拟重启：重新建表（IF NOT EXISTS）不影响已持久化数据
    with TestClient(app) as client2:
        detail = client2.get(f"/analysis/published/{publication_id}").json()
        assert detail["pair_count"] == 2
        assert {p["reference_source"] for p in detail["pairs"]} == {"留档-1", "留档-2"}
        assert {p["measured_value"] for p in detail["pairs"]} == {1.0, 2.0}
        # 重启后仍可重复发布防护
        r2 = client2.post("/verification/publish", json=publish_payload(
            snap, "S1", "2026-12-02T00:00:00Z", ind_id, [
                {"value": 1.0, "tolerance": 0.0}, {"value": 2.0, "tolerance": 0.0},
            ]
        ))
        assert r2.status_code == 409


def test_concurrent_publish_and_read_never_half_group(client, make_parquet):
    ind_id, version = make_indicator(client)
    stations = [f"S{i}" for i in range(8)]
    f = make_parquet("n.parquet", {
        "station": stations,
        "sampled_at": ["2026-12-03T00:00:00Z"] * 8,
        "氨氮": [float(i) for i in range(8)],
    }, types={"sampled_at": pa.string()})
    upload_file(client, f, version)
    snap = make_snapshot(client)

    errors: list[Exception] = []
    barrier = threading.Barrier(9)

    def publisher(station: str, value: float):
        try:
            barrier.wait()
            r = client.post("/verification/publish", json=publish_payload(
                snap, station, "2026-12-03T00:00:00Z", ind_id,
                [{"value": value, "tolerance": 0.0, "source": f"单-{station}"}]
            ))
            assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def reader():
        try:
            barrier.wait()
            for _ in range(50):
                for item in client.get("/analysis/published").json()["publications"]:
                    detail = client.get(
                        f"/analysis/published/{item['publication_id']}"
                    ).json()
                    # 读到的发布必须是完整一组，绝不会出现半组
                    assert len(detail["pairs"]) == detail["pair_count"] == item["pair_count"]
                    assert detail["pair_count"] >= 1
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=publisher, args=(s, float(i)))
               for i, s in enumerate(stations)]
    threads.append(threading.Thread(target=reader))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    listing = client.get("/analysis/published").json()
    assert listing["count"] == 8


def test_measured_spanning_multiple_shards_reports_each_source(client, make_parquet):
    """同键实测分布在两个分片：配对与来源必须精确到分片 ID + 物理行号。"""
    ind_id, version = make_indicator(client, "高锰酸盐")
    f1 = make_parquet("o1.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": ["2027-01-01T00:00:00Z"] * 2,
        "高锰酸盐": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    f2 = make_parquet("o2.parquet", {
        "station": ["S1", "X"],
        "sampled_at": ["2027-01-01T00:00:00Z", "2027-01-01T00:00:00Z"],
        "高锰酸盐": [3.0, 9.0],
    }, types={"sampled_at": pa.string()})
    shard1 = upload_file(client, f1, version).json()["shard_id"]
    shard2 = upload_file(client, f2, version).json()["shard_id"]
    snap = make_snapshot(client)

    r = client.post("/verification/publish", json=publish_payload(
        snap, "S1", "2027-01-01T00:00:00Z", ind_id, [
            {"value": 3.0, "tolerance": 0.0},
            {"value": 1.0, "tolerance": 0.0},
            {"value": 2.0, "tolerance": 0.0},
        ]
    ))
    assert r.status_code == 200, r.text
    pairs = r.json()["pairs"]
    # 来源精确到分片与行号：前两个值来自 shard1 的行 0/1，第三个来自 shard2 的行 0
    sources = sorted((p["measured_shard_id"], p["measured_row_index"], p["measured_value"])
                     for p in pairs)
    assert sources == [
        (shard1, 0, 1.0),
        (shard1, 1, 2.0),
        (shard2, 0, 3.0),
    ]


def test_docs_include_new_endpoints(client):
    spec = client.get("/openapi.json").json()
    assert "/verification/publish" in spec["paths"]
    assert "/analysis/published" in spec["paths"]
    assert "/analysis/published/{publication_id}" in spec["paths"]
    assert client.get("/docs").status_code == 200
