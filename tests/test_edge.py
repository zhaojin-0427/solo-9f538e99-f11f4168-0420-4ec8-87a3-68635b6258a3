"""补充：字段变更失败的原子性、边界与时间戳格式。"""
from __future__ import annotations

import pyarrow as pa


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def test_failed_field_change_creates_no_version_and_keeps_state(client):
    before = client.get("/schema").json()
    assert before["current_version"] == 1

    # 改不存在的名 -> 404，字段版不变
    r = client.post("/schema/indicators/rename", json={"old_name": "幽灵", "new_name": "x"})
    assert r.status_code == 404
    assert client.get("/schema").json()["current_version"] == 1

    client.post("/schema/indicators", json={"name": "a"})
    client.post("/schema/indicators", json={"name": "b"})
    v_before = client.get("/schema").json()["current_version"]

    # 改成已被占用的名字 -> 409，版本号、名称均不变
    r = client.post("/schema/indicators/rename", json={"old_name": "a", "new_name": "b"})
    assert r.status_code == 409
    schema = client.get("/schema").json()
    assert schema["current_version"] == v_before
    assert {i["name"] for i in schema["indicators"]} == {"a", "b"}

    # 改回自己的名字是幂等的，不产生新版本
    r = client.post("/schema/indicators/rename", json={"old_name": "a", "new_name": "a"})
    assert r.status_code == 200
    assert r.json()["new_version"] == v_before

    # 删除不存在的 -> 404，无版本
    assert client.delete("/schema/indicators/幽灵").status_code == 404
    assert client.get("/schema").json()["current_version"] == v_before


def test_exactly_eight_indicators_allowed_ninth_rejected(client):
    for i in range(8):
        r = client.post("/schema/indicators", json={"name": f"ind{i}"})
        assert r.status_code == 201, r.text
    assert client.post("/schema/indicators", json={"name": "ninth"}).status_code == 409

    # 删掉一个后可以再建（新建是新身份）
    assert client.delete("/schema/indicators/ind7").status_code == 200
    r = client.post("/schema/indicators", json={"name": "ind8"})
    assert r.status_code == 201
    assert len(client.get("/schema").json()["indicators"]) == 8


def test_timestamp_variants_and_integer_indicators(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "t"}).json()["new_version"]

    # Z 后缀、带偏移量（应转 UTC）、原生 timestamp、整数指标列
    from datetime import datetime, timezone

    f = make_parquet("ts.parquet", {
        "station": ["z", "offset", "naive", "int"],
        "sampled_at": pa.array(
            [
                datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc),  # 源值 02:00+02:00
                datetime(2026, 7, 1, 0, 0, 0),                       # 无时区按 UTC
                datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us"),
        ).to_pylist(),
        "t": [1.5, 2.5, 3, None],
    }, types={"sampled_at": pa.timestamp("us"), "t": pa.float64()})
    r = upload_file(client, f, v)
    assert r.status_code == 200, r.text

    # 再单独验证字符串形式（Z 与偏移量）的解析
    f_str = make_parquet("ts-str.parquet", {
        "station": ["s1", "s2"],
        "sampled_at": ["2026-07-01T00:00:00Z", "2026-07-01T02:00:00+02:00"],
        "t": [5.0, 6.0],
    }, types={"sampled_at": pa.string(), "t": pa.float64()})
    assert upload_file(client, f_str, v).status_code == 200

    body = client.get("/rows", params={"page_size": 100}).json()
    assert body["total_rows"] == 6
    assert all(
        row["sampled_at"] == "2026-07-01T00:00:00.000000Z" for row in body["rows"]
    )
    # 整数 3 被接纳为浮点 3.0
    values = {row["station"]: row["t"] for row in body["rows"]}
    assert values["naive"] == 3.0
    assert values["int"] is None
    assert values["s1"] == 5.0
    assert values["s2"] == 6.0


def test_pagination_total_pages_and_sorting(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "p"}).json()["new_version"]
    f = make_parquet("page.parquet", {
        "station": ["B", "A", "A", "B"],
        "sampled_at": [
            "2026-08-02T00:00:00Z",
            "2026-08-01T00:00:00Z",
            "2026-08-02T00:00:00Z",
            "2026-08-01T00:00:00Z",
        ],
        "p": [4.0, 1.0, 2.0, 3.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, v).status_code == 200

    first = client.get("/rows", params={"page": 1, "page_size": 2}).json()
    assert first["total_rows"] == 4
    assert first["total_pages"] == 2
    keys = [(r["station"], r["sampled_at"]) for r in first["rows"]]
    assert keys == [("A", "2026-08-01T00:00:00.000000Z"), ("A", "2026-08-02T00:00:00.000000Z")]

    second = client.get(
        "/rows", params={"snapshot_id": first["snapshot_id"], "page": 2, "page_size": 2}
    ).json()
    keys = [(r["station"], r["sampled_at"]) for r in second["rows"]]
    assert keys == [("B", "2026-08-01T00:00:00.000000Z"), ("B", "2026-08-02T00:00:00.000000Z")]

    # 超界页返回空行但分页信息不变
    empty = client.get(
        "/rows", params={"snapshot_id": first["snapshot_id"], "page": 3, "page_size": 2}
    ).json()
    assert empty["rows"] == []
    assert empty["total_rows"] == 4
