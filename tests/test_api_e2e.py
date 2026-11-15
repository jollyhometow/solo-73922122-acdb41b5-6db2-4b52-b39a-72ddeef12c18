"""端到端测试：HTTP 生产 -> 后台消费者 -> 入仓/隔离 -> 查询/补正。

使用进程内 InMemoryBroker，无需 Docker 即可验证主链路；
位点提交/重放语义由 tests/test_consumer_reliability.py 单独覆盖。
"""
from __future__ import annotations

import json
import time

import pytest


def _wait_until(predicate, timeout=10.0, interval=0.05):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后值={last!r}")


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert set(body["assigned_partitions"]) == {0, 1}  # 内存消费者订阅两个分区


def test_full_pipeline_valid_and_quarantine_and_correction(client):
    # 1) 合法消息
    r = client.post("/messages", json={
        "message": {"container_no": "MSCU1234567",
                    "gross_weight": 20000, "tare_weight": 4000}
    })
    assert r.status_code == 201
    valid_meta = r.json()

    # 2) 毛重 < 皮重 -> 隔离
    r = client.post("/messages", json={
        "message": {"container_no": "BADU0000001",
                    "gross_weight": 3000, "tare_weight": 4200}
    })
    bad_meta = r.json()

    # 3) 畸形 JSON 原文 -> 隔离
    r = client.post("/messages", json={"raw_text": "{oops"})
    assert r.status_code == 201
    malformed_meta = r.json()

    # 等待消费者处理完
    def net_count():
        return len(client.get("/net-weights").json())

    _wait_until(lambda: net_count() == 1)

    nets = client.get("/net-weights").json()
    assert nets[0]["container_no"] == "MSCU1234567"
    assert nets[0]["net_weight"] == 16000
    assert nets[0]["source_partition"] == valid_meta["partition"]
    assert nets[0]["source_offset"] == valid_meta["offset"]

    quarantined = _wait_until(lambda: (
        q if len(q := client.get("/quarantine").json()) == 2 else None
    ))
    reasons = {q["reason_code"] for q in quarantined}
    assert "GROSS_LT_TARE" in reasons and "NOT_JSON" in reasons

    # 4) 按源分区/offset 查看隔离原文
    pos = client.get(
        f"/quarantine/by-position/{bad_meta['partition']}/{bad_meta['offset']}"
    ).json()
    assert json.loads(pos["raw_payload"])["container_no"] == "BADU0000001"
    assert pos["status"] == "OPEN"

    # 5) 先提交一份非法补正 -> 被拒但保持 OPEN
    r = client.post(f"/quarantine/{pos['id']}/correction", json={
        "container_no": "X", "gross_weight": 100, "tare_weight": 200
    })
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] is False and body["status"] == "OPEN"
    assert body["reason"]

    # 6) 合法补正生效，生成明细
    r = client.post(f"/quarantine/{pos['id']}/correction", json={
        "container_no": "MSCU1234567", "gross_weight": 5000, "tare_weight": 4200
    })
    body = r.json()
    assert r.status_code == 200 and body["accepted"] is True
    assert body["status"] == "CORRECTED"
    assert body["net_weight"]["net_weight"] == 800
    assert body["net_weight"]["from_correction"] is True

    # 7) 重复补正被拒
    r = client.post(f"/quarantine/{pos['id']}/correction", json={
        "container_no": "MSCU1234567", "gross_weight": 5000, "tare_weight": 4200
    })
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "ALREADY_CORRECTED"

    # 8) 明细共两条；隔离区仍可查到已补正记录
    nets = client.get("/net-weights").json()
    assert len(nets) == 2
    corrected = client.get("/quarantine?status=CORRECTED").json()
    assert len(corrected) == 1 and corrected[0]["id"] == pos["id"]


def test_seed_demo_endpoint(client):
    # 记录基线：同一 TestClient 的内存库可能已有前序用例数据，断言用增量
    base_n = len(client.get("/net-weights").json())
    base_q = len(client.get("/quarantine").json())

    r = client.post("/demo/seed")
    assert r.status_code == 200 and len(r.json()) == 6

    def totals():
        n = len(client.get("/net-weights").json())
        q = len(client.get("/quarantine").json())
        return (n, q)

    # 6 条演示消息 = 2 条合法入仓 + 4 条隔离
    _wait_until(lambda: (totals()[0] - base_n) >= 2 and (totals()[1] - base_q) >= 4)
    n, q = totals()
    assert n - base_n == 2 and q - base_q == 4


def test_unknown_position_returns_404(client):
    r = client.get("/quarantine/by-position/1/999999")
    assert r.status_code == 404


def test_produce_requires_payload(client):
    r = client.post("/messages", json={})
    assert r.status_code == 422


@pytest.mark.parametrize("partition,offset_key", [(0, "offset"), (1, "offset")])
def test_same_content_different_positions_preserved(client, partition, offset_key):
    payload = {"container_no": "DUPU0000000",
               "gross_weight": 100, "tare_weight": 200}  # 毛<皮 -> 隔离
    r1 = client.post("/messages", json={"message": payload, "partition": partition})
    assert r1.status_code == 201
    # 同文再发（不同 offset）
    r2 = client.post("/messages", json={"message": payload, "partition": partition})
    assert r2.json()[offset_key] == r1.json()[offset_key] + 1

    _wait_until(lambda: len(client.get("/quarantine").json()) >= 2)
    rows = client.get("/quarantine").json()
    positions = {(x["source_partition"], x["source_offset"]) for x in rows}
    assert (r1.json()["partition"], r1.json()["offset"]) in positions
    assert (r2.json()["partition"], r2.json()["offset"]) in positions
