"""消费者可靠性测试（InMemoryBroker + SQLite）。

覆盖：
1. 正常处理后位点推进；
2. 写库失败 -> 位点不推进 -> 恢复后重试成功，不丢不重；
3. 位点提交失败 -> seek 重放 -> 唯一键去重，不产生重复明细；
4. 新消费者实例从已提交位点接续（模拟实例退出/重平衡）。
"""
from __future__ import annotations

import time

import pytest
from sqlalchemy import func, select

from app import database as db_module
from app.consumer import ConsumerWorker
from app.messaging import InMemoryBroker, set_broker
from app.models import NetWeightRecord, QuarantineRecord

from app.config import get_settings

TOPIC = "weighbridge-events"
GROUP = get_settings().consumer_group
VALID = '{"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000}'
BAD = '{"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200}'


@pytest.fixture()
def engine_broker(tmp_path):
    # 临时文件库（WAL），避免内存库 StaticPool 跨线程可见性抖动
    from tests.conftest import make_file_engine_factory

    engine, factory, url = make_file_engine_factory(tmp_path)

    singleton = db_module.Database.__new__(db_module.Database)
    singleton.url = url
    singleton.engine = engine
    singleton._session_factory = factory
    db_module._db = singleton

    broker = InMemoryBroker()
    broker.create_topic(TOPIC, 2)
    set_broker(broker)
    created: list[ConsumerWorker] = []

    # 包装构造器，确保夹具回收时所有 worker 线程都被停掉
    real_init = ConsumerWorker.__init__

    def tracking_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        created.append(self)

    ConsumerWorker.__init__ = tracking_init
    try:
        yield factory, broker
    finally:
        ConsumerWorker.__init__ = real_init
        for w in created:
            try:
                w.stop(timeout=3)
            except Exception:
                pass
        set_broker(None)
        db_module._db = None
        engine.dispose()


def _wait_count(factory, model, expected, timeout=5.0):
    """等待行数 >= expected（消费者可能在一次轮询间隙处理多条，不能用严格相等）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with factory() as s:
            n = s.scalar(select(func.count()).select_from(model))
        if n is not None and n >= expected:
            return n
        time.sleep(0.03)
    raise AssertionError(f"等待 {model.__name__} 行数>={expected} 超时")


def test_happy_path_commits_offsets(engine_broker):
    factory, broker = engine_broker
    producer = broker.producer()
    m1 = producer.produce(TOPIC, VALID.encode(), None, 0)
    m2 = producer.produce(TOPIC, BAD.encode(), None, 1)

    worker = ConsumerWorker(broker=broker, session_factory=factory)
    worker.start()
    try:
        _wait_count(factory, NetWeightRecord, 1)
        _wait_count(factory, QuarantineRecord, 1)
    finally:
        worker.stop()

    # 位点已提交到 group
    parts = broker._partitions(TOPIC)
    assert parts[0].committed[GROUP] >= m1.offset + 1
    assert parts[1].committed[GROUP] >= m2.offset + 1


def test_db_failure_blocks_offset_then_resumes(engine_broker, monkeypatch):
    factory, broker = engine_broker
    producer = broker.producer()
    msg = producer.produce(TOPIC, VALID.encode(), None, 0)

    # 让前两次写库抛错，第三次恢复
    state = {"failures_left": 2}
    original = factory.__call__ if hasattr(factory, "__call__") else None

    class FlakySession:
        def __init__(self, real):
            self._real = real
            self.is_active = True

        def __getattr__(self, item):
            return getattr(self._real, item)

        def commit(self):
            if state["failures_left"] > 0:
                state["failures_left"] -= 1
                from sqlalchemy.exc import OperationalError

                raise OperationalError("INSERT", {}, Exception("simulated db outage"))
            return self._real.commit()

    def flaky_factory():
        return FlakySession(factory())

    worker = ConsumerWorker(broker=broker, session_factory=flaky_factory)
    # 加速退避
    monkeypatch.setattr(worker, "_wait", lambda seconds: time.sleep(0.01))
    worker.start()
    try:
        _wait_count(factory, NetWeightRecord, 1, timeout=8)
    finally:
        worker.stop()

    with factory() as s:
        rows = s.scalars(select(NetWeightRecord)).all()
        assert len(rows) == 1  # 失败重试没有产生重复
        assert rows[0].source_offset == msg.offset
    # 最终位点必须推进到该消息之后
    assert broker._partitions(TOPIC)[0].committed[GROUP] == msg.offset + 1


def test_commit_failure_replay_is_deduplicated(engine_broker, monkeypatch):
    factory, broker = engine_broker
    producer = broker.producer()
    msg = producer.produce(TOPIC, VALID.encode(), None, 0)

    # 包装 broker：它创建出的每个 consumer，第一次 commit 必失败，确定性模拟断连
    real_subscribe = broker.subscribe
    calls = {"n": 0}

    def subscribe_once_failing(*args, **kwargs):
        consumer = real_subscribe(*args, **kwargs)
        original_commit = consumer.commit

        def flaky_commit():
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("broker disconnected during commit")
            return original_commit()

        consumer.commit = flaky_commit
        return consumer

    monkeypatch.setattr(broker, "subscribe", subscribe_once_failing)

    worker = ConsumerWorker(broker=broker, session_factory=factory)
    monkeypatch.setattr(worker, "_wait", lambda seconds: time.sleep(0.01))
    worker.start()
    try:
        _wait_count(factory, NetWeightRecord, 1, timeout=8)
        # 多等一个处理周期，确认重放被幂等跳过而非重复入库
        time.sleep(0.3)
    finally:
        worker.stop()

    assert calls["n"] >= 1  # 断连路径确实被触发
    with factory() as s:
        assert s.scalar(select(func.count()).select_from(NetWeightRecord)) == 1
    # 最终位点一致
    assert broker._partitions(TOPIC)[0].committed[GROUP] == msg.offset + 1


def test_new_worker_resumes_from_committed_offset(engine_broker):
    """模拟实例退出：第一个 worker 处理一条后停止，第二个从已提交位点接续且不重复。"""
    factory, broker = engine_broker
    producer = broker.producer()
    # 第一阶段只有一条消息，保证 worker1 退出前恰好处理它
    first = producer.produce(TOPIC, VALID.encode(), None, 0)

    worker1 = ConsumerWorker(broker=broker, session_factory=factory)
    worker1.start()
    try:
        _wait_count(factory, NetWeightRecord, 1, timeout=5)
    finally:
        worker1.stop()  # 模拟实例退出

    # 已提交位点应精确包含第一条
    assert broker._partitions(TOPIC)[0].committed[GROUP] == first.offset + 1

    # 实例退出后第二条消息才到达
    second = producer.produce(TOPIC, VALID.encode(), None, 0)

    # 新实例（同 group）接续：从已提交 offset=1 开始，只处理第二条
    worker2 = ConsumerWorker(broker=broker, session_factory=factory)
    worker2.start()
    try:
        _wait_count(factory, NetWeightRecord, 2, timeout=8)
    finally:
        worker2.stop()

    with factory() as s:
        rows = s.scalars(select(NetWeightRecord)).all()
        offsets = sorted(x.source_offset for x in rows)
        # 同文不同位置分别保留、无重复
        assert len(rows) == 2
    assert offsets == [first.offset, second.offset]
    assert broker._partitions(TOPIC)[0].committed[GROUP] == second.offset + 1
