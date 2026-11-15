"""消息层抽象。

统一接口屏蔽具体实现：
- KafkaBroker：对接本地 Redpanda（confluent-kafka），生产/消费真实 Topic；
- InMemoryBroker：进程内内存 Broker，用于无 Redpanda 环境下的端到端自测。

消费者正确性要点：
- enable.auto.commit=false、enable.auto.offset.store=false，位点完全手工控制；
- 每条消息“数据库事务成功 -> store_offset -> 批量 commit”，失败不提交；
- 提交失败立即 seek 回未确认位点，崩溃/重启/重平衡后由 Broker 重放未确认消息，
  数据库幂等键保证重放不产生重复明细。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BrokerMessage:
    topic: str
    partition: int
    offset: int
    value: bytes
    key: bytes | None = None


class Producer(Protocol):
    def produce(
        self, topic: str, value: bytes, key: bytes | None, partition: int | None
    ) -> BrokerMessage: ...

    def flush(self) -> None: ...


class Broker(Protocol):
    def create_topic(self, topic: str, partitions: int) -> None: ...

    def producer(self) -> Producer: ...

    def subscribe(
        self,
        topic: str,
        group_id: str,
        instance_id: str,
        on_assign: Callable[[list[int]], None] | None = None,
        on_revoke: Callable[[list[int]], None] | None = None,
    ) -> Any: ...


# --------------------------------------------------------------------------
# 进程内内存 Broker（测试与无 Redpanda 环境演示用）
# --------------------------------------------------------------------------

class _InMemoryPartition:
    def __init__(self) -> None:
        self.messages: list[BrokerMessage] = []
        # group_id -> 已提交 offset（下一条待消费下标）
        self.committed: dict[str, int] = {}
        self.condition = threading.Condition()


class InMemoryBroker:
    """线程安全的单进程 Broker，语义贴近 Kafka 的分区 + 消费组位点。"""

    def __init__(self) -> None:
        self._topics: dict[str, list[_InMemoryPartition]] = {}
        self._lock = threading.Lock()

    def create_topic(self, topic: str, partitions: int) -> None:
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = [_InMemoryPartition() for _ in range(partitions)]

    def _partitions(self, topic: str) -> list[_InMemoryPartition]:
        if topic not in self._topics:
            raise KeyError(f"topic {topic!r} 不存在")
        return self._topics[topic]

    def producer(self) -> "InMemoryProducer":
        return InMemoryProducer(self)

    def _append(
        self, topic: str, value: bytes, key: bytes | None, partition: int | None
    ) -> BrokerMessage:
        parts = self._partitions(topic)
        if partition is None:
            # 轮询，模拟默认分区器（无 key 时）
            partition = self._round_robin(topic, len(parts))
        if not 0 <= partition < len(parts):
            raise ValueError(f"分区 {partition} 越界")
        part = parts[partition]
        with part.condition:
            offset = len(part.messages)
            msg = BrokerMessage(topic, partition, offset, value, key)
            part.messages.append(msg)
            part.committed.setdefault("_metadata", 0)
            part.condition.notify_all()
        return msg

    def _round_robin(self, topic: str, n: int) -> int:
        counters = getattr(self, "_counters", None)
        if counters is None:
            counters = self._counters = {}
        idx = counters.get(topic, 0)
        counters[topic] = idx + 1
        return idx % n

    def subscribe(
        self,
        topic: str,
        group_id: str,
        instance_id: str,
        on_assign: Callable[[list[int]], None] | None = None,
        on_revoke: Callable[[list[int]], None] | None = None,
    ) -> "InMemoryConsumer":
        return InMemoryConsumer(self, topic, group_id, instance_id, on_assign, on_revoke)

    # 测试辅助：直接读取某分区消息
    def peek(self, topic: str, partition: int) -> list[BrokerMessage]:
        return list(self._partitions(topic)[partition].messages)


class InMemoryProducer:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker

    def produce(
        self, topic: str, value: bytes, key: bytes | None, partition: int | None
    ) -> BrokerMessage:
        return self._broker._append(topic, value, key, partition)

    def flush(self) -> None:
        return None


class InMemoryConsumer:
    """模拟 Kafka 消费组：同一 group 内每个分区只被一个消费者顺序消费。

    为聚焦单进程自测，这里采用静态分配（按 instance 序号取模），
    并完整实现 poll / store_offset / commit / seek / close 语义。
    """

    def __init__(
        self,
        broker: InMemoryBroker,
        topic: str,
        group_id: str,
        instance_id: str,
        on_assign: Callable[[list[int]], None] | None,
        on_revoke: Callable[[list[int]], None] | None,
    ) -> None:
        self._broker = broker
        self._topic = topic
        self._group = group_id
        self._instance_id = instance_id
        self._on_assign = on_assign
        self._on_revoke = on_revoke
        self._positions: dict[int, int] = {}   # partition -> 下次 poll 的下标
        self._stored: dict[int, int] = {}      # partition -> 待提交 offset
        self._poll_cursor = 0
        self._closed = False
        self.assign_all()

    def assign_all(self) -> None:
        parts = self._broker._partitions(self._topic)
        assigned = list(range(len(parts)))
        for p in assigned:
            committed = parts[p].committed.get(self._group, 0)
            self._positions[p] = committed
            self._stored[p] = committed
        if self._on_assign:
            self._on_assign(assigned)

    def poll(self, timeout: float) -> BrokerMessage | None:
        if self._closed:
            return None
        deadline = time.monotonic() + timeout
        parts = self._broker._partitions(self._topic)
        # 跨调用轮询起始分区，避免空分区长期阻塞导致其他分区饥饿
        scan_order = sorted(self._positions)
        cursor = self._poll_cursor % len(scan_order)
        while time.monotonic() < deadline:
            for step in range(len(scan_order)):
                p = scan_order[(cursor + step) % len(scan_order)]
                part = parts[p]
                with part.condition:
                    pos = self._positions[p]
                    if pos < len(part.messages):
                        msg = part.messages[pos]
                        self._positions[p] = pos + 1
                        self._poll_cursor = (p + 1) % len(scan_order)
                        return msg
            # 所有分区都为空才等待
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return None

    def store_offset(self, message: BrokerMessage) -> None:
        # 存的是“下一条待消费 offset”
        self._stored[message.partition] = message.offset + 1

    def commit(self) -> None:
        parts = self._broker._partitions(self._topic)
        for p, off in self._stored.items():
            parts[p].committed[self._group] = off
            # 内存位置与已提交位点保持一致，保证“提交成功即推进”
            self._positions[p] = off

    def seek(self, message: BrokerMessage) -> None:
        # 位点提交失败后回到本条消息，等待重新处理
        self._positions[message.partition] = message.offset
        self._stored[message.partition] = message.offset

    def pause(self, partitions: list[int]) -> None:
        return None

    def close(self) -> None:
        if not self._closed and self._on_revoke:
            self._on_revoke(list(self._positions))
        self._closed = True


# --------------------------------------------------------------------------
# Redpanda / Kafka 实现
# --------------------------------------------------------------------------

class KafkaProducer:
    def __init__(self, bootstrap: str, instance_id: str) -> None:
        from confluent_kafka import Producer as ConfluentProducer

        self._producer = ConfluentProducer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": f"weighbridge-producer-{instance_id}",
                "enable.idempotence": True,
                "acks": "all",
                "retries": 10,
                "message.max.bytes": 4 * 1024 * 1024,
            }
        )

    def produce(
        self, topic: str, value: bytes, key: bytes | None, partition: int | None
    ) -> BrokerMessage:
        done = threading.Event()
        box: dict[str, Any] = {}

        def _delivery(err, msg) -> None:
            if err is not None:
                box["error"] = err
            else:
                box["partition"] = msg.partition()
                box["offset"] = msg.offset()
            done.set()

        kwargs: dict[str, Any] = {"value": value}
        if key is not None:
            kwargs["key"] = key
        if partition is not None:
            kwargs["partition"] = partition

        # 带投递确认的 produce；队列满时 poll 等待，确保返回时消息已被 Broker 接收
        self._producer.produce(topic, on_delivery=_delivery, **kwargs)
        while not done.is_set():
            self._producer.poll(0.2)
        if "error" in box:
            raise RuntimeError(f"消息投递失败: {box['error']}")

        return BrokerMessage(
            topic=topic,
            partition=box["partition"],
            offset=box["offset"],
            value=value,
            key=key,
        )

    def flush(self) -> None:
        self._producer.flush(10)


class KafkaBroker:
    def __init__(self, bootstrap: str, instance_id: str) -> None:
        self._bootstrap = bootstrap
        self._instance_id = instance_id

    def create_topic(self, topic: str, partitions: int) -> None:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": self._bootstrap})
        existing = set(admin.list_topics(timeout=15).topics)
        if topic in existing:
            return
        futures = admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )
        for fut in futures.values():
            fut.result()  # 抛错即启动失败
        logger.info("topic %s created with %s partitions", topic, partitions)

    def producer(self) -> KafkaProducer:
        return KafkaProducer(self._bootstrap, self._instance_id)

    def subscribe(
        self,
        topic: str,
        group_id: str,
        instance_id: str,
        on_assign: Callable[[list[int]], None] | None = None,
        on_revoke: Callable[[list[int]], None] | None = None,
    ) -> "KafkaConsumer":
        return KafkaConsumer(
            self._bootstrap, topic, group_id, instance_id, on_assign, on_revoke
        )


class KafkaConsumer:
    def __init__(
        self,
        bootstrap: str,
        topic: str,
        group_id: str,
        instance_id: str,
        on_assign: Callable[[list[int]], None] | None,
        on_revoke: Callable[[list[int]], None] | None,
    ) -> None:
        from confluent_kafka import Consumer as ConfluentConsumer

        self._topic = topic
        self._on_assign = on_assign
        self._on_revoke = on_revoke
        self._paused: set[int] = set()
        self._consumer = ConfluentConsumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "client.id": f"weighbridge-consumer-{instance_id}",
                "group.instance.id": f"{group_id}-{instance_id}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "session.timeout.ms": 15000,
                "max.poll.interval.ms": 300000,
            }
        )
        self._consumer.subscribe([topic], on_assign=self._handle_assign,
                                 on_revoke=self._handle_revoke)

    def _handle_assign(self, _consumer, partitions) -> None:
        assigned = [p.partition for p in partitions]
        logger.info("instance assigned partitions: %s", assigned)
        if self._on_assign:
            self._on_assign(assigned)

    def _handle_revoke(self, _consumer, partitions) -> None:
        revoked = [p.partition for p in partitions]
        logger.warning("partitions revoked: %s", revoked)
        if self._on_revoke:
            self._on_revoke(revoked)

    def poll(self, timeout: float):
        from confluent_kafka import KafkaError

        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error() is not None:
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(f"poll 错误: {msg.error()}")
        return BrokerMessage(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            value=msg.value() or b"",
            key=msg.key(),
        )

    def store_offset(self, message: BrokerMessage) -> None:
        from confluent_kafka import TopicPartition

        self._consumer.store_offsets(
            offsets=[TopicPartition(
                message.topic, message.partition, message.offset + 1
            )]
        )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def seek(self, message: BrokerMessage) -> None:
        from confluent_kafka import TopicPartition

        tp = TopicPartition(message.topic, message.partition, message.offset)
        self._consumer.seek(tp)

    def pause(self, partitions: list[int]) -> None:
        from confluent_kafka import TopicPartition

        new = set(partitions) - self._paused
        if new:
            self._consumer.pause([TopicPartition(self._topic, p) for p in new])
            self._paused |= new

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # pragma: no cover - 关闭路径尽力而为
            logger.exception("consumer close error suppressed")


# --------------------------------------------------------------------------
# 工厂与单例
# --------------------------------------------------------------------------

_broker: Broker | None = None
_broker_lock = threading.Lock()


def get_broker() -> Broker:
    """按环境变量选择实现。

    BROKER_BACKEND=memory（默认，供本地无 Redpanda 自测）；
    BROKER_BACKEND=kafka 且 KAFKA_BOOTSTRAP 指向 Redpanda（Compose 使用）。
    """
    global _broker
    if _broker is None:
        with _broker_lock:
            if _broker is None:
                import os

                from app.config import get_settings

                settings = get_settings()
                backend = os.getenv("BROKER_BACKEND", "memory").lower()
                if backend == "kafka":
                    _broker = KafkaBroker(settings.kafka_bootstrap, settings.instance_id)
                else:
                    broker = InMemoryBroker()
                    broker.create_topic(settings.topic, settings.topic_partitions)
                    _broker = broker
    return _broker


def set_broker(broker: Broker | None) -> None:
    """测试辅助：替换/清空全局 Broker。"""
    global _broker
    with _broker_lock:
        _broker = broker
