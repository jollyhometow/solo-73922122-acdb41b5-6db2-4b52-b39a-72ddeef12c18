"""消费工作线程：把消息处理、数据库事务与位点提交严格排序。

提交顺序（每条消息）：
  1. 处理消息并完成数据库事务（净重明细或隔离记录持久化）；
  2. 成功 -> store_offset（仅内存标记，未与 Broker 交互）；
  3. 每处理一条同步 commit 一次位点（演示流量小，优先正确性）；
  4. 任一步失败 -> 不推进位点，seek 回本条消息后重试。

崩溃语义：
  - 步骤 1 成功、步骤 3 未完成时进程退出：重启后 Broker 重放该消息，
    数据库唯一键识别为重复并跳过，随后位点正常提交 -> 不丢、不重；
  - 重平衡收回分区：未提交的位点不会被其他实例确认，新 owner 从旧位点重放，
    同样由幂等键去重。
"""
from __future__ import annotations

import logging
import threading
from typing import Callable

from sqlalchemy.exc import SQLAlchemyError

from app.config import get_settings
from app.database import get_database
from app.messaging import Broker, BrokerMessage, get_broker
from app.services import process_consumed_message

logger = logging.getLogger(__name__)


class ConsumerWorker:
    def __init__(
        self,
        broker: Broker | None = None,
        session_factory: Callable | None = None,
    ) -> None:
        self._settings = get_settings()
        self._broker = broker or get_broker()
        db = get_database()
        self._session_factory = session_factory or db.session
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._consumer = None
        self.assigned_partitions: list[int] = []
        self._last_message: BrokerMessage | None = None
        self._consecutive_failures = 0

    # ---------------- 生命周期 ----------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._consumer = self._broker.subscribe(
            self._settings.topic,
            self._settings.consumer_group,
            self._settings.instance_id,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._thread = threading.Thread(
            target=self._run, name=f"consumer-{self._settings.instance_id}", daemon=True
        )
        self._thread.start()
        # 等待线程进入主循环，消除“启动竞态”导致的测试抖动
        self._ready.wait(timeout=5.0)
        logger.info("consumer worker started (instance=%s group=%s)",
                    self._settings.instance_id, self._settings.consumer_group)

    def stop(self, timeout: float = 15.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._consumer is not None:
            self._consumer.close()
        logger.info("consumer worker stopped")

    # ---------------- 回调 ----------------

    def _on_assign(self, partitions: list[int]) -> None:
        self.assigned_partitions = partitions
        logger.info("[%s] 分配到分区 %s", self._settings.instance_id, partitions)

    def _on_revoke(self, partitions: list[int]) -> None:
        logger.warning("[%s] 分区被收回 %s", self._settings.instance_id, partitions)
        self.assigned_partitions = [
            p for p in self.assigned_partitions if p not in partitions
        ]
        # 丢弃内存中尚未提交的位点记忆：新 owner 会从 Broker 已提交位点重放
        self._last_message = None

    # ---------------- 主循环 ----------------

    def _run(self) -> None:
        s = self._settings
        self._ready.set()
        while not self._stop.is_set():
            try:
                message = self._consumer.poll(s.poll_timeout_seconds)
            except Exception:
                logger.exception("poll 失败，%ss 后重试", s.poll_timeout_seconds)
                self._wait(s.poll_timeout_seconds)
                continue

            if message is None:
                continue

            self._last_message = message
            self._handle_one(message)

    def _handle_one(self, message: BrokerMessage) -> None:
        try:
            raw_text = self._decode(message.value)
        except Exception:
            logger.exception("消息解码失败，进入无限重试（不会丢消息）")
            self._retry_after_seek(message)
            return

        session = self._session_factory()
        try:
            outcome = process_consumed_message(
                session,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                raw_text=raw_text,
            )
        except SQLAlchemyError:
            # 写库失败：事务未成功，绝不能提交位点；回滚后 seek 重试
            session.rollback()
            logger.exception(
                "写库失败 %s-%s:%s，将重试",
                message.topic, message.partition, message.offset,
            )
            self._retry_after_seek(message)
            return
        except Exception:
            session.rollback()
            logger.exception("未预期错误，位点不推进并重试")
            self._retry_after_seek(message)
            return
        finally:
            # process_consumed_message 正常路径已自行 commit/rollback；
            # 异常路径已 rollback。统一在此关闭，防止连接泄漏或重复关闭。
            session.close()

        logger.info(
            "processed %s-%s:%s action=%s id=%s detail=%s",
            message.topic, message.partition, message.offset,
            outcome.action, outcome.record_id, outcome.detail,
        )

        # 数据库持久化成功后才触碰位点
        try:
            self._consumer.store_offset(message)
            self._consumer.commit()
        except Exception:
            # 提交位点时断连：seek 回本条消息，重放后由唯一键去重
            logger.exception(
                "位点提交失败 %s-%s:%s，seek 后等待重放",
                message.topic, message.partition, message.offset,
            )
            try:
                self._consumer.seek(message)
            except Exception:
                logger.exception("seek 也失败，依赖重启/重平衡重放兜底")
            self._consecutive_failures += 1
            # 退避，避免故障期空转打爆 Broker
            self._wait(min(30.0, 0.5 * 2 ** min(self._consecutive_failures, 6)))
            return

        self._consecutive_failures = 0
        self._last_message = None

    def _retry_after_seek(self, message: BrokerMessage) -> None:
        try:
            self._consumer.seek(message)
        except Exception:
            logger.exception("seek 失败，等待重平衡/重启重放")
        self._consecutive_failures += 1
        self._wait(min(30.0, 0.5 * 2 ** min(self._consecutive_failures, 6)))

    def _decode(self, value: bytes) -> str:
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                # 保留可打印转义原文，仍走隔离（NOT_JSON）
                return value.decode("utf-8", errors="replace")
        return str(value)

    def _wait(self, seconds: float) -> None:
        self._stop.wait(seconds)
