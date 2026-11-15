"""ORM 模型：净重明细、隔离记录。

幂等键 (topic, source_partition, source_offset) 在两张表上各自唯一，
与消费位点的手工提交配合，实现“每条源消息仅入仓一次”。
"""
from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

# SQLite 仅允许 INTEGER PRIMARY KEY 自增，生产 PostgreSQL 仍使用 BIGINT
PKBigInt = BigInteger().with_variant(Integer(), "sqlite")


class QuarantineStatus(str, enum.Enum):
    OPEN = "OPEN"          # 仍在隔离，等待合法补正
    CORRECTED = "CORRECTED"  # 已接受唯一一次合法补正并生成净重明细


class NetWeightRecord(Base):
    """净重明细（入仓事实表，只追加）。"""

    __tablename__ = "net_weight_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_net_weight_source_position",
        ),
        CheckConstraint("gross_weight >= 0", name="ck_net_gross_non_negative"),
        CheckConstraint("tare_weight >= 0", name="ck_net_tare_non_negative"),
        CheckConstraint("gross_weight >= tare_weight", name="ck_net_gross_ge_tare"),
        CheckConstraint("net_weight >= 0", name="ck_net_weight_non_negative"),
        Index("ix_net_weight_container_no", "container_no"),
        Index("ix_net_weight_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    gross_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tare_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源定位
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源标记：消费者正常处理 / 隔离补正
    from_correction: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # 补正成功时关联的隔离记录 id；正常路径为空
    quarantine_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class QuarantineRecord(Base):
    """隔离记录：无效消息的原文、错误原因与补正状态。"""

    __tablename__ = "quarantine_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_quarantine_source_position",
        ),
        Index("ix_quarantine_status", "status"),
        Index("ix_quarantine_partition_offset", "source_partition", "source_offset"),
        Index("ix_quarantine_container_no", "container_no"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)

    # 原文原样保留（UTF-8 解码失败时保留可打印转义），同文不同位置分别保留
    raw_payload: Mapped[str] = mapped_column(String, nullable=False)
    # 解析出的箱号（无法解析时为空串），便于检索
    container_no: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_detail: Mapped[str] = mapped_column(String(512), nullable=False)

    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    status: Mapped[QuarantineStatus] = mapped_column(
        Enum(QuarantineStatus, name="quarantine_status"),
        nullable=False,
        default=QuarantineStatus.OPEN,
    )
    # 最近一次失败补正的原因（成功后清空思路：保留最后错误，便于审计）
    last_correction_error: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    correction_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    corrected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
