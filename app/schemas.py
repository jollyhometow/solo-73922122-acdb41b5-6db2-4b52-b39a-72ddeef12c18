"""HTTP 接口的 Pydantic 模型。

刻意保持宽松：演示消息接口接受任意 JSON 对象（甚至畸形 JSON 原文），
以便把“坏消息”真正送进流水线验证隔离能力。
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from app.models import QuarantineStatus


class ProduceMessageRequest(BaseModel):
    """演示生产请求：消息内容由调用方自由构造。"""

    message: dict | list | str | int | float | bool | None = Field(
        default=None,
        description="消息体，将以 JSON 序列化后投递；典型为 "
        '{"container_no": "ABCD1234567", "gross_weight": 20000, "tare_weight": 4000}',
    )
    raw_text: str | None = Field(
        default=None,
        description="直接投递的原始文本（与 message 二选一），可用于投递畸形 JSON",
    )
    partition: int | None = Field(default=None, ge=0, description="可选，指定目标分区")
    key: str | None = Field(default=None, description="可选，消息 key（默认取箱号）")

    @field_validator("message", "raw_text")
    @classmethod
    def _not_both_empty(cls, v, info):  # pragma: no cover - 声明性校验
        return v


class ProducedMessage(BaseModel):
    topic: str
    partition: int
    offset: int
    key: str | None = None
    delivered_text: str


class CorrectionRequest(BaseModel):
    """对隔离记录的补正：只接受三个业务字段的合法值。"""

    container_no: str = Field(..., examples=["ABCD1234567"])
    gross_weight: int = Field(..., ge=0, examples=[20000])
    tare_weight: int = Field(..., ge=0, examples=[4000])


class NetWeightOut(BaseModel):
    id: int
    container_no: str
    gross_weight: int
    tare_weight: int
    net_weight: int
    topic: str
    source_partition: int
    source_offset: int
    from_correction: bool
    quarantine_id: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


class QuarantineOut(BaseModel):
    id: int
    raw_payload: str
    container_no: str
    reason_code: str
    reason_detail: str
    topic: str
    source_partition: int
    source_offset: int
    status: QuarantineStatus
    last_correction_error: str | None
    correction_attempts: int
    corrected_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CorrectionResult(BaseModel):
    accepted: bool
    quarantine_id: int
    status: QuarantineStatus
    reason: str | None = None
    net_weight: NetWeightOut | None = None


class HealthOut(BaseModel):
    status: str
    instance_id: str
    consumer_enabled: bool
    assigned_partitions: list[int] = Field(default_factory=list)
