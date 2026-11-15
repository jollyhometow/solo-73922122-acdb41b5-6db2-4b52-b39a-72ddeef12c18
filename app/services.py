"""核心业务服务层。

设计要点：
1. 全流程幂等键 = (topic, source_partition, source_offset)，两张表各加唯一约束；
2. 消费处理采用“先查后插 + 唯一约束兜底 + 捕获 IntegrityError”，重复投递安全；
3. 补正用 SELECT ... FOR UPDATE 行锁串行化，状态机 OPEN -> CORRECTED 只允许翻转一次；
4. 任何失败都回滚事务，隔离记录维持 OPEN（已补正的维持 CORRECTED），不产生脏明细；
5. 迟到/重放的消息若发现该位点已有记录（含已补正），直接跳过，绝不覆盖补正结果。
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import NetWeightRecord, QuarantineRecord, QuarantineStatus
from app.validation import InvalidWeighEvent, ValidWeighEvent, parse_and_validate

logger = logging.getLogger(__name__)


class CorrectionError(Exception):
    """补正被拒绝（业务原因）。"""

    def __init__(self, code: str, detail: str, status_code: int = 409) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


@dataclass
class ConsumptionOutcome:
    """单条消息的处理结果。"""

    action: str  # "NET_RECORD" | "QUARANTINED" | "DUPLICATE_SKIP"
    record_id: int | None = None
    detail: str = ""


def _position_exists(
    session: Session, topic: str, partition: int, offset: int
) -> NetWeightRecord | QuarantineRecord | None:
    """检查该源位点是否已被处理过（任一表命中即视为已处理）。"""
    net = session.execute(
        select(NetWeightRecord).where(
            NetWeightRecord.topic == topic,
            NetWeightRecord.source_partition == partition,
            NetWeightRecord.source_offset == offset,
        )
    ).scalar_one_or_none()
    if net is not None:
        return net
    q = session.execute(
        select(QuarantineRecord).where(
            QuarantineRecord.topic == topic,
            QuarantineRecord.source_partition == partition,
            QuarantineRecord.source_offset == offset,
        )
    ).scalar_one_or_none()
    return q


def process_consumed_message(
    session: Session,
    *,
    topic: str,
    partition: int,
    offset: int,
    raw_text: str,
) -> ConsumptionOutcome:
    """处理一条已消费消息并在同一事务内落库；返回处理结果。

    本函数只负责数据库事务的“工作单元”，由调用方负责消费位点提交，
    从而保证“持久化成功后才推进位点”。
    """
    # 1) 幂等前置检查：重放/迟到消息直接跳过，尤其保护已补正记录不被覆盖
    existing = _position_exists(session, topic, partition, offset)
    if existing is not None:
        logger.info(
            "skip duplicate at %s-%s:%s (existing=%s id=%s)",
            topic, partition, offset, type(existing).__name__, existing.id,
        )
        return ConsumptionOutcome(
            action="DUPLICATE_SKIP",
            record_id=existing.id,
            detail="该源位点已有持久化记录，跳过",
        )

    result = parse_and_validate(raw_text)

    try:
        if isinstance(result, ValidWeighEvent):
            record = NetWeightRecord(
                container_no=result.container_no,
                gross_weight=result.gross_weight,
                tare_weight=result.tare_weight,
                net_weight=result.gross_weight - result.tare_weight,
                topic=topic,
                source_partition=partition,
                source_offset=offset,
                from_correction=False,
            )
            session.add(record)
            session.commit()
            return ConsumptionOutcome(
                action="NET_RECORD", record_id=record.id, detail="净重明细已入仓"
            )

        assert isinstance(result, InvalidWeighEvent)
        quarantined = QuarantineRecord(
            raw_payload=raw_text,
            container_no=result.container_no,
            reason_code=result.reason_code,
            reason_detail=result.reason_detail,
            topic=topic,
            source_partition=partition,
            source_offset=offset,
            status=QuarantineStatus.OPEN,
        )
        session.add(quarantined)
        session.commit()
        return ConsumptionOutcome(
            action="QUARANTINED",
            record_id=quarantined.id,
            detail=f"已隔离：{result.reason_code} {result.reason_detail}",
        )
    except IntegrityError:
        # 并发/重平衡边缘场景下唯一约束兜底：回滚并按重复处理，位点仍可安全推进
        session.rollback()
        existing = _position_exists(session, topic, partition, offset)
        logger.warning(
            "unique-constraint guard at %s-%s:%s -> duplicate skip",
            topic, partition, offset,
        )
        return ConsumptionOutcome(
            action="DUPLICATE_SKIP",
            record_id=existing.id if existing else None,
            detail="唯一约束兜底：该源位点已被其他实例处理，跳过",
        )


def apply_correction(
    session: Session, quarantine_id: int, payload: dict
) -> tuple[QuarantineRecord, NetWeightRecord | None, str | None]:
    """对一条隔离记录应用补正。

    返回 (隔离记录, 生成的净重明细或 None, 拒绝原因或 None)。
    并发补正由行锁串行化：只有第一份合法补正生效，其余看到 CORRECTED 后拒绝；
    任何非法补正只登记尝试信息并提交，隔离状态保持 OPEN。
    """
    q = session.get(QuarantineRecord, quarantine_id, with_for_update=True)
    if q is None:
        raise CorrectionError("NOT_FOUND", f"隔离记录 {quarantine_id} 不存在", 404)

    if q.status == QuarantineStatus.CORRECTED:
        # 只接受一次合法补正：后续尝试直接拒绝，不改动任何数据
        raise CorrectionError(
            "ALREADY_CORRECTED",
            f"隔离记录 {quarantine_id} 已完成补正，每条记录仅接受一次合法补正",
            409,
        )

    # 复用与消费链路完全相同的校验规则，保证口径一致
    validation = parse_and_validate(json.dumps(payload, ensure_ascii=False))
    if isinstance(validation, InvalidWeighEvent):
        q.correction_attempts += 1
        q.last_correction_error = f"{validation.reason_code}: {validation.reason_detail}"
        session.commit()
        logger.info(
            "correction rejected for quarantine %s: %s",
            quarantine_id, q.last_correction_error,
        )
        return q, None, q.last_correction_error

    assert isinstance(validation, ValidWeighEvent)
    try:
        net = NetWeightRecord(
            container_no=validation.container_no,
            gross_weight=validation.gross_weight,
            tare_weight=validation.tare_weight,
            net_weight=validation.gross_weight - validation.tare_weight,
            topic=q.topic,
            source_partition=q.source_partition,
            source_offset=q.source_offset,
            from_correction=True,
            quarantine_id=q.id,
        )
        session.add(net)
        session.flush()  # 先让唯一约束在同一事务内生效

        q.status = QuarantineStatus.CORRECTED
        q.container_no = validation.container_no
        q.correction_attempts += 1
        q.last_correction_error = None
        q.corrected_at = datetime.now(timezone.utc)
        session.commit()
        logger.info(
            "correction accepted for quarantine %s -> net record %s",
            quarantine_id, net.id,
        )
        return q, net, None
    except IntegrityError as exc:
        # 理论上不会发生（该位点在净重表尚无行）；稳妥起见回滚并保持隔离
        session.rollback()
        raise CorrectionError(
            "PERSIST_CONFLICT", f"补正落库冲突，隔离状态保留: {exc.orig}", 409
        ) from exc


# ---------------- 查询 ----------------

def list_net_records(
    session: Session,
    *,
    container_no: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[NetWeightRecord]:
    stmt = select(NetWeightRecord).order_by(
        NetWeightRecord.source_partition, NetWeightRecord.source_offset
    )
    if container_no:
        stmt = stmt.where(NetWeightRecord.container_no == container_no)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.execute(stmt).scalars().all())


def list_quarantine(
    session: Session,
    *,
    partition: int | None = None,
    status: QuarantineStatus | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[QuarantineRecord]:
    stmt = select(QuarantineRecord).order_by(
        QuarantineRecord.source_partition, QuarantineRecord.source_offset
    )
    if partition is not None:
        stmt = stmt.where(QuarantineRecord.source_partition == partition)
    if status is not None:
        stmt = stmt.where(QuarantineRecord.status == status)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.execute(stmt).scalars().all())


def get_quarantine_by_position(
    session: Session, *, topic: str, partition: int, offset: int
) -> QuarantineRecord | None:
    return session.execute(
        select(QuarantineRecord).where(
            QuarantineRecord.topic == topic,
            QuarantineRecord.source_partition == partition,
            QuarantineRecord.source_offset == offset,
        )
    ).scalar_one_or_none()
