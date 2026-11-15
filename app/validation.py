"""称量消息的纯函数校验。

规则：
- 消息体必须是 JSON 对象；
- 箱号为非空字符串，长度 4~17，仅允许大写/小写字母、数字与连字符；
- 毛重、皮重必须是整数千克（布尔、浮点、字符串数字均拒绝），非负且不超上限；
- 毛重必须 >= 皮重，否则隔离。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from app.config import get_settings

# 箱号：字母数字与连字符（覆盖 ISO 6346 箱号及内部编号场景）
_CONTAINER_RE = re.compile(r"^[A-Za-z0-9-]+$")


class ReasonCode:
    NOT_JSON = "NOT_JSON"
    NOT_OBJECT = "NOT_OBJECT"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_TYPE = "INVALID_TYPE"
    INVALID_CONTAINER_NO = "INVALID_CONTAINER_NO"
    WEIGHT_OUT_OF_RANGE = "WEIGHT_OUT_OF_RANGE"
    GROSS_LT_TARE = "GROSS_LT_TARE"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass(frozen=True)
class ValidWeighEvent:
    container_no: str
    gross_weight: int
    tare_weight: int


@dataclass(frozen=True)
class InvalidWeighEvent:
    reason_code: str
    reason_detail: str
    container_no: str = ""


def _is_strict_int(value: Any) -> bool:
    """只接受真正的 int；bool 是 int 子类，必须排除；float/str 一律拒绝。"""
    return isinstance(value, int) and not isinstance(value, bool)


def parse_and_validate(raw_text: str) -> ValidWeighEvent | InvalidWeighEvent:
    """把原始消息文本解析并校验，返回合法事件或隔离原因。"""
    settings = get_settings()

    try:
        payload = json.loads(raw_text)
    except (json.JSONDecodeError, TypeError) as exc:
        return InvalidWeighEvent(
            ReasonCode.NOT_JSON, f"消息不是合法 JSON: {exc.msg if isinstance(exc, json.JSONDecodeError) else exc}"
        )

    if not isinstance(payload, dict):
        return InvalidWeighEvent(
            ReasonCode.NOT_OBJECT, f"消息体必须是 JSON 对象，实际为 {type(payload).__name__}"
        )

    container_no = payload.get("container_no")
    if container_no is None:
        return InvalidWeighEvent(ReasonCode.MISSING_FIELD, "缺少字段 container_no")
    if not isinstance(container_no, str):
        return InvalidWeighEvent(
            ReasonCode.INVALID_TYPE,
            f"container_no 必须是字符串，实际为 {type(container_no).__name__}",
        )

    container_no = container_no.strip()
    if not (
        settings.container_no_min_len <= len(container_no) <= settings.container_no_max_len
        and _CONTAINER_RE.fullmatch(container_no) is not None
    ):
        return InvalidWeighEvent(
            ReasonCode.INVALID_CONTAINER_NO,
            f"箱号格式非法：{container_no!r}，需 {settings.container_no_min_len}-"
            f"{settings.container_no_max_len} 位字母/数字/连字符",
            container_no=container_no,
        )

    for field in ("gross_weight", "tare_weight"):
        if field not in payload:
            return InvalidWeighEvent(
                ReasonCode.MISSING_FIELD, f"缺少字段 {field}", container_no=container_no
            )
        value = payload[field]
        if not _is_strict_int(value):
            return InvalidWeighEvent(
                ReasonCode.INVALID_TYPE,
                f"{field} 必须是整数千克，实际为 {type(value).__name__}: {value!r}",
                container_no=container_no,
            )
        if value < 0 or value > settings.max_weight_kg:
            return InvalidWeighEvent(
                ReasonCode.WEIGHT_OUT_OF_RANGE,
                f"{field}={value} 超出允许范围 [0, {settings.max_weight_kg}]",
                container_no=container_no,
            )

    gross_weight: int = payload["gross_weight"]
    tare_weight: int = payload["tare_weight"]
    if gross_weight < tare_weight:
        return InvalidWeighEvent(
            ReasonCode.GROSS_LT_TARE,
            f"毛重 {gross_weight} 小于皮重 {tare_weight}，净重为负",
            container_no=container_no,
        )

    return ValidWeighEvent(
        container_no=container_no,
        gross_weight=gross_weight,
        tare_weight=tare_weight,
    )
