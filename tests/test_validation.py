"""校验纯函数测试：覆盖各隔离原因与合法路径。"""
from __future__ import annotations

from app.validation import (
    ReasonCode,
    InvalidWeighEvent,
    ValidWeighEvent,
    parse_and_validate,
)


def test_valid_event():
    r = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000}'
    )
    assert isinstance(r, ValidWeighEvent)
    assert r.container_no == "MSCU1234567"
    assert r.gross_weight == 20000


def test_malformed_json():
    r = parse_and_validate("{not json")
    assert isinstance(r, InvalidWeighEvent) and r.reason_code == ReasonCode.NOT_JSON


def test_payload_not_object():
    r = parse_and_validate("[1,2,3]")
    assert r.reason_code == ReasonCode.NOT_OBJECT


def test_missing_container():
    r = parse_and_validate('{"gross_weight": 1, "tare_weight": 1}')
    assert r.reason_code == ReasonCode.MISSING_FIELD


def test_container_too_short_and_bad_chars():
    r = parse_and_validate('{"container_no": "AB", "gross_weight": 1, "tare_weight": 1}')
    assert r.reason_code == ReasonCode.INVALID_CONTAINER_NO
    r2 = parse_and_validate(
        '{"container_no": "BAD CONTAINER", "gross_weight": 1, "tare_weight": 1}'
    )
    assert r2.reason_code == ReasonCode.INVALID_CONTAINER_NO


def test_float_and_bool_rejected():
    r = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": 1.5, "tare_weight": 1}'
    )
    assert r.reason_code == ReasonCode.INVALID_TYPE
    r2 = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": true, "tare_weight": 1}'
    )
    assert r2.reason_code == ReasonCode.INVALID_TYPE
    r3 = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": "100", "tare_weight": 1}'
    )
    assert r3.reason_code == ReasonCode.INVALID_TYPE


def test_negative_and_missing_weight():
    r = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": -1, "tare_weight": 1}'
    )
    assert r.reason_code == ReasonCode.WEIGHT_OUT_OF_RANGE
    r2 = parse_and_validate('{"container_no": "MSCU1234567", "gross_weight": 100}')
    assert r2.reason_code == ReasonCode.MISSING_FIELD


def test_gross_less_than_tare():
    r = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": 3000, "tare_weight": 4200}'
    )
    assert r.reason_code == ReasonCode.GROSS_LT_TARE
    assert r.container_no == "MSCU1234567"


def test_gross_equals_tare_is_valid_zero_net():
    r = parse_and_validate(
        '{"container_no": "MSCU1234567", "gross_weight": 4000, "tare_weight": 4000}'
    )
    assert isinstance(r, ValidWeighEvent)
