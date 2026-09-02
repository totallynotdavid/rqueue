"""Resource bounds from REQUIREMENTS.md §8."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from rqueue.errors import ValidationError
from rqueue.limits import (
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_METADATA_BYTES,
    MAX_PAYLOAD_BYTES,
    QUEUE_WILDCARD,
    truncate,
    validate_identifier,
    validate_key,
    validate_metadata,
    validate_name,
    validate_payload,
    validate_queue_target,
    validate_scheduled_at,
)


def test_payload_must_be_json() -> None:
    assert validate_payload({"a": 1}) == '{"a":1}'
    with pytest.raises(ValidationError):
        validate_payload(lambda: None)
    with pytest.raises(ValidationError):
        validate_payload(object())
    with pytest.raises(ValidationError):
        validate_payload(float("nan"))


def test_payload_size_is_bounded() -> None:
    with pytest.raises(ValidationError, match="over the"):
        validate_payload({"blob": "x" * (MAX_PAYLOAD_BYTES + 1)})


def test_metadata_must_be_an_object_and_bounded() -> None:
    assert validate_metadata(None) == "{}"
    with pytest.raises(ValidationError):
        validate_metadata([1, 2, 3])  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        validate_metadata({"blob": "x" * (MAX_METADATA_BYTES + 1)})


@pytest.mark.parametrize(
    "value", ["", "public schema", "Public", "1public", "pub;lic", "a" * 64]
)
def test_identifier_rejects_anything_that_needs_quoting(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_identifier(value, kind="schema")


def test_identifier_accepts_a_plain_lower_case_name() -> None:
    assert validate_identifier("task_queue", kind="schema") == "task_queue"


def test_names_reject_control_characters() -> None:
    with pytest.raises(ValidationError):
        validate_name("bad name", kind="task name", max_length=64)
    assert validate_name("prepare.simulation-1", kind="task name", max_length=64)


def test_the_queue_wildcard_is_not_a_name_a_queue_could_take() -> None:
    """Pause targets accept '*'; nothing else may, so the two never collide."""
    assert validate_queue_target(QUEUE_WILDCARD) == "*"
    assert validate_queue_target("compute") == "compute"
    with pytest.raises(ValidationError):
        validate_name(QUEUE_WILDCARD, kind="queue name", max_length=64)
    with pytest.raises(ValidationError):
        validate_queue_target("com*pute")


def test_keys_carry_application_data_but_stay_bounded() -> None:
    assert validate_key("simulation:abc def", kind="dedupe_key", max_length=64)
    with pytest.raises(ValidationError):
        validate_key("x" * 65, kind="dedupe_key", max_length=64)
    with pytest.raises(ValidationError):
        validate_key("a\x00b", kind="dedupe_key", max_length=64)


def test_scheduling_horizon_is_bounded() -> None:
    now = datetime(2026, 9, 1, tzinfo=UTC)
    assert validate_scheduled_at(now + timedelta(days=30), now=now)
    with pytest.raises(ValidationError, match="horizon"):
        validate_scheduled_at(now + timedelta(days=400), now=now)
    with pytest.raises(ValidationError, match="timezone-aware"):
        validate_scheduled_at(datetime(2026, 9, 2), now=now)


def test_error_text_is_truncated_not_rejected() -> None:
    truncated = truncate(
        "x" * (MAX_ERROR_MESSAGE_LENGTH + 500), MAX_ERROR_MESSAGE_LENGTH
    )
    assert truncated is not None
    assert len(truncated) == MAX_ERROR_MESSAGE_LENGTH
    assert truncated.endswith("...")
    assert truncate(None, 10) is None
    assert truncate("a\x00b", 10) == "ab"
