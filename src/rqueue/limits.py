"""Resource bounds and the validators that enforce them (REQUIREMENTS.md §8).

Every bound here is also expressed as a CHECK constraint in the migrations, so
a direct SQL writer cannot get past it either. The Python side exists to turn a
bound violation into a typed error before a statement is ever sent.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from rqueue.errors import ValidationError

__all__ = [
    "MAX_CONCURRENCY",
    "MAX_DEDUPE_KEY_LENGTH",
    "MAX_ERROR_MESSAGE_LENGTH",
    "MAX_METADATA_BYTES",
    "MAX_PAYLOAD_BYTES",
    "MAX_TASK_NAME_LENGTH",
    "validate_identifier",
    "validate_payload",
]

#: Largest serialized JSON payload accepted by ``enqueue`` (256 KiB).
MAX_PAYLOAD_BYTES: Final = 256 * 1024
#: Largest serialized JSON blob accepted for a job's ``metadata`` (8 KiB).
MAX_METADATA_BYTES: Final = 8 * 1024
MAX_TASK_NAME_LENGTH: Final = 128
MAX_QUEUE_NAME_LENGTH: Final = 64
MAX_WORKER_ID_LENGTH: Final = 128
MAX_DEDUPE_KEY_LENGTH: Final = 256
MAX_CONCURRENCY_KEY_LENGTH: Final = 256
MAX_SCHEDULE_NAME_LENGTH: Final = 128
#: Error text is truncated to this many characters before it reaches a row.
MAX_ERROR_MESSAGE_LENGTH: Final = 4096
MAX_ERROR_TYPE_LENGTH: Final = 256
MAX_ATTEMPTS_LIMIT: Final = 1000
#: A job may not be scheduled further than this into the future.
MAX_SCHEDULING_HORIZON: Final = timedelta(days=365)
#: Largest batch a single claim may take, and largest ``enqueue_many`` batch.
MAX_BATCH_SIZE: Final = 1000
MAX_ENQUEUE_BATCH: Final = 1000
MAX_CONCURRENCY: Final = 1024
MIN_PRIORITY: Final = -32768
MAX_PRIORITY: Final = 32767
#: Bounds on a lease, in seconds. Sub-second leases are allowed so tests can
#: drive real expiry without sleeping for a wall-clock minute.
MIN_LEASE_SECONDS: Final = 0.1
MAX_LEASE_SECONDS: Final = 24 * 3600.0

# PostgreSQL folds unquoted identifiers to lower case; requiring that shape up
# front means the schema name we interpolate is byte-identical to the one
# PostgreSQL stores, and cannot carry a quote or a dot.
_IDENTIFIER_RE: Final = re.compile(r"^[a-z_][a-z0-9_]*$")
_MAX_IDENTIFIER_LENGTH: Final = 63

_NAME_RE: Final = re.compile(r"^[A-Za-z0-9_.:\-]+$")


def validate_identifier(value: str, *, kind: str) -> str:
    """Validate a PostgreSQL identifier that will be interpolated into SQL.

    Schema names cannot be passed as bind parameters, so they are the one
    string that reaches a statement by interpolation. Restricting them to
    lower-case ``[a-z_][a-z0-9_]*`` and 63 bytes makes the interpolation
    unambiguously safe without relying on quoting.
    """
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ValidationError(
            f"{kind} must match [a-z_][a-z0-9_]* (got {value!r})",
        )
    if len(value) > _MAX_IDENTIFIER_LENGTH:
        raise ValidationError(
            f"{kind} must be at most {_MAX_IDENTIFIER_LENGTH} characters"
        )
    return value


def validate_name(value: str, *, kind: str, max_length: int) -> str:
    """Validate a queue, task, schedule, or worker name."""
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{kind} must be a non-empty string")
    if len(value) > max_length:
        raise ValidationError(f"{kind} must be at most {max_length} characters")
    if not _NAME_RE.match(value):
        raise ValidationError(
            f"{kind} may only contain letters, digits, '_', '.', ':' and '-' "
            f"(got {value!r})"
        )
    return value


def validate_key(value: str | None, *, kind: str, max_length: int) -> str | None:
    """Validate an optional dedupe or concurrency key.

    Keys carry application data (``simulation:<external id>``), so they are not
    held to the name character class -- only to a length bound and a ban on
    NUL, which PostgreSQL cannot store in ``text``.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{kind} must be a non-empty string when provided")
    if len(value) > max_length:
        raise ValidationError(f"{kind} must be at most {max_length} characters")
    if "\x00" in value:
        raise ValidationError(f"{kind} may not contain a NUL character")
    return value


def encode_json(
    value: Any,
    *,
    kind: str,
    max_bytes: int,
    require_object: bool = False,
) -> str:
    """Serialize a JSON-only value, rejecting anything unserializable.

    ``json.dumps`` with the default encoder is what makes "payloads are JSON
    only" (§3) true: a callable, a pickle, or an arbitrary object raises here
    rather than reaching the database.
    """
    if require_object and not isinstance(value, Mapping):
        raise ValidationError(f"{kind} must be a JSON object")
    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{kind} must be JSON-serializable: {exc}") from exc
    size = len(encoded.encode("utf-8"))
    if size > max_bytes:
        raise ValidationError(
            f"{kind} is {size} bytes, over the {max_bytes} byte limit"
        )
    return encoded


def validate_payload(value: Any) -> str:
    """Serialize a job payload, enforcing the JSON-only rule and size bound."""
    return encode_json(value, kind="payload", max_bytes=MAX_PAYLOAD_BYTES)


def validate_metadata(value: Mapping[str, Any] | None) -> str:
    """Serialize optional job metadata as a JSON object."""
    if value is None:
        return "{}"
    if not isinstance(value, Mapping):
        raise ValidationError("metadata must be a JSON object")
    return encode_json(
        dict(value),
        kind="metadata",
        max_bytes=MAX_METADATA_BYTES,
        require_object=True,
    )


def validate_max_attempts(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError("max_attempts must be an int")
    if not 1 <= value <= MAX_ATTEMPTS_LIMIT:
        raise ValidationError(
            f"max_attempts must be between 1 and {MAX_ATTEMPTS_LIMIT} (got {value})"
        )
    return value


def validate_priority(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError("priority must be an int")
    if not MIN_PRIORITY <= value <= MAX_PRIORITY:
        raise ValidationError(
            f"priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
        )
    return value


def validate_concurrency(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError("concurrency must be an int")
    if not 1 <= value <= MAX_CONCURRENCY:
        raise ValidationError(
            f"concurrency must be between 1 and {MAX_CONCURRENCY} (got {value})"
        )
    return value


def validate_batch_size(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError("batch size must be an int")
    if not 1 <= value <= MAX_BATCH_SIZE:
        raise ValidationError(
            f"batch size must be between 1 and {MAX_BATCH_SIZE} (got {value})"
        )
    return value


def validate_lease_seconds(value: float) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValidationError("lease duration must be a number of seconds")
    if not MIN_LEASE_SECONDS <= float(value) <= MAX_LEASE_SECONDS:
        raise ValidationError(
            f"lease duration must be between {MIN_LEASE_SECONDS} and "
            f"{MAX_LEASE_SECONDS} seconds (got {value})"
        )
    return float(value)


def validate_timeout(value: float | None) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValidationError("timeout must be a number of seconds or None")
    if float(value) <= 0:
        raise ValidationError("timeout must be positive")
    return float(value)


def validate_scheduled_at(value: datetime, *, now: datetime | None = None) -> datetime:
    """Require an aware timestamp inside the scheduling horizon."""
    if not isinstance(value, datetime):
        raise ValidationError("scheduled_at must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError("scheduled_at must be timezone-aware")
    reference = now or datetime.now(UTC)
    if value - reference > MAX_SCHEDULING_HORIZON:
        raise ValidationError(
            f"scheduled_at is beyond the {MAX_SCHEDULING_HORIZON.days} day "
            "scheduling horizon"
        )
    return value


def truncate(value: str | None, max_length: int) -> str | None:
    """Bound a free-text field destined for a row, marking any truncation."""
    if value is None:
        return None
    collapsed = value.replace("\x00", "")
    if len(collapsed) <= max_length:
        return collapsed
    return collapsed[: max_length - 3] + "..."
