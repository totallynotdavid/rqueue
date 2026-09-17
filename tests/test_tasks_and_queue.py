"""Task registration and enqueue validation, without a database."""

from __future__ import annotations

from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime, timedelta

import pytest

from rqueue import Queue, TaskContext, TaskRegistration
from rqueue.errors import ConfigurationError, UnknownTask, ValidationError
from rqueue.models import JobRequest
from rqueue.retry import RetryPolicy


async def handler(payload: object, context: TaskContext) -> None:
    return None


def test_registration_is_explicit_and_unique(offline_queue: Queue) -> None:
    offline_queue.register(name="a", handler=handler)
    assert "a" in offline_queue.tasks
    with pytest.raises(ValidationError, match="already registered"):
        offline_queue.register(name="a", handler=handler)


def test_sync_handlers_are_rejected(offline_queue: Queue) -> None:
    def blocking(payload: object, context: TaskContext) -> None:
        return None

    with pytest.raises(ConfigurationError, match=r"asyncio\.to_thread"):
        offline_queue.register(name="blocking", handler=blocking)  # type: ignore[arg-type]


def test_unregistered_task_lookup_raises(offline_queue: Queue) -> None:
    with pytest.raises(UnknownTask):
        offline_queue.get_task("missing")


def test_decorator_returns_the_original_function(offline_queue: Queue) -> None:
    decorated = offline_queue.task(name="decorated")(handler)
    assert decorated is handler


def test_handler_free_declaration_supplies_enqueue_defaults(
    offline_queue: Queue,
) -> None:
    retry = RetryPolicy(max_attempts=7)
    declaration = offline_queue.declare_task(
        name="producer_only", retry=retry, timeout=12.5
    )

    assert declaration.name == "producer_only"
    assert declaration.retry is retry
    assert declaration.timeout == 12.5
    assert (
        offline_queue.build_insert(JobRequest(task="producer_only")).max_attempts == 7
    )
    assert (
        offline_queue.build_insert(JobRequest(task="producer_only")).timeout_seconds
        == 12.5
    )
    assert offline_queue.tasks == {}


def test_enqueue_does_not_require_an_importable_retry_predicate(
    offline_queue: Queue,
) -> None:
    retry = RetryPolicy(retry_if=lambda exc: isinstance(exc, RuntimeError))
    registration = offline_queue.register(
        name="local_predicate", handler=handler, retry=retry
    )

    spec = offline_queue.build_insert(JobRequest(task="local_predicate"))

    assert spec.retry_policy_json is None
    assert offline_queue.declarations == {}
    assert registration.retry.retry_if is retry.retry_if


def test_declare_task_validates_retry_shape(offline_queue: Queue) -> None:
    with pytest.raises(ConfigurationError, match="RetryPolicy instance"):
        offline_queue.declare_task(name="invalid", retry=object())  # type: ignore[arg-type]


def test_declare_task_rejects_worker_retry_hooks(offline_queue: Queue) -> None:
    with pytest.raises(ConfigurationError, match="retry_on and retry_if"):
        offline_queue.declare_task(
            name="shared",
            retry=RetryPolicy(max_attempts=7, retry_on=(ValueError,)),
        )

    with pytest.raises(ConfigurationError, match="retry_on and retry_if"):
        offline_queue.declare_task(
            name="shared_if",
            retry=RetryPolicy(
                max_attempts=7,
                retry_if=lambda exc: isinstance(exc, ValueError),
            ),
        )


def test_declare_task_without_retry_strips_hooks_from_queue_default(
    offline_queue: Queue,
) -> None:
    default_retry = RetryPolicy(retry_on=(ValueError,))
    queue = Queue(
        offline_queue.pool,
        name=offline_queue.name,
        schema=offline_queue.schema,
        default_retry=default_retry,
    )

    declaration = queue.declare_task(name="custom-default")

    assert declaration.retry is not default_retry
    assert declaration.retry.max_attempts == default_retry.max_attempts
    assert declaration.retry.initial_backoff == default_retry.initial_backoff
    assert declaration.retry.max_backoff == default_retry.max_backoff
    assert declaration.retry.multiplier == default_retry.multiplier
    assert declaration.retry.jitter == default_retry.jitter
    assert declaration.retry.retry_on == (Exception,)
    assert declaration.retry.retry_if is None


def test_declaration_after_registration_strips_default_hooks(
    offline_queue: Queue,
) -> None:
    default_retry = RetryPolicy(
        initial_backoff=30.0,
        retry_on=(ValueError,),
        retry_if=lambda exc: isinstance(exc, ValueError),
    )
    queue = Queue(
        offline_queue.pool,
        name=offline_queue.name,
        schema=offline_queue.schema,
        default_retry=default_retry,
    )
    queue.register(
        name="hookful-worker",
        handler=handler,
        retry=RetryPolicy(max_attempts=7, initial_backoff=12.0),
    )

    declaration = queue.declare_task(name="hookful-worker")

    assert declaration.retry.max_attempts == 7
    assert declaration.retry.initial_backoff == 12.0
    assert declaration.retry.retry_on == (Exception,)
    assert declaration.retry.retry_if is None


def test_registration_adopts_matching_declaration(offline_queue: Queue) -> None:
    retry = RetryPolicy(max_attempts=7)
    declaration = offline_queue.declare_task(name="shared", retry=retry, timeout=12.5)

    registration = offline_queue.register(name="shared", handler=handler)

    assert registration.declaration is not declaration
    assert registration.retry.max_attempts == retry.max_attempts
    assert registration.retry.initial_backoff == retry.initial_backoff
    assert registration.retry.max_backoff == retry.max_backoff
    assert registration.retry.multiplier == retry.multiplier
    assert registration.retry.jitter == retry.jitter
    assert registration.retry.retry_on == offline_queue.default_retry.retry_on
    assert registration.retry.retry_if is offline_queue.default_retry.retry_if
    assert registration.timeout == 12.5


def test_declaration_after_registration_adopts_registration_defaults(
    offline_queue: Queue,
) -> None:
    retry = RetryPolicy(
        max_attempts=10,
        initial_backoff=30.0,
        max_backoff=300.0,
        multiplier=3.0,
        jitter=0.0,
    )
    offline_queue.register(
        name="late-declaration",
        handler=handler,
        retry=retry,
        timeout=12.5,
    )

    declaration = offline_queue.declare_task(name="late-declaration")
    spec = offline_queue.build_insert(JobRequest(task="late-declaration"))

    assert declaration.retry.max_attempts == 10
    assert declaration.retry.initial_backoff == 30.0
    assert declaration.timeout == 12.5
    assert spec.max_attempts == 10
    assert spec.timeout_seconds == 12.5
    assert spec.retry_policy_json is not None


def test_omitted_registration_adopts_a_later_declaration(
    offline_queue: Queue,
) -> None:
    offline_queue.register(name="declared-later", handler=handler)
    retry = RetryPolicy(max_attempts=5, initial_backoff=30.0, jitter=0.0)

    offline_queue.declare_task(name="declared-later", retry=retry)

    registration = offline_queue.get_task("declared-later")
    spec = offline_queue.build_insert(JobRequest(task="declared-later"))
    assert registration.retry.max_attempts == 5
    assert registration.retry.initial_backoff == 30.0
    assert spec.max_attempts == 5
    assert spec.retry_policy_json is not None


def test_explicit_registration_and_declaration_mismatch_in_either_order(
    offline_queue: Queue,
) -> None:
    first = RetryPolicy(max_attempts=3)
    second = RetryPolicy(max_attempts=5)
    offline_queue.register(name="registered-first", handler=handler, retry=first)
    with pytest.raises(ValidationError, match="existing registration"):
        offline_queue.declare_task(name="registered-first", retry=second)

    offline_queue.declare_task(name="declared-first", retry=second)
    with pytest.raises(ValidationError, match="existing declaration"):
        offline_queue.register(name="declared-first", handler=handler, retry=first)


def test_registration_keeps_retry_hooks_local_to_the_worker(
    offline_queue: Queue,
) -> None:
    declaration_retry = RetryPolicy(max_attempts=7)
    worker_retry = RetryPolicy(max_attempts=7, retry_on=(RuntimeError,))
    offline_queue.declare_task(name="shared", retry=declaration_retry)

    registration = offline_queue.register(
        name="shared", handler=handler, retry=worker_retry
    )

    assert registration.retry is worker_retry
    assert offline_queue.declarations["shared"].retry is declaration_retry


def test_registration_rejects_mismatched_declaration(offline_queue: Queue) -> None:
    offline_queue.declare_task(name="shared", retry=RetryPolicy(max_attempts=7))

    with pytest.raises(ValidationError, match="retry policy"):
        offline_queue.register(
            name="shared", handler=handler, retry=RetryPolicy(max_attempts=2)
        )
    assert "shared" not in offline_queue.tasks


def test_declaration_rejects_mismatched_registration(offline_queue: Queue) -> None:
    offline_queue.register(name="shared", handler=handler, timeout=12.5)

    with pytest.raises(ValidationError, match="timeout"):
        offline_queue.declare_task(name="shared", timeout=30.0)


def test_failed_registration_does_not_leave_a_declaration(
    offline_queue: Queue,
) -> None:
    def blocking(payload: object, context: TaskContext) -> None:
        return None

    with pytest.raises(ConfigurationError):
        offline_queue.register(name="later", handler=blocking)  # type: ignore[arg-type]

    registration = offline_queue.register(
        name="later", handler=handler, retry=RetryPolicy(max_attempts=7)
    )
    assert registration.retry.max_attempts == 7


def test_task_registration_keeps_its_legacy_constructor() -> None:
    retry = RetryPolicy(max_attempts=7)
    registration = TaskRegistration(
        name="legacy",
        handler=handler,
        decoder=lambda payload: payload,
        retry=retry,
        timeout=12.5,
    )

    assert registration.name == "legacy"
    assert registration.retry is retry
    assert registration.timeout == 12.5
    assert registration.declaration.name == "legacy"
    replaced = dataclass_replace(registration, handler=handler)
    assert replaced.declaration == registration.declaration
    assert replaced.handler is handler


def test_task_registration_replace_can_override_retry() -> None:
    registration = TaskRegistration(
        name="replaceable",
        handler=handler,
        retry=RetryPolicy(max_attempts=3),
    )
    retry = RetryPolicy(max_attempts=9, initial_backoff=15.0)

    # ``retry`` is a compatibility-only custom-constructor argument; it is
    # intentionally outside the dataclass field list used by mypy here.
    replaced = dataclass_replace(registration, retry=retry)  # type: ignore[call-arg]

    assert replaced.retry is retry
    assert replaced.name == registration.name
    assert replaced.handler is registration.handler


def test_task_registration_validates_retry_with_a_declaration(
    offline_queue: Queue,
) -> None:
    with pytest.raises(ConfigurationError, match="RetryPolicy instance"):
        TaskRegistration(
            name="invalid",
            handler=handler,
            retry=object(),  # type: ignore[arg-type]
        )


def test_dedupe_key_requires_an_explicit_conflict_mode(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="explicit on_conflict"):
        offline_queue.build_insert(JobRequest(task="t", dedupe_key="k"))
    spec = offline_queue.build_insert(
        JobRequest(task="t", dedupe_key="k", on_conflict="raise")
    )
    assert spec.raise_on_conflict is True
    assert not offline_queue.build_insert(
        JobRequest(task="t", dedupe_key="k", on_conflict="return_existing")
    ).raise_on_conflict


def test_unknown_conflict_mode_is_rejected(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="on_conflict"):
        offline_queue.build_insert(
            JobRequest(task="t", dedupe_key="k", on_conflict="ignore")
        )


def test_scheduled_at_and_delay_are_mutually_exclusive(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError, match="not both"):
        offline_queue.build_insert(
            JobRequest(
                task="t",
                scheduled_at=datetime.now(UTC),
                delay=5,
            )
        )


def test_delay_becomes_a_scheduled_instant(offline_queue: Queue) -> None:
    spec = offline_queue.build_insert(JobRequest(task="t", delay=60))
    assert spec.scheduled_at is not None
    assert spec.scheduled_at > datetime.now(UTC) + timedelta(seconds=50)


def test_max_attempts_defaults_to_the_registered_policy(offline_queue: Queue) -> None:
    offline_queue.register(
        name="picky", handler=handler, retry=RetryPolicy(max_attempts=7)
    )
    assert offline_queue.build_insert(JobRequest(task="picky")).max_attempts == 7
    assert offline_queue.build_insert(JobRequest(task="other")).max_attempts == 3


def test_undeclared_enqueue_does_not_persist_retry_defaults(
    offline_queue: Queue,
) -> None:
    spec = offline_queue.build_insert(JobRequest(task="other"))

    assert spec.retry_policy_json is None


def test_undeclared_enqueue_keeps_worker_retry_backoff(
    offline_queue: Queue,
) -> None:
    worker_retry = RetryPolicy(
        max_attempts=10,
        initial_backoff=60.0,
        max_backoff=7200.0,
        multiplier=3.0,
        jitter=0.0,
    )
    registration = offline_queue.register(
        name="email", handler=handler, retry=worker_retry
    )
    spec = offline_queue.build_insert(JobRequest(task="email"))

    effective = registration.for_job(
        retry_policy=None,
        max_attempts=spec.max_attempts,
        timeout=spec.timeout_seconds,
    )

    assert effective.retry.initial_backoff == 60.0
    assert effective.retry.max_backoff == 7200.0
    assert effective.retry.multiplier == 3.0
    assert spec.retry_policy_json is None


def test_timeout_defaults_to_the_registered_timeout(offline_queue: Queue) -> None:
    offline_queue.register(name="slow", handler=handler, timeout=12.5)
    assert offline_queue.build_insert(JobRequest(task="slow")).timeout_seconds == 12.5


def test_queue_name_can_be_overridden_for_the_scheduler(offline_queue: Queue) -> None:
    spec = offline_queue.build_insert(JobRequest(task="t"), queue_name="other")
    assert spec.queue == "other"


def test_payload_is_serialized_at_validation_time(offline_queue: Queue) -> None:
    with pytest.raises(ValidationError):
        offline_queue.build_insert(JobRequest(task="t", payload={"fn": handler}))


def test_a_batch_is_validated_before_any_row_is_written(offline_queue: Queue) -> None:
    # enqueue_many builds every spec first; a bad entry raises before the first
    # insert, so nothing is written. Exercised here through the same helper.
    requests = [JobRequest(task="t"), JobRequest(task="t", payload=object())]
    with pytest.raises(ValidationError):
        [offline_queue.build_insert(request) for request in requests]
