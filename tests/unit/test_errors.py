"""Pure unit tests for rayx.runtime error classes + ROW_FIELDS (no rayx import).

The ``errors`` and ``validate`` fixtures load ``_errors.py`` / ``_validate.py`` by
file path; see tests/unit/conftest.py.
"""

EXPECTED_ROW_FIELDS = (
    "actor_id",
    "submit_ns",
    "start_ns",
    "end_ns",
    "total_ms",
    "queue_wait_ms",
    "service_ms_observed",
    "status",
    "error",
)

# Must NOT appear in a runtime row: the operation value, and the harness-facade
# echoes that the runtime deliberately omits.
FORBIDDEN_ROW_FIELDS = ("value", "label", "chunks", "chunk_delay_ms",
                        "chunks_completed")


# --- error hierarchy --------------------------------------------------------


def test_queue_full_error_is_runtime_error(errors):
    assert issubclass(errors.QueueFullError, RuntimeError)


def test_runtime_operation_error_is_runtime_error(errors):
    assert issubclass(errors.RuntimeOperationError, RuntimeError)


def test_failed_and_cancelled_subclass_runtime_operation_error(errors):
    assert issubclass(errors.OperationFailedError, errors.RuntimeOperationError)
    assert issubclass(errors.OperationCancelledError, errors.RuntimeOperationError)


def test_error_names(errors):
    assert errors.QueueFullError.__name__ == "QueueFullError"
    assert errors.RuntimeOperationError.__name__ == "RuntimeOperationError"
    assert errors.OperationFailedError.__name__ == "OperationFailedError"
    assert errors.OperationCancelledError.__name__ == "OperationCancelledError"


def test_error_module_is_public_rayx_runtime(errors):
    # Public-facing module name is preserved as rayx.runtime (not the private
    # _errors submodule) so reprs / tracebacks / pickle reference the public home.
    for cls in (errors.QueueFullError, errors.RuntimeOperationError,
                errors.OperationFailedError, errors.OperationCancelledError):
        assert cls.__module__ == "rayx.runtime"


# --- ROW_FIELDS contract ----------------------------------------------------


def test_row_fields_exact(validate):
    assert tuple(validate.ROW_FIELDS) == EXPECTED_ROW_FIELDS
    assert len(validate.ROW_FIELDS) == 9


def test_row_fields_exclude_value_and_harness_echoes(validate):
    for field in FORBIDDEN_ROW_FIELDS:
        assert field not in validate.ROW_FIELDS
