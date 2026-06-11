"""Fixtures for the PURE rayx.runtime unit tests.

These tests must NOT import ``rayx`` / ``rayx.runtime`` / ``_rayx`` (importing the
package triggers the ``_rayx`` extension + HPX). Instead the pure, import-light
submodules ``_validate.py`` and ``_errors.py`` are loaded directly **by file path**
via :func:`importlib.util.spec_from_file_location`, which bypasses the package
``__init__``. This works only because those two modules are stdlib-only and have no
relative imports -- so this suite runs in the lightweight repo-sanity CI job, with no
built extension.
"""
import importlib.util
import pathlib

import pytest

# repo_root / python / src / rayx / runtime  (parents[2] == repo root)
_RUNTIME_DIR = (pathlib.Path(__file__).resolve().parents[2]
                / "python" / "src" / "rayx" / "runtime")


def _load_by_path(name):
    """Load ``<runtime>/<name>.py`` as a standalone module (no package import)."""
    path = _RUNTIME_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"rayx_runtime_unit_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def validate():
    """The import-light ``_validate`` module (validators + ROW_FIELDS)."""
    return _load_by_path("_validate")


@pytest.fixture(scope="session")
def errors():
    """The import-light ``_errors`` module (the exception hierarchy)."""
    return _load_by_path("_errors")


@pytest.fixture
def op_table():
    """A representative typed-signature table mirroring the native registry.

    Shape matches ``_rayx.runtime_op_table()`` (closed value model: int64 /
    finite double): ``{op_id: {"arg_types": [type, ...], "result_type": type}}``.
    validate_call() takes the table as a parameter, so the pure unit tests need no
    access to the real C++ registry.
    """
    def sig(*arg_types):
        return {"arg_types": list(arg_types), "result_type": "int64"}

    def sig_d(*arg_types):
        return {"arg_types": list(arg_types), "result_type": "double"}

    return {
        "square": sig("int64"),
        "add": sig("int64", "int64"),
        "boom": sig(),
        "busy_sum": sig("int64"),
        "fanout_sum": sig("int64", "int64"),
        "scale_double": sig_d("double", "double"),
        "park_ms": sig("int64"),
    }


@pytest.fixture
def actor_table():
    """A representative typed actor-metadata table mirroring the native registry.

    Shape matches ``_rayx.runtime_actor_table()``:
    ``{actor_type: {"init_arg_types": [type, ...], "methods": {method_id:
    {"arg_types": [type, ...], "result_type": type}}}}``. validate_actor_create /
    validate_actor_call take the table as a parameter, so the pure unit tests need no
    access to the real C++ actor registry.
    """
    return {
        "counter": {
            "init_arg_types": ["int64"],
            "methods": {
                "add": {"arg_types": ["int64"], "result_type": "int64"},
                "get": {"arg_types": [], "result_type": "int64"},
                "reset": {"arg_types": ["int64"], "result_type": "int64"},
                "busy_get": {"arg_types": ["int64"], "result_type": "int64"},
            },
        }
    }
