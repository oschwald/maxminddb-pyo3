from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

if TYPE_CHECKING:
    from types import ModuleType


@pytest.fixture
def compare_refs(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    benchmark_dir = Path(__file__).resolve().parents[1] / "benchmarks"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    return importlib.import_module("compare_refs")


@pytest.mark.parametrize("threshold", ["nan", "inf", "-inf", "-1"])
def test_invalid_threshold_fails_before_building_refs(
    compare_refs: ModuleType, monkeypatch: pytest.MonkeyPatch, threshold: str
) -> None:
    monkeypatch.setattr(
        sys, "argv", ["compare_refs.py", f"--max-regression-pct={threshold}"]
    )
    monkeypatch.setattr(
        compare_refs, "resolve_benchmark_database", Mock(return_value=Path("test.mmdb"))
    )
    prepare_ref = Mock()
    monkeypatch.setattr(compare_refs, "prepare_ref", prepare_ref)

    with pytest.raises(ValueError, match="must be finite and non-negative"):
        compare_refs.main()

    prepare_ref.assert_not_called()


@pytest.mark.parametrize("threshold", [None, "0", "5"])
def test_valid_threshold_is_accepted(
    compare_refs: ModuleType, monkeypatch: pytest.MonkeyPatch, threshold: str | None
) -> None:
    argv = ["compare_refs.py"]
    if threshold is not None:
        argv.append(f"--max-regression-pct={threshold}")
    monkeypatch.setattr(sys, "argv", argv)

    compare_refs.validate_args(compare_refs.parse_args())
