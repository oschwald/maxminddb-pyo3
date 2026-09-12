from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import Mock, call

import pytest

from .maxmind.reader_test import _bounded

resource = pytest.importorskip("resource")
signal = pytest.importorskip("signal")
UNLIMITED = resource.RLIM_INFINITY


@pytest.mark.parametrize(
    ("soft", "hard", "expected"),
    [
        (UNLIMITED, UNLIMITED, 1024),
        (512, UNLIMITED, 512),
        (512, 2048, 512),
        (2048, 2048, 1024),
        (512, 512, 512),
    ],
)
@pytest.mark.parametrize("raise_error", [False, True])
def test_bounded_preserves_limits_and_restores_them(
    monkeypatch: pytest.MonkeyPatch,
    soft: int,
    hard: int,
    expected: int,
    *,
    raise_error: bool,
) -> None:
    # Mock OS calls so the cases never change the test runner's real limits.
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(Path, "open", Mock(return_value=io.StringIO("0")))
    monkeypatch.setattr(resource, "getrlimit", Mock(return_value=(soft, hard)))
    setrlimit = Mock()
    monkeypatch.setattr(resource, "setrlimit", setrlimit)
    monkeypatch.setattr(signal, "signal", Mock())
    monkeypatch.setattr(signal, "alarm", Mock())

    def exercise_guard() -> None:
        with _bounded(address_space=1024):
            setrlimit.assert_called_once_with(resource.RLIMIT_AS, (expected, hard))
            if raise_error:
                msg = "test failure"
                raise ValueError(msg)

    if raise_error:
        with pytest.raises(ValueError, match="test failure"):
            exercise_guard()
    else:
        exercise_guard()

    assert setrlimit.call_args_list == [
        call(resource.RLIMIT_AS, (expected, hard)),
        call(resource.RLIMIT_AS, (soft, hard)),
    ]
