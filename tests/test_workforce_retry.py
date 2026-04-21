# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Unit tests for ``Workforce._retry_http`` and ``_retry_after_seconds``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ai4s.jobq.orchestration.workforce import Workforce


def _fake_response(status: int, headers: dict[str, str] | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.headers = headers or {}
    r.request = MagicMock(url="https://example/test")
    return r


class TestRetryAfterSeconds:
    def test_prefers_x_ms_header(self) -> None:
        resp = _fake_response(429, {"x-ms-retry-after-ms": "2500", "Retry-After": "30"})
        assert Workforce._retry_after_seconds(resp) == pytest.approx(2.5)

    def test_retry_after_numeric(self) -> None:
        resp = _fake_response(429, {"Retry-After": "7"})
        assert Workforce._retry_after_seconds(resp) == pytest.approx(7.0)

    def test_retry_after_http_date(self) -> None:
        future = datetime.now(timezone.utc) + timedelta(seconds=5)
        resp = _fake_response(503, {"Retry-After": future.strftime("%a, %d %b %Y %H:%M:%S GMT")})
        got = Workforce._retry_after_seconds(resp)
        assert got is not None
        assert 3.0 <= got <= 6.0

    def test_missing_header_returns_none(self) -> None:
        assert Workforce._retry_after_seconds(_fake_response(500)) is None

    def test_garbage_header_returns_none(self) -> None:
        assert Workforce._retry_after_seconds(_fake_response(429, {"Retry-After": "nope"})) is None


class TestRetryHttp:
    def test_returns_immediately_on_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        calls = [_fake_response(200)]
        send = MagicMock(side_effect=calls)
        got = Workforce._retry_http(Workforce, send)
        assert got.status_code == 200
        assert send.call_count == 1

    def test_retries_on_429_then_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []
        monkeypatch.setattr("time.sleep", slept.append)
        calls = [
            _fake_response(429, {"x-ms-retry-after-ms": "100"}),
            _fake_response(429, {"Retry-After": "0"}),
            _fake_response(200),
        ]
        send = MagicMock(side_effect=calls)
        got = Workforce._retry_http(Workforce, send, max_retries=5)
        assert got.status_code == 200
        assert send.call_count == 3
        assert slept == [pytest.approx(0.1), pytest.approx(0.0)]

    def test_retries_5xx(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        calls = [_fake_response(503), _fake_response(502), _fake_response(200)]
        send = MagicMock(side_effect=calls)
        assert Workforce._retry_http(Workforce, send, max_retries=5).status_code == 200
        assert send.call_count == 3

    def test_does_not_retry_4xx_other_than_429(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        send = MagicMock(side_effect=[_fake_response(403)])
        assert Workforce._retry_http(Workforce, send).status_code == 403
        assert send.call_count == 1

    def test_gives_up_after_max_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        send = MagicMock(side_effect=[_fake_response(429) for _ in range(10)])
        got = Workforce._retry_http(Workforce, send, max_retries=3)
        assert got.status_code == 429
        assert send.call_count == 4  # initial + 3 retries
