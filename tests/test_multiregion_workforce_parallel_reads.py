# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Unit tests for ``MultiRegionWorkforce`` opt-in parallel region reads.

These tests pin down the new mechanics introduced in
:func:`MultiRegionWorkforce._region_parallelism`,
:func:`MultiRegionWorkforce._map_regions`,
:func:`MultiRegionWorkforce._fmt_uptime`,
and the discovery / writer split in
:func:`MultiRegionWorkforce._resume_all_regions`.

We do **not** spin up real Azure clients. ``Workforce`` instances are built
via ``Workforce.__new__`` and the ``MultiRegionWorkforce`` is built via
``MultiRegionWorkforce.__new__`` so no network or credential is touched. Only
the attributes the methods under test read are populated.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

# ``ai4s.jobq.orchestration.workforce`` imports ``azure.ai.ml`` at module
# load, which only ships with the optional ``workforce`` extra. Skip the
# whole test file when that extra is not installed (matches
# ``test_workforce_parallel.py``).
pytest.importorskip("azure.ai.ml")

from ai4s.jobq.orchestration.multiregion_workforce import (
    _MAX_REGION_PARALLELISM,
    MultiRegionWorkforce,
    _region_parallelism,
)
from ai4s.jobq.orchestration.workforce import Workforce

# --- helpers ----------------------------------------------------------------


def _make_workforce(name: str = "wf") -> Workforce:
    """Build a bare ``Workforce`` with a deterministic ``__str__``."""
    wf = Workforce.__new__(Workforce)
    wf._experiment_name = name
    return wf


def _make_mrw(
    workforces: list[Workforce], *, parallel_region_reads: bool = False
) -> MultiRegionWorkforce:
    """Build a bare ``MultiRegionWorkforce`` with only the fields the
    methods under test read."""
    mrw = MultiRegionWorkforce.__new__(MultiRegionWorkforce)
    mrw.workforces = workforces
    mrw.parallel_region_reads = parallel_region_reads
    mrw._phase_durations = {}
    return mrw


# --- _region_parallelism ----------------------------------------------------


class TestRegionParallelism:
    """Pin down the ``n // 5 + 1`` policy with the small-fleet and cap edges."""

    @pytest.mark.parametrize(
        ("n", "expected"),
        [
            (0, 1),  # degenerate but well-defined
            (1, 1),
            (5, 1),  # boundary: still sequential
            (6, 2),  # first parallel step
            (10, 3),
            (11, 3),
            (50, 11),
            (84, 17),  # the measured-on-the-devbox case
            (155, 32),  # cap-1
            (160, 32),  # exactly at cap
            (200, 32),  # cap holds
            (10_000, 32),
        ],
    )
    def test_table(self, n: int, expected: int) -> None:
        assert _region_parallelism(n) == expected

    def test_cap_constant(self) -> None:
        # Sanity: the cap matches the documented constant; if someone edits
        # one without the other this fails loudly.
        assert _region_parallelism(_MAX_REGION_PARALLELISM * 5) == _MAX_REGION_PARALLELISM
        assert _region_parallelism(10_000) == _MAX_REGION_PARALLELISM


# --- _fmt_uptime ------------------------------------------------------------


class TestFmtUptime:
    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (0, "0s"),
            (1, "1s"),
            (59, "59s"),
            (60, "1m00s"),
            (61, "1m01s"),
            (3599, "59m59s"),
            (3600, "1h00m00s"),
            (3661, "1h01m01s"),
            (90061, "25h01m01s"),
        ],
    )
    def test_format(self, seconds: int, expected: str) -> None:
        assert MultiRegionWorkforce._fmt_uptime(seconds) == expected


# --- _map_regions: dispatch shape ------------------------------------------


def _identity_fn(idx: int, total: int, wf: Workforce) -> tuple[Any, float, bool]:
    """Return the workforce's experiment name as ``value``, with a fixed dummy duration."""
    return wf._experiment_name, 0.0, True


def _failing_for(names: set[str]):
    def fn(idx: int, total: int, wf: Workforce) -> tuple[Any, float, bool]:
        if wf._experiment_name in names:
            return None, 0.0, False
        return wf._experiment_name, 0.0, True

    return fn


class TestMapRegionsSequential:
    def test_empty_fleet_short_circuits(self) -> None:
        mrw = _make_mrw([], parallel_region_reads=True)
        assert mrw._map_regions(_identity_fn, "Empty") == []

    def test_sequential_when_flag_off(self, caplog: pytest.LogCaptureFixture) -> None:
        wfs = [_make_workforce(f"wf-{i}") for i in range(20)]
        mrw = _make_mrw(wfs, parallel_region_reads=False)
        with caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            results = mrw._map_regions(_identity_fn, "TestPhase")
        assert results == [wf._experiment_name for wf in wfs]
        # The opening rollup announces sequential mode and 1 thread.
        assert any(
            "TestPhase: starting on 20 region(s) (sequential, 1 thread)." in r.message
            for r in caplog.records
        )

    def test_sequential_when_small_fleet(self) -> None:
        # n <= 5 is sequential even with parallel_region_reads=True.
        wfs = [_make_workforce(f"wf-{i}") for i in range(5)]
        mrw = _make_mrw(wfs, parallel_region_reads=True)
        # Capture which thread each call ran on; sequential => all main thread.
        thread_ids: list[int] = []

        def fn(idx, total, wf):
            thread_ids.append(threading.get_ident())
            return wf._experiment_name, 0.0, True

        results = mrw._map_regions(fn, "Tiny")
        assert results == [wf._experiment_name for wf in wfs]
        # All in main thread.
        assert len(set(thread_ids)) == 1
        assert thread_ids[0] == threading.get_ident()


class TestMapRegionsParallel:
    def test_results_in_input_order_under_parallelism(self) -> None:
        """As-completed scheduling must not reorder the returned list."""
        wfs = [_make_workforce(f"wf-{i:02d}") for i in range(20)]
        mrw = _make_mrw(wfs, parallel_region_reads=True)

        def fn(idx, total, wf):
            # Inverse-rank sleep so first-submitted regions complete *last*.
            # If the implementation accidentally returned in completion
            # order, this assertion would fail.
            time.sleep(max(0.0, (total - idx) * 0.005))
            return wf._experiment_name, 0.0, True

        results = mrw._map_regions(fn, "OrderTest")
        assert results == [wf._experiment_name for wf in wfs]

    def test_actually_uses_threads(self) -> None:
        wfs = [_make_workforce(f"wf-{i}") for i in range(20)]
        mrw = _make_mrw(wfs, parallel_region_reads=True)
        thread_ids: list[int] = []

        def fn(idx, total, wf):
            thread_ids.append(threading.get_ident())
            return wf._experiment_name, 0.0, True

        mrw._map_regions(fn, "ThreadCheck")
        # Pool size for 20 regions is 20 // 5 + 1 = 5; expect at least 2
        # distinct workers including the main thread or not.
        assert len(set(thread_ids)) > 1

    def test_speedup_vs_sequential(self) -> None:
        """A slow per-region call should finish much faster in parallel."""
        wfs = [_make_workforce(f"wf-{i}") for i in range(10)]

        def slow_fn(idx, total, wf):
            time.sleep(0.05)
            return wf._experiment_name, 0.05, True

        mrw_seq = _make_mrw(wfs, parallel_region_reads=False)
        t0 = time.monotonic()
        mrw_seq._map_regions(slow_fn, "Seq")
        seq = time.monotonic() - t0

        mrw_par = _make_mrw(wfs, parallel_region_reads=True)
        t0 = time.monotonic()
        mrw_par._map_regions(slow_fn, "Par")
        par = time.monotonic() - t0

        # 10 regions * 0.05s sequential ≈ 0.5s; parallel with 3 threads
        # (10 // 5 + 1) is roughly ceil(10/3) * 0.05 ≈ 0.2s. Be generous to
        # avoid flakes on busy CI: just require parallel < 0.6 * sequential.
        assert par < seq * 0.6, f"expected speedup; got seq={seq:.3f}s par={par:.3f}s"


class TestMapRegionsRollup:
    def test_failures_counted_but_do_not_raise(self, caplog: pytest.LogCaptureFixture) -> None:
        wfs = [_make_workforce(f"wf-{i}") for i in range(6)]
        mrw = _make_mrw(wfs, parallel_region_reads=False)
        fn = _failing_for({"wf-1", "wf-3"})
        with caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            results = mrw._map_regions(fn, "WithFailures")
        # Failed entries return whatever ``fn`` produced (here ``None``);
        # the dispatcher does not substitute its own value.
        assert results[0] == "wf-0"
        assert results[1] is None
        assert results[3] is None
        # Closing rollup line includes the failure count and p50/max.
        rollup = next(
            r.message for r in caplog.records if r.message.startswith("WithFailures: done in")
        )
        assert "failed 2/6" in rollup
        assert "p50=" in rollup
        assert "max=" in rollup

    def test_slowest_three_in_rollup(self, caplog: pytest.LogCaptureFixture) -> None:
        wfs = [_make_workforce(f"wf-{i}") for i in range(5)]
        # Pre-baked durations: wf-2 slowest, wf-4 second, wf-0 third.
        durations = {"wf-0": 1.5, "wf-1": 0.1, "wf-2": 9.0, "wf-3": 0.2, "wf-4": 3.0}

        def fn(idx, total, wf):
            name = wf._experiment_name
            return name, durations[name], True

        mrw = _make_mrw(wfs, parallel_region_reads=False)
        with caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            mrw._map_regions(fn, "Slowest")
        rollup = next(r.message for r in caplog.records if r.message.startswith("Slowest: done in"))
        # Slowest-3 in descending order, with names + parenthesised duration.
        slowest_idx = rollup.index("slowest:")
        slowest_segment = rollup[slowest_idx:]
        assert slowest_segment.index("wf-2") < slowest_segment.index("wf-4")
        assert slowest_segment.index("wf-4") < slowest_segment.index("wf-0")
        # And the un-slow ones are not in the slowest-3 segment.
        assert "wf-1" not in slowest_segment
        assert "wf-3" not in slowest_segment

    def test_heartbeat_quiet_on_small_fleet(self, caplog: pytest.LogCaptureFixture) -> None:
        wfs = [_make_workforce(f"wf-{i}") for i in range(10)]
        mrw = _make_mrw(wfs, parallel_region_reads=False)
        with caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            mrw._map_regions(_identity_fn, "Quiet")
        # Heartbeat threshold gates on n >= 20; with n=10 we expect only the
        # opening "starting on" line and the closing "done in" line.
        progress_lines = [r for r in caplog.records if "progress " in r.message]
        assert progress_lines == []

    def test_heartbeat_fires_on_large_fleet(self, caplog: pytest.LogCaptureFixture) -> None:
        wfs = [_make_workforce(f"wf-{i:03d}") for i in range(40)]
        mrw = _make_mrw(wfs, parallel_region_reads=False)
        with caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            mrw._map_regions(_identity_fn, "Loud")
        progress_lines = [r for r in caplog.records if "Loud: progress " in r.message]
        assert progress_lines, "expected at least one heartbeat line for n=40"


# --- _resume_all_regions: discovery / writer split --------------------------


class TestResumeAllRegions:
    def test_discovery_uses_map_regions_writers_stay_sequential(self) -> None:
        """Discovery must run through ``_map_regions`` (so it parallelises
        when the flag is on) but ``parallel_resume`` calls must run
        sequentially in input order."""
        wf_a = _make_workforce("wf-a")
        wf_a.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=3)
        )
        wf_a.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        wf_b = _make_workforce("wf-b")
        wf_b.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=0)  # no work for B
        )
        wf_b.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        wf_c = _make_workforce("wf-c")
        wf_c.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=5)
        )
        wf_c.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        mrw = _make_mrw([wf_a, wf_b, wf_c], parallel_region_reads=True)

        total = mrw._resume_all_regions()

        # Returned total = sum of paused counts for regions where discovery
        # succeeded and num_paused > 0.
        assert total == 3 + 5

        # Discovery hit every region.
        wf_a.get_detailed_state.assert_called_once()  # type: ignore[attr-defined]
        wf_b.get_detailed_state.assert_called_once()  # type: ignore[attr-defined]
        wf_c.get_detailed_state.assert_called_once()  # type: ignore[attr-defined]

        # Writer calls only on regions with paused workers, with the right counts.
        wf_a.parallel_resume.assert_called_once_with(3)  # type: ignore[attr-defined]
        wf_b.parallel_resume.assert_not_called()  # type: ignore[attr-defined]
        wf_c.parallel_resume.assert_called_once_with(5)  # type: ignore[attr-defined]

        # Phase timings recorded for both halves.
        assert "resume_disc" in mrw._phase_durations
        assert "resume" in mrw._phase_durations

    def test_discovery_failure_skips_only_that_region(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        wf_ok = _make_workforce("wf-ok")
        wf_ok.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=2)
        )
        wf_ok.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        wf_bad = _make_workforce("wf-bad")
        wf_bad.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            side_effect=RuntimeError("AML 503")
        )
        wf_bad.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        mrw = _make_mrw([wf_ok, wf_bad], parallel_region_reads=False)

        with caplog.at_level(logging.ERROR, logger="ai4s.jobq.orchestration.multiregion_workforce"):
            total = mrw._resume_all_regions()

        assert total == 2
        wf_ok.parallel_resume.assert_called_once_with(2)  # type: ignore[attr-defined]
        # Failed-discovery region is skipped, not retried as a writer.
        wf_bad.parallel_resume.assert_not_called()  # type: ignore[attr-defined]

    def test_resume_writer_failure_does_not_abort_other_regions(self) -> None:
        wf_first = _make_workforce("wf-first")
        wf_first.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=1)
        )
        wf_first.parallel_resume = MagicMock(  # type: ignore[attr-defined]
            side_effect=RuntimeError("MFE 500")
        )

        wf_second = _make_workforce("wf-second")
        wf_second.get_detailed_state = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(num_paused=4)
        )
        wf_second.parallel_resume = MagicMock()  # type: ignore[attr-defined]

        mrw = _make_mrw([wf_first, wf_second], parallel_region_reads=False)
        total = mrw._resume_all_regions()

        # Failure in wf-first must not block wf-second.
        wf_second.parallel_resume.assert_called_once_with(4)  # type: ignore[attr-defined]
        # Total counts only the regions whose writer call did not raise.
        assert total == 4
