# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Unit tests for ``Workforce`` parallel-method helpers.

These tests pin down the thread-safety + selection logic of
``parallel_hire`` / ``parallel_lay_off`` / ``resume`` / ``parallel_resume``
without hitting Azure. The ``MLClient``/credential dependencies of
``Workforce.__init__`` are side-stepped by constructing instances via
``Workforce.__new__`` and setting only the attributes the methods under test
read.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# ``ai4s.jobq.orchestration.workforce`` imports ``azure.ai.ml`` at module
# load, which only ships with the optional ``workforce`` extra. Skip the
# whole test file when that extra is not installed (e.g. the default CI
# environment).
pytest.importorskip("azure.ai.ml")

from ai4s.jobq.orchestration.workforce import AmlExperiment, AmlJob, Workforce


def _make_job(name: str, status: str = "Paused") -> AmlJob:
    return AmlJob(
        experiment=AmlExperiment(
            id="e",
            subscription_id="s",
            resource_group="rg",
            workspace="ws",
        ),
        status=status,  # type: ignore[arg-type]
        name=name,
        start_time=None,
        cluster="c",
        error_msg=None,
        metrics={},
    )


def _bare_workforce(**overrides) -> Workforce:
    """Build a ``Workforce`` instance bypassing ``__init__``.

    We set only the attributes touched by the methods under test.
    """
    wf = Workforce.__new__(Workforce)
    wf._experiment_name = "exp"
    wf._aml_client = MagicMock()
    wf._credential = MagicMock()
    wf._workspace_location = "eastus"
    wf.session = MagicMock()
    wf._aml_client.subscription_id = "sub"
    wf._aml_client.resource_group_name = "rg"
    wf._aml_client.workspace_name = "ws"
    for k, v in overrides.items():
        setattr(wf, k, v)
    return wf


class TestBuildWorker:
    def test_returns_independent_copies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proto = MagicMock()
        proto.environment_variables = {"SHARED": "1"}
        wf = _bare_workforce()
        wf._job = proto
        monkeypatch.delenv("APPLICATIONINSIGHTS_CONNECTION_STRING", raising=False)

        j1 = wf._build_worker()
        j2 = wf._build_worker()

        assert j1 is not j2
        assert j1.environment_variables is not j2.environment_variables
        assert j1.name != j2.name
        assert j1.name.startswith("exp-")
        assert j1.experiment_name == "exp"
        # Prototype must not be mutated.
        assert proto.environment_variables == {"SHARED": "1"}

    def test_propagates_appinsights_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proto = MagicMock()
        proto.environment_variables = None
        wf = _bare_workforce()
        wf._job = proto
        monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "conn")

        j = wf._build_worker()
        assert j.environment_variables["APPLICATIONINSIGHTS_CONNECTION_STRING"] == "conn"


class TestParallelHire:
    def test_no_op_on_non_positive_n(self) -> None:
        wf = _bare_workforce()
        wf.parallel_hire(0, progress=False)
        assert wf._aml_client.jobs.create_or_update.call_count == 0

    def test_submits_n_jobs_first_serial_then_parallel(self) -> None:
        proto = MagicMock()
        proto.environment_variables = None
        wf = _bare_workforce()
        wf._job = proto

        wf.parallel_hire(5, workers=4, progress=False)

        assert wf._aml_client.jobs.create_or_update.call_count == 5
        submitted_names = {
            call.args[0].name for call in wf._aml_client.jobs.create_or_update.call_args_list
        }
        # All submitted jobs have distinct randomly-generated names.
        assert len(submitted_names) == 5

    def test_partial_failures_are_logged_not_raised(self) -> None:
        proto = MagicMock()
        proto.environment_variables = None
        wf = _bare_workforce()
        wf._job = proto
        calls = {"n": 0}

        def flaky(_job):
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("boom")

        wf._aml_client.jobs.create_or_update.side_effect = flaky
        wf.parallel_hire(4, workers=2, progress=False)
        assert wf._aml_client.jobs.create_or_update.call_count == 4

    def test_tolerates_duplicate_job_name_error(self) -> None:
        """Regression: SDK transport-retry re-sending a successful POST
        surfaces as HttpResponseError(JobPropertyImmutable). Because job
        names are generated with 8 chars of a-z0-9 entropy, encountering
        this error on our *own* fresh name is only possible via the retry
        race, so it must be treated as success, not propagated."""
        from azure.core.exceptions import HttpResponseError

        proto = MagicMock()
        proto.environment_variables = None
        wf = _bare_workforce()
        wf._job = proto
        calls = {"n": 0}

        def flaky(_job):
            calls["n"] += 1
            if calls["n"] == 2:
                # Realistic shape — JobPropertyImmutable appears in str(exc).
                raise HttpResponseError(
                    message=(
                        "(UserError) A job with this name already exists. "
                        'InnerError: {"code": "Immutable", "innerError": '
                        '{"code": "JobPropertyImmutable"}}'
                    )
                )

        wf._aml_client.jobs.create_or_update.side_effect = flaky
        # Must not raise — sequential hire.
        wf.hire(3, progress=False)
        assert wf._aml_client.jobs.create_or_update.call_count == 3

    def test_parallel_hire_tolerates_duplicate_job_name_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        proto = MagicMock()
        proto.environment_variables = None
        wf = _bare_workforce()
        wf._job = proto
        calls = {"n": 0}

        def flaky(_job):
            calls["n"] += 1
            if calls["n"] == 3:
                raise HttpResponseError(message="(UserError) ... JobPropertyImmutable ...")

        wf._aml_client.jobs.create_or_update.side_effect = flaky
        wf.parallel_hire(5, workers=2, progress=False)
        assert wf._aml_client.jobs.create_or_update.call_count == 5


class TestParallelLayoff:
    def test_sort_prefers_paused_then_queued_then_waiting(self) -> None:
        wf = _bare_workforce()
        jobs = [
            _make_job("running", status="Running"),
            _make_job("queued", status="Queued"),
            _make_job("paused", status="Paused"),
            _make_job("waiting", status="Waiting"),
        ]
        wf.list_jobs = MagicMock(return_value=iter(jobs))  # type: ignore[method-assign]

        picks = wf._layoff_candidates(3)
        assert [j.name for j in picks] == ["paused", "queued", "waiting"]

    def test_caps_at_available_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        wf = _bare_workforce()
        wf.list_jobs = MagicMock(  # type: ignore[method-assign]
            return_value=iter([_make_job("a", status="Queued")])
        )
        with caplog.at_level("WARNING"):
            picks = wf._layoff_candidates(5)
        assert len(picks) == 1
        assert "Only 1 workers to stop" in caplog.text

    def test_parallel_layoff_cancels_each(self) -> None:
        wf = _bare_workforce()
        jobs = [_make_job(f"j{i}", status="Queued") for i in range(3)]
        wf.list_jobs = MagicMock(return_value=iter(jobs))  # type: ignore[method-assign]
        wf.parallel_lay_off(3, workers=2, progress=False)
        assert wf._aml_client.jobs.begin_cancel.call_count == 3

    def test_cancel_one_swallows_permission_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        wf = _bare_workforce()
        err = HttpResponseError(
            message="Cannot execute Cancel because user does not have sufficient permission."
        )
        wf._aml_client.jobs.begin_cancel.side_effect = err

        wf._cancel_one(_make_job("x", status="Queued"))  # must not raise

    def test_cancel_one_reraises_other_errors(self) -> None:
        from azure.core.exceptions import HttpResponseError

        wf = _bare_workforce()
        err = HttpResponseError(message="bad request")
        wf._aml_client.jobs.begin_cancel.side_effect = err
        with pytest.raises(HttpResponseError):
            wf._cancel_one(_make_job("x", status="Queued"))


class TestResume:
    def test_resume_one_url_shape_and_headers(self) -> None:
        wf = _bare_workforce()
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        response = MagicMock(status_code=202)
        wf.session.post = MagicMock(return_value=response)

        wf._resume_one(_make_job("exp-abcd1234", status="Paused"))

        url = wf.session.post.call_args.args[0]
        assert url == (
            "https://ml.azure.com/api/eastus/execution/v1.0/"
            "subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.MachineLearningServices/workspaces/ws/"
            "experiments/exp/runId/exp-abcd1234/resume"
        )
        headers = wf.session.post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer tok"

    def test_resume_one_swallows_4xx_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        wf = _bare_workforce()
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        wf.session.post = MagicMock(
            return_value=MagicMock(
                status_code=404,
                text='{"error":{"code":"NotFound","message":"run does not exist"}}',
            )
        )
        with caplog.at_level("WARNING"):
            wf._resume_one(_make_job("x", status="Paused"))  # must not raise
        assert "Failed to resume worker x" in caplog.text
        assert "404" in caplog.text
        wf._aml_client.jobs.begin_cancel.assert_not_called()

    def test_resume_one_cancels_when_max_execution_time_exceeded(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        wf = _bare_workforce()
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        wf.session.post = MagicMock(
            return_value=MagicMock(
                status_code=400,
                text=(
                    '{"error":{"code":"BadRequest","message":"Cannot resume resource '
                    "/subscriptions/.../jobs/x because it has exceeded max job "
                    'execution time of 313 hours."}}'
                ),
            )
        )
        with caplog.at_level("WARNING"):
            wf._resume_one(_make_job("x", status="Paused"))
        assert "cancelling instead" in caplog.text
        wf._aml_client.jobs.begin_cancel.assert_called_once_with("x")

    def test_resume_one_raises_on_5xx_after_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        wf = _bare_workforce()
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        wf.session.post = MagicMock(
            return_value=MagicMock(status_code=503, text="unavailable", headers={})
        )
        monkeypatch.setattr("ai4s.jobq.orchestration.workforce.time.sleep", lambda _s: None)
        with pytest.raises(RuntimeError, match="Failed to resume"):
            wf._resume_one(_make_job("x", status="Paused"))
        # initial attempt + _RESUME_MAX_RETRIES retries
        assert wf.session.post.call_count == wf._RESUME_MAX_RETRIES + 1

    def test_resume_one_retries_then_succeeds_on_5xx(self, monkeypatch: pytest.MonkeyPatch) -> None:
        wf = _bare_workforce()
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        wf.session.post = MagicMock(
            side_effect=[
                MagicMock(status_code=503, text="overloaded", headers={}),
                MagicMock(status_code=500, text="inner 503", headers={}),
                MagicMock(status_code=202, text=""),
            ]
        )
        monkeypatch.setattr("ai4s.jobq.orchestration.workforce.time.sleep", lambda _s: None)
        wf._resume_one(_make_job("x", status="Paused"))  # must not raise
        assert wf.session.post.call_count == 3

    def test_resume_retry_delay_honors_retry_after(self) -> None:
        from ai4s.jobq.orchestration.workforce import Workforce

        response = MagicMock(headers={"Retry-After": "2.5"})
        assert Workforce._resume_retry_delay(response, attempt=0) == 2.5
        response = MagicMock(headers={"x-ms-retry-after-ms": "1500"})
        assert Workforce._resume_retry_delay(response, attempt=0) == 1.5

    def test_parallel_resume_no_op_on_non_positive(self) -> None:
        wf = _bare_workforce()
        wf.list_jobs = MagicMock()  # type: ignore[method-assign]
        wf.parallel_resume(0, progress=False)
        wf.list_jobs.assert_not_called()

    def test_parallel_resume_dispatches(self) -> None:
        wf = _bare_workforce()
        jobs = [_make_job(f"p{i}", status="Paused") for i in range(3)]
        wf.list_jobs = MagicMock(return_value=iter(jobs))  # type: ignore[method-assign]
        wf._credential.get_token = MagicMock(return_value=MagicMock(token="tok"))
        wf.session.post = MagicMock(return_value=MagicMock(status_code=200))

        wf.parallel_resume(3, workers=2, progress=False)
        assert wf.session.post.call_count == 3

    def test_paused_candidates_caps_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        wf = _bare_workforce()
        wf.list_jobs = MagicMock(  # type: ignore[method-assign]
            return_value=iter([_make_job("a", status="Paused")])
        )
        with caplog.at_level("WARNING"):
            picks = wf._paused_candidates(5)
        assert len(picks) == 1
        assert "Only 1 paused workers" in caplog.text
