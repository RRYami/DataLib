"""Tests for orchestration and state tracking."""

from __future__ import annotations


from orchestrator import Pipeline
from utils.state import StateTracker


class TestPipeline:
    """Tests for the Pipeline orchestrator."""

    def test_empty_pipeline(self) -> None:
        p = Pipeline()
        result = p.run()
        assert result.ok is True
        assert result.tasks_run == 0
        assert result.tasks_failed == 0

    def test_successful_task(self) -> None:
        p = Pipeline()
        p.add_task("ok", lambda: None)
        result = p.run()
        assert result.ok is True
        assert result.tasks_run == 1
        assert result.tasks_failed == 0
        assert "ok" in result.task_results
        assert result.task_results["ok"]["status"] == "success"

    def test_failed_task(self) -> None:
        p = Pipeline()
        p.add_task("bad", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        result = p.run()
        assert result.ok is False
        assert result.tasks_run == 1
        assert result.tasks_failed == 1
        assert result.task_results["bad"]["status"] == "failed"
        assert "boom" in result.task_results["bad"]["error"]

    def test_mixed_tasks(self) -> None:
        p = Pipeline()
        p.add_task("ok", lambda: None)
        p.add_task("bad", lambda: (_ for _ in ()).throw(ValueError("nope")))
        result = p.run()
        assert result.ok is False
        assert result.tasks_run == 2
        assert result.tasks_failed == 1
        assert result.task_results["ok"]["status"] == "success"
        assert result.task_results["bad"]["status"] == "failed"

    def test_result_to_dict(self) -> None:
        p = Pipeline()
        p.add_task("ok", lambda: None)
        result = p.run()
        d = result.to_dict()
        assert d["ok"] is True
        assert d["tasks_run"] == 1
        assert "duration_seconds" in d


class TestStateTracker:
    """Tests for the JSONL state tracker."""

    def test_record_and_read(self, tmp_path) -> None:
        st = StateTracker(state_dir=tmp_path)
        st.record("FRED", "treasury", "DGS10", "success")
        last = st.get_last_status("FRED", "treasury", "DGS10")
        assert last is not None
        assert last["status"] == "success"
        assert last["source"] == "FRED"

    def test_get_failed_symbols(self, tmp_path) -> None:
        st = StateTracker(state_dir=tmp_path)
        st.record("POLYGON", "daily_bars", "AAPL", "success")
        st.record("POLYGON", "daily_bars", "BAD1", "failed")
        st.record("POLYGON", "daily_bars", "BAD2", "failed")
        st.record("POLYGON", "daily_bars", "BAD1", "success")  # retry succeeded
        failed = st.get_failed_symbols("POLYGON", "daily_bars")
        assert "BAD2" in failed
        assert "BAD1" not in failed  # latest is success
        assert "AAPL" not in failed

    def test_missing_state(self, tmp_path) -> None:
        st = StateTracker(state_dir=tmp_path)
        assert st.get_last_status("X", "Y", "Z") is None
        assert st.get_failed_symbols("X", "Y") == []

    def test_reset(self, tmp_path) -> None:
        st = StateTracker(state_dir=tmp_path)
        st.record("SRC", "DS", "SYM", "failed")
        st.reset()
        assert st.get_last_status("SRC", "DS", "SYM") is None
