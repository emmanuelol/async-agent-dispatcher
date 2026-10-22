import sys
import pytest
from async_agent_dispatcher.worker import (
    BoundedStringIO,
    isolated_argv,
    run_job_module,
    run_agent_graph,
    JobExecutionError,
)

def test_bounded_string_io_truncation():
    buf = BoundedStringIO(max_bytes=50)
    buf.write("A" * 30)
    assert buf.getvalue() == "A" * 30
    assert not buf.truncated

    # Write beyond limit
    buf.write("B" * 30)
    assert buf.truncated
    content = buf.getvalue()
    assert "[OUTPUT OOM PREVENTED]" in content
    # Excess bytes discarded
    assert len(content) < 100


def test_isolated_argv():
    original = list(sys.argv)
    with isolated_argv(["dummy_script.py", "--flag", "val"]):
        assert sys.argv == ["dummy_script.py", "--flag", "val"]
    assert sys.argv == original


def test_run_agent_graph_callable():
    def dummy_graph(state):
        return {"step": state.get("step", 0) + 1}

    out = run_agent_graph(dummy_graph, {"step": 5})
    assert out == {"step": 6}


def test_run_agent_graph_invoke_object():
    class DummyGraph:
        def invoke(self, state):
            return {"status": "ok", "tenant": state.get("tenant")}

    graph = DummyGraph()
    out = run_agent_graph(graph, {"tenant": "test_org"})
    assert out == {"status": "ok", "tenant": "test_org"}


def test_run_job_module_execution(tmp_path, monkeypatch):
    script = tmp_path / "mock_module.py"
    script.write_text("import sys\nprint('hello stdout')\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    output = run_job_module("mock_module")
    assert "hello stdout" in output


def test_run_job_module_failure(tmp_path, monkeypatch):
    script = tmp_path / "failing_module.py"
    script.write_text("raise ValueError('test intentional failure')\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(JobExecutionError) as exc_info:
        run_job_module("failing_module")
    assert "ValueError" in str(exc_info.value)


def test_run_agent_graph_falsy_object():
    class FalsyGraph:
        def __len__(self):
            return 0  # truthiness is False

        def invoke(self, state):
            return {"received": state}

    graph = FalsyGraph()
    res = run_agent_graph(graph, {"key": "val"})
    assert res == {"received": {"key": "val"}}


@pytest.mark.asyncio
async def test_arun_agent_graph():
    from async_agent_dispatcher.worker import arun_agent_graph

    class AsyncGraph:
        async def ainvoke(self, state):
            return {"async_status": "done", "val": state.get("x")}

    graph = AsyncGraph()
    res = await arun_agent_graph(graph, {"x": 100})
    assert res == {"async_status": "done", "val": 100}


def test_run_job_module_concurrency_guard(tmp_path, monkeypatch):
    import threading
    from async_agent_dispatcher.worker import _job_execution_lock

    script = tmp_path / "concurrent_mod.py"
    script.write_text("print('test')\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    # Manually hold lock to simulate another thread executing run_job_module
    with _job_execution_lock:
        with pytest.raises(RuntimeError) as exc_info:
            run_job_module("concurrent_mod")
        assert "incompatible with concurrent" in str(exc_info.value)

