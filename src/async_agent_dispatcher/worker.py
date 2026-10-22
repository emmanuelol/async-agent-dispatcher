import asyncio
import contextlib
import importlib
import inspect
import io
import json
import logging
import os
import runpy
import sys
import threading
import traceback
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

logger = logging.getLogger("async_agent_dispatcher.worker")

_job_execution_lock = threading.Lock()


class JobExecutionError(Exception):
    """Raised when a job module fails execution, preserving captured stdout and stderr."""
    pass


class BoundedStringIO(io.StringIO):
    """
    In-memory string buffer that enforces a hard ceiling on written bytes.
    Prevents container OOM kills from unbounded logging while allowing execution to proceed.
    """
    def __init__(self, max_bytes: int = 5_000_000):
        super().__init__()
        self.max_bytes = max_bytes
        self.current_bytes = 0
        self.truncated = False

    def write(self, s: str) -> int:
        s_len = len(s.encode("utf-8")) if isinstance(s, str) else len(s)
        if self.current_bytes + s_len > self.max_bytes:
            if not self.truncated:
                super().write("\n...[OUTPUT OOM PREVENTED]...\n")
                self.truncated = True
            return s_len
        self.current_bytes += s_len
        return super().write(s)


@contextlib.contextmanager
def isolated_argv(new_argv: Sequence[str]):
    """Safely substitutes sys.argv within a context block, restoring the original argv upon exit."""
    original_argv = list(sys.argv)
    sys.argv = list(new_argv)
    try:
        yield
    finally:
        sys.argv = original_argv


def _is_gevent_monkey_patched() -> bool:
    if "gevent.monkey" in sys.modules:
        monkey = sys.modules["gevent.monkey"]
        is_patched = getattr(monkey, "is_module_patched", None)
        if callable(is_patched) and is_patched("os"):
            return True
    return False


def run_job_module(
    module_name: str,
    *args: str,
    max_total_bytes: int = 5000,
    max_stderr_bytes: int = 3000,
    reload_module: bool = False,
    telemetry_hook: Optional[Callable[[], None]] = None,
) -> str:
    """
    Worker task: executes a python job module using runpy in an isolated process.
    Guards strictly against concurrent in-process thread execution or gevent monkey patching.
    """
    if _is_gevent_monkey_patched():
        raise RuntimeError(
            "run_job_module is strictly incompatible with gevent workers due to process-global state mutation. "
            "Use process-forked workers (e.g., RQ default worker)."
        )

    if not _job_execution_lock.acquire(blocking=False):
        raise RuntimeError(
            "run_job_module is strictly incompatible with concurrent thread execution within the same process. "
            "Process-global streams (sys.argv, sys.stdout) cannot be safely shared across concurrent threads."
        )

    try:
        if telemetry_hook is not None:
            try:
                telemetry_hook()
            except Exception as e:
                logger.warning(f"Telemetry hook failed before executing {module_name}: {e}")

        # Evict cached connections and optionally reload module
        if module_name in sys.modules:
            mod = sys.modules[module_name]
            for attr in ("_redis_client", "_redis_pool", "_redis_conn"):
                if hasattr(mod, attr) and getattr(mod, attr) is not None:
                    try:
                        getattr(mod, attr).close()
                    except Exception:
                        pass
                    setattr(mod, attr, None)
            if reload_module:
                try:
                    importlib.reload(mod)
                except Exception as e:
                    logger.warning(f"Failed to reload module {module_name}: {e}")

        f_out = BoundedStringIO()
        f_err = BoundedStringIO()
        error_to_raise: Optional[BaseException] = None

        with isolated_argv([module_name] + list(args)):
            with contextlib.redirect_stdout(f_out), contextlib.redirect_stderr(f_err):
                try:
                    runpy.run_module(module_name, run_name="__main__")
                except SystemExit as e:
                    if e.code != 0 and e.code is not None:
                        print(f"Execution failed with exit code {e.code}.", file=sys.stderr)
                        error_to_raise = RuntimeError(f"Job module '{module_name}' exited with code {e.code}")
                except Exception as e:
                    error_detail = {
                        "error": str(e),
                        "type": type(e).__name__,
                        "traceback": traceback.format_exc(),
                    }
                    print(json.dumps(error_detail), file=sys.stderr)
                    error_to_raise = e

        output = f_out.getvalue()
        err_output = f_err.getvalue()

        if len(err_output) > max_stderr_bytes:
            err_output = "...\n" + err_output[-max_stderr_bytes:]

        formatted_err = f"\n[stderr]:\n{err_output}" if err_output else ""
        max_stdout = max(0, max_total_bytes - len(formatted_err))

        if len(output) > max_stdout:
            output = output[:max_stdout] + "\n...[STDOUT TRUNCATED]..."

        final_output = output + formatted_err

        if error_to_raise is not None:
            raise JobExecutionError(final_output) from error_to_raise

        return final_output
    finally:
        _job_execution_lock.release()


def _extract_graph_instance(target: Union[str, Any]) -> Any:
    if isinstance(target, str):
        module = importlib.import_module(target)
        graph = None
        for attr in ("app", "graph", "agent"):
            val = getattr(module, attr, None)
            if val is not None:
                graph = val
                break
        if graph is None and hasattr(module, "create_graph"):
            graph = module.create_graph()
        if graph is None:
            raise AttributeError(f"Could not locate graph instance (app, graph, agent) in '{target}'.")
        return graph
    return target


def run_agent_graph(target: Union[str, Any], initial_state: Optional[Dict[str, Any]] = None) -> Any:
    """
    Native synchronous execution entrypoint for state graphs (e.g., LangGraph or custom workflow graphs).
    Avoids process-global stream mutations (sys.argv, sys.stdout) to ensure async compatibility.
    """
    graph = _extract_graph_instance(target)
    state = initial_state or {}
    if hasattr(graph, "invoke"):
        return graph.invoke(state)
    elif callable(graph):
        return graph(state)
    else:
        raise TypeError(f"Target graph object {type(graph)} is neither invokable nor callable.")


async def arun_agent_graph(target: Union[str, Any], initial_state: Optional[Dict[str, Any]] = None) -> Any:
    """
    Native asynchronous execution entrypoint for state graphs.
    Supports ainvoke, async callable graphs, or offloads sync invoke to a thread pool.
    """
    graph = _extract_graph_instance(target)
    state = initial_state or {}
    if hasattr(graph, "ainvoke"):
        return await graph.ainvoke(state)
    elif hasattr(graph, "invoke"):
        return await asyncio.to_thread(graph.invoke, state)
    elif inspect.iscoroutinefunction(graph):
        return await graph(state)
    elif callable(graph):
        return await asyncio.to_thread(graph, state)
    else:
        raise TypeError(f"Target graph object {type(graph)} is neither invokable nor callable.")
