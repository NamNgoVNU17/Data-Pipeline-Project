"""
Monitoring Layer: Push custom metrics tới Prometheus via StatsD / pushgateway.
"""
import os, time, logging, socket
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

STATSD_HOST = os.getenv("STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("STATSD_PORT", "9125"))
NAMESPACE   = os.getenv("PIPELINE_NAMESPACE", "data_pipeline")


def _send_statsd(metric: str, value: float, metric_type: str = "g") -> None:
    """Gửi metric đến StatsD qua UDP."""
    message = f"{NAMESPACE}.{metric}:{value}|{metric_type}"
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode(), (STATSD_HOST, STATSD_PORT))
    except Exception as e:
        logger.warning(f"[METRICS] StatsD send failed: {e}")


def emit_row_count(dataset: str, layer: str, count: int) -> None:
    _send_statsd(f"{layer}.{dataset}.row_count", count, "g")
    logger.info(f"[METRICS] {layer}.{dataset}.row_count = {count}")


def emit_quality_score(dataset: str, pass_rate: float) -> None:
    _send_statsd(f"quality.{dataset}.pass_rate", pass_rate * 100, "g")
    logger.info(f"[METRICS] quality.{dataset}.pass_rate = {pass_rate:.2%}")


def emit_task_duration(task: str, duration_s: float) -> None:
    _send_statsd(f"task.{task}.duration_seconds", duration_s, "g")


def emit_pipeline_failure(dag_id: str, task_id: str) -> None:
    _send_statsd(f"failure.{dag_id}.{task_id}", 1, "c")  # counter
    logger.error(f"[METRICS] Pipeline failure: {dag_id}/{task_id}")


def track_duration(task_name: str) -> Callable:
    """Decorator đo thời gian chạy và emit metric."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start = time.monotonic()
            try:
                result = func(*args, **kwargs)
                duration = time.monotonic() - start
                emit_task_duration(task_name, duration)
                return result
            except Exception as e:
                emit_pipeline_failure("unknown", task_name)
                raise
        return wrapper
    return decorator
