"""Utility gửi alert ra Slack / Email."""
import os, json, logging, urllib.request
from typing import Any

logger = logging.getLogger(__name__)


def send_slack_alert(message: str, level: str = "error") -> None:
    """Gửi message tới Slack webhook."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping alert.")
        return

    emoji = {"error": "❌", "warning": "⚠️", "info": "✅"}.get(level, "ℹ️")
    payload = json.dumps({"text": f"{emoji} *[DataPipeline]* {message}"}).encode()
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5):
        pass


def airflow_on_failure_callback(context: dict[str, Any]) -> None:
    """Callback cho Airflow DAG on_failure_callback."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_dt = context["execution_date"]
    log_url = context["task_instance"].log_url
    send_slack_alert(
        f"DAG `{dag_id}` | Task `{task_id}` FAILED\n"
        f"Execution: {exec_dt}\nLog: {log_url}",
        level="error",
    )


def airflow_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback khi SLA bị vi phạm."""
    send_slack_alert(
        f"SLA MISS on DAG `{dag.dag_id}`: tasks {[t.task_id for t in task_list]}",
        level="warning",
    )
