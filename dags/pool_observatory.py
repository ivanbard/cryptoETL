"""Hourly collection with replayable batches; no historical API backfill."""

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import subprocess
from uuid import NAMESPACE_URL, uuid5

from airflow.sdk import dag, get_current_context, task


ROOT = Path("/app")
PYTHON = "/opt/pipeline/bin/python"


def report_failure(context):
    instance = context["task_instance"]
    logging.error("Pool pipeline failed: dag=%s run=%s task=%s; inspect task logs in Airflow",
                  instance.dag_id, instance.run_id, instance.task_id)


@dag(
    dag_id="pool_observatory",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=10), "on_failure_callback": report_failure},
    tags=["crypto", "etl"],
)
def pool_observatory():
    @task
    def collect_archive():
        context = get_current_context()
        batch_id = str(uuid5(NAMESPACE_URL, f"pool_observatory:{context['run_id']}"))
        subprocess.run([PYTHON, "pipeline.py", "collect", "--archive-only", "--run-id", batch_id],
                       cwd=ROOT, check=True, stderr=subprocess.STDOUT)
        # Only the batch path travels through XCom; observations stay on disk.
        return str(ROOT / "data" / "raw" / batch_id)

    @task
    def load_batch(batch_path):
        subprocess.run([PYTHON, "pipeline.py", "replay", batch_path],
                       cwd=ROOT, check=True, stderr=subprocess.STDOUT)

    @task
    def build_analytics():
        subprocess.run(["/opt/pipeline/bin/dbt", "build", "--project-dir", "dbt", "--profiles-dir", "."],
                       cwd=ROOT, check=True, stderr=subprocess.STDOUT)

    archived = collect_archive()
    loaded = load_batch(archived)
    loaded >> build_analytics()


pool_observatory()
