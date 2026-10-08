"""Run inside the Airflow image to verify DAG loading and recovery boundaries."""

from importlib.util import find_spec
from pathlib import Path
import unittest
from unittest.mock import patch


@unittest.skipUnless(find_spec("airflow"), "Run in the Airflow container")
class AirflowCheck(unittest.TestCase):
    def test_dag_and_batch_identity(self):
        from airflow.models import DagBag

        bag = DagBag(dag_folder=str(Path(__file__).parent / "dags"))
        self.assertEqual(bag.import_errors, {})
        dag = bag.dags["pool_observatory"]
        self.assertEqual(set(dag.task_ids), {"collect_archive", "load_batch", "build_analytics"})
        self.assertEqual(dag.get_task("load_batch").upstream_task_ids, {"collect_archive"})
        self.assertEqual(dag.get_task("build_analytics").upstream_task_ids, {"load_batch"})
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        for task in dag.tasks:
            self.assertEqual(task.retries, 2)
            self.assertIsNotNone(task.execution_timeout)

        collect = dag.get_task("collect_archive").python_callable
        with patch("subprocess.run") as run:
            with patch.dict(collect.__globals__, {"get_current_context": lambda: {"run_id": "run-a"}}):
                first = collect()
                self.assertEqual(collect(), first)
            with patch.dict(collect.__globals__, {"get_current_context": lambda: {"run_id": "run-b"}}):
                self.assertNotEqual(collect(), first)
            self.assertTrue(run.call_args.kwargs["check"])


if __name__ == "__main__":
    unittest.main()
