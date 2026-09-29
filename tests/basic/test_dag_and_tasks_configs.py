import os

import pendulum
from airflow.models import DagBag

DAG_BAG = os.path.join(os.path.dirname(__file__), "../../dags/basic")
os.environ["AIRFLOW_HOME"] = "/app/airflow"
os.environ["DBT_DIR"] = "/app/airflow/dbt_lewagon"


class TestDagConfig:
    dagbag = DagBag(dag_folder=DAG_BAG, include_examples=False)

    def test_dag_config(self):
        assert self.dagbag.import_errors == {}
        dag = self.dagbag.get_dag(dag_id="dbt_basics")
        assert dag.schedule_interval == "@daily"
        assert dag.catchup is False
        assert dag.default_args == {
            "depends_on_past": True,
        }
        assert dag.start_date == pendulum.today("UTC").add(days=-1)

    def test_tasks(self):
        dag = self.dagbag.get_dag(dag_id="dbt_basics")
        assert list(map(lambda task: task.task_id, dag.tasks)) == [
            "dbt_run",
            "dbt_test",
        ]

    def test_dbt_run_task(self):
        dag = self.dagbag.get_dag(dag_id="dbt_basics")
        task = dag.get_task("dbt_run")
        assert task.__class__.__name__ == "BashOperator"
        assert task.bash_command == "dbt run --project-dir /app/airflow/dbt_lewagon"
        assert list(map(lambda task: task.task_id, task.upstream_list)) == []
        assert list(map(lambda task: task.task_id, task.downstream_list)) == [
            "dbt_test"
        ]

    def test_dbt_test_task(self):
        dag = self.dagbag.get_dag(dag_id="dbt_basics")
        task = dag.get_task("dbt_test")
        assert task.__class__.__name__ == "BashOperator"
        assert task.bash_command == "dbt test --project-dir /app/airflow/dbt_lewagon"
        assert list(map(lambda task: task.task_id, task.upstream_list)) == ["dbt_run"]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []
