import os

import pendulum
from airflow.models import DagBag

DAG_BAG = os.path.join(os.path.dirname(__file__), "../../dags/advanced")


class TestDagConfig:
    dagbag = DagBag(dag_folder=DAG_BAG, include_examples=False)

    def test_dag_config(self):
        assert self.dagbag.import_errors == {}
        dag = self.dagbag.get_dag(dag_id="dbt_advanced")
        assert dag.schedule_interval == "@daily"
        assert dag.catchup is True
        assert dag.default_args == {
            "depends_on_past": False,
        }
        assert dag.start_date == pendulum.today("UTC").add(days=-1)
