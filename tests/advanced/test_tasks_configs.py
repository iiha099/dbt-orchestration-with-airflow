import os

from airflow.models import DagBag

DAG_BAG = os.path.join(os.path.dirname(__file__), "../../dags/advanced")
DBT_DIR = os.getenv("DBT_DIR")


class TestTasksConfigs:
    dagbag = DagBag(dag_folder=DAG_BAG, include_examples=False)

    def test_dag_tasks(self):
        """
        Testing against the tasks in the actual Airflow DAG after they have been dynamically generated
        with dbt_advanced.py::create_dags_dependencies()
        """

        dag = self.dagbag.get_dag(dag_id="dbt_advanced")
        dir_locations = f"--project-dir {DBT_DIR}"
        bash_command_by_task_name = {
            "model.dbt_lewagon.my_first_dbt_model": f"dbt run --models my_first_dbt_model {dir_locations}",
            "model.dbt_lewagon.my_second_dbt_model": f"dbt run --models my_second_dbt_model {dir_locations}",
            "test.dbt_lewagon.unique_my_first_dbt_model_id": f"dbt test --models unique_my_first_dbt_model_id {dir_locations}",
            "test.dbt_lewagon.not_null_my_first_dbt_model_id": f"dbt test --models not_null_my_first_dbt_model_id {dir_locations}",
            "test.dbt_lewagon.unique_my_second_dbt_model_id": f"dbt test --models unique_my_second_dbt_model_id {dir_locations}",
            "test.dbt_lewagon.not_null_my_second_dbt_model_id": f"dbt test --models not_null_my_second_dbt_model_id {dir_locations}",
        }

        assert list(map(lambda task: task.task_id, dag.tasks)) == list(
            bash_command_by_task_name.keys()
        )

        for task_name, bash_command in bash_command_by_task_name.items():
            task = dag.get_task(task_name)
            assert task.__class__.__name__ == "BashOperator"
            assert task.bash_command == bash_command

        task = dag.get_task("model.dbt_lewagon.my_first_dbt_model")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == []
        downstream_list = list(map(lambda task: task.task_id, task.downstream_list))
        assert "model.dbt_lewagon.my_second_dbt_model" in downstream_list
        assert "test.dbt_lewagon.unique_my_first_dbt_model_id" in downstream_list
        assert "test.dbt_lewagon.not_null_my_first_dbt_model_id" in downstream_list

        task = dag.get_task("model.dbt_lewagon.my_second_dbt_model")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "model.dbt_lewagon.my_first_dbt_model"
        ]
        downstream_list = list(map(lambda task: task.task_id, task.downstream_list))
        assert "test.dbt_lewagon.unique_my_second_dbt_model_id" in downstream_list
        assert "test.dbt_lewagon.not_null_my_second_dbt_model_id" in downstream_list

        task = dag.get_task("test.dbt_lewagon.unique_my_first_dbt_model_id")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "model.dbt_lewagon.my_first_dbt_model"
        ]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []

        task = dag.get_task("test.dbt_lewagon.not_null_my_first_dbt_model_id")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "model.dbt_lewagon.my_first_dbt_model"
        ]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []

        task = dag.get_task("test.dbt_lewagon.unique_my_second_dbt_model_id")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "model.dbt_lewagon.my_second_dbt_model"
        ]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []

        task = dag.get_task("test.dbt_lewagon.not_null_my_second_dbt_model_id")
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "model.dbt_lewagon.my_second_dbt_model"
        ]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []
