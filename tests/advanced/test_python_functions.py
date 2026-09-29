import os

from airflow import DAG
from dags.advanced import dbt_advanced
from pendulum.datetime import DateTime
from pendulum.tz.timezone import Timezone

DBT_DIR = os.getenv("DBT_DIR")


#### Manifest directories ####
# In prod the directory would be: dbt/target/manifest.json
# Paths have been changed to: dbt/manifest.json
# Because *target/ is in this codebase's .gitignore
# And we need the manifest.json's to test against.


def test_load_manifest():
    data = dbt_advanced.load_manifest("tests/advanced/dbt_light/manifest.json")
    assert list(data.keys()) == [
        "metadata",
        "nodes",
        "sources",
        "macros",
        "docs",
        "exposures",
        "selectors",
        "disabled",
        "parent_map",
        "child_map",
    ]
    assert list(data["nodes"]) == [
        "model.dbt_lewagon.my_first_dbt_model",
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321",
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710",
    ]
    assert data["child_map"] == {
        "model.dbt_lewagon.my_first_dbt_model": [
            "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710",
            "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321",
        ]
    }
    assert data["parent_map"] == {
        "model.dbt_lewagon.my_first_dbt_model": [],
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321": [
            "model.dbt_lewagon.my_first_dbt_model"
        ],
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710": [
            "model.dbt_lewagon.my_first_dbt_model"
        ],
    }

    data = dbt_advanced.load_manifest("tests/advanced/dbt/manifest.json")
    assert list(data.keys()) == [
        "metadata",
        "nodes",
        "sources",
        "macros",
        "docs",
        "exposures",
        "selectors",
        "disabled",
        "parent_map",
        "child_map",
    ]
    assert list(data["nodes"]) == [
        "model.dbt_lewagon.my_first_dbt_model",
        "model.dbt_lewagon.my_second_dbt_model",
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321",
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710",
        "test.dbt_lewagon.unique_my_second_dbt_model_id.57a0f8c493",
        "test.dbt_lewagon.not_null_my_second_dbt_model_id.151b76d778",
    ]
    assert data["child_map"] == {
        "model.dbt_lewagon.my_first_dbt_model": [
            "model.dbt_lewagon.my_second_dbt_model",
            "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710",
            "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321",
        ],
        "model.dbt_lewagon.my_second_dbt_model": [
            "test.dbt_lewagon.not_null_my_second_dbt_model_id.151b76d778",
            "test.dbt_lewagon.unique_my_second_dbt_model_id.57a0f8c493",
        ],
    }
    assert data["parent_map"] == {
        "model.dbt_lewagon.my_first_dbt_model": [],
        "model.dbt_lewagon.my_second_dbt_model": [
            "model.dbt_lewagon.my_first_dbt_model"
        ],
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321": [
            "model.dbt_lewagon.my_first_dbt_model"
        ],
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710": [
            "model.dbt_lewagon.my_first_dbt_model"
        ],
        "test.dbt_lewagon.unique_my_second_dbt_model_id.57a0f8c493": [
            "model.dbt_lewagon.my_second_dbt_model"
        ],
        "test.dbt_lewagon.not_null_my_second_dbt_model_id.151b76d778": [
            "model.dbt_lewagon.my_second_dbt_model"
        ],
    }


def test_create_tasks():
    """
    Tests the output dictionary of dbt_advanced.py::create_tasks()
    """
    data = dbt_advanced.load_manifest("tests/advanced/dbt_light/manifest.json")
    dir_locations = f"--project-dir {DBT_DIR}"
    bash_command_by_node_id = {
        "model.dbt_lewagon.my_first_dbt_model": f"dbt run --models my_first_dbt_model {dir_locations}",
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321": f"dbt test --models unique_my_first_dbt_model_id {dir_locations}",
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710": f"dbt test --models not_null_my_first_dbt_model_id {dir_locations}",
    }

    tasks = dbt_advanced.create_tasks(data)
    assert list(tasks.keys()) == list(bash_command_by_node_id.keys())

    for task_name, bash_command in bash_command_by_node_id.items():
        task = tasks[task_name]
        assert task.__class__.__name__ == "BashOperator"
        assert task.bash_command == bash_command

    data = dbt_advanced.load_manifest("tests/advanced/dbt/manifest.json")
    bash_command_by_node_id = {
        "model.dbt_lewagon.my_first_dbt_model": f"dbt run --models my_first_dbt_model {dir_locations}",
        "model.dbt_lewagon.my_second_dbt_model": f"dbt run --models my_second_dbt_model {dir_locations}",
        "test.dbt_lewagon.unique_my_first_dbt_model_id.16e066b321": f"dbt test --models unique_my_first_dbt_model_id {dir_locations}",
        "test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710": f"dbt test --models not_null_my_first_dbt_model_id {dir_locations}",
        "test.dbt_lewagon.unique_my_second_dbt_model_id.57a0f8c493": f"dbt test --models unique_my_second_dbt_model_id {dir_locations}",
        "test.dbt_lewagon.not_null_my_second_dbt_model_id.151b76d778": f"dbt test --models not_null_my_second_dbt_model_id {dir_locations}",
    }

    tasks = dbt_advanced.create_tasks(data)
    assert list(tasks.keys()) == list(bash_command_by_node_id.keys())

    for task_name, bash_command in bash_command_by_node_id.items():
        task = tasks[task_name]
        assert task.__class__.__name__ == "BashOperator"
        assert task.bash_command == bash_command


def test_create_dags_dependencies():
    start_date = DateTime(2022, 6, 10, 0, 0, 0, tzinfo=Timezone("UTC"))

    with DAG(
        dag_id="dbt",
        schedule="@monthly",
        default_args={"start_date": start_date},
    ) as dag:
        data = dbt_advanced.load_manifest("tests/advanced/dbt_light/manifest.json")
        dbt_advanced.create_dags_dependencies(data, dbt_advanced.create_tasks(data))

    task = dag.get_task("model.dbt_lewagon.my_first_dbt_model")
    assert list(map(lambda task: task.task_id, task.upstream_list)) == []
    downstream_list = list(map(lambda task: task.task_id, task.downstream_list))
    assert "test.dbt_lewagon.unique_my_first_dbt_model_id" in downstream_list
    assert "test.dbt_lewagon.not_null_my_first_dbt_model_id" in downstream_list

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

    with DAG(
        dag_id="dbt",
        schedule="@monthly",
        default_args={"start_date": start_date},
    ) as dag:
        data = dbt_advanced.load_manifest("tests/advanced/dbt/manifest.json")
        dbt_advanced.create_dags_dependencies(data, dbt_advanced.create_tasks(data))

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
