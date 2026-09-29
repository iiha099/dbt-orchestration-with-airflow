import lewagonde
from yaml import load
from yaml.loader import SafeLoader
import pytest


@pytest.mark.optional
def test_docker_compose():
    with open("docker-compose.yml") as f:
        docker_compose_data = load(f, SafeLoader)

    assert set(docker_compose_data.get("services", {}).keys()) == {
        "postgres",
        "scheduler",
        "webserver",
    }
    scheduler = docker_compose_data["services"]["scheduler"]

    assert lewagonde.dict_or_kvlist_to_dict(scheduler.get("environment")) == {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:$POSTGRES_PASSWORD@postgres:5432/db",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }

    for expected_volume in [
        "./dags:/app/airflow/dags",
        "./data:/app/airflow/data",
        "./logs:/app/airflow/logs",
        "./dbt_lewagon:/app/airflow/dbt_lewagon",
    ]:
        assert expected_volume in scheduler.get("volumes")

    assert any(
        "/.gcp_keys:/app/airflow/.gcp_keys" in volume
        for volume in scheduler.get("volumes")
    )

    webserver = docker_compose_data["services"]["webserver"]

    assert set(webserver.get("volumes")) == {
        "./dags:/app/airflow/dags",
        "./data:/app/airflow/data",
        "./logs:/app/airflow/logs",
    }
