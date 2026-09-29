import os
import pytest
import yaml


@pytest.mark.optional
def test_profiles_yml():
    assert "profiles.yml" in os.listdir("dbt_lewagon")
    with open("dbt_lewagon/profiles.yml", "r") as stream:
        profiles = yaml.safe_load(stream)
        assert "dbt_lewagon" in profiles
        assert "outputs" in profiles["dbt_lewagon"]
        assert "dev" in profiles["dbt_lewagon"]["outputs"]
        dev = profiles["dbt_lewagon"]["outputs"]["dev"]
        assert set(dev.keys()) == {
            "dataset",
            "job_execution_timeout_seconds",
            "job_retries",
            "location",
            "method",
            "priority",
            "project",
            "threads",
            "type",
            "keyfile",
        }
        assert dev["dataset"].startswith("dbt_")
        assert dev["dataset"].endswith("day2")
        assert dev["job_execution_timeout_seconds"] == 300
        assert dev["job_retries"] == 1
        assert dev["location"] == "EU"
        assert dev["method"] == "service-account"
        assert dev["priority"] == "interactive"
        assert dev["project"] is not None
        assert dev["threads"] == 1
        assert dev["type"] == "bigquery"
        assert dev["keyfile"].startswith("/app/airflow/.gcp_keys/")
        assert dev["keyfile"].endswith(".json")
