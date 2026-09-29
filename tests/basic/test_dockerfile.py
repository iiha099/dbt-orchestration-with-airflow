import re

import dockerfile
import pytest


@pytest.fixture
def dockerfile_validation():
    return [
        ("FROM", "python:3.12.8-slim"),
        ("ARG", "DEBIAN_FRONTEND=noninteractive"),
        ("ENV", "PYTHONUNBUFFERED 1"),
        ("ENV", "AIRFLOW_HOME /app/airflow"),
        ("ENV", "DBT_DIR $AIRFLOW_HOME/dbt_lewagon"),
        ("ENV", "DBT_TARGET_DIR $DBT_DIR/target"),
        ("ENV", "DBT_PROFILES_DIR $DBT_DIR"),
        ("ENV", "DBT_VERSION 1.9.1"),
        ("WORKDIR", "$AIRFLOW_HOME"),
        ("COPY", "scripts scripts"),
        ("RUN", "chmod +x scripts/entrypoint.sh"),
        ("COPY", "pyproject.toml poetry.lock ./"),
        (
            "RUN",
            "pip3 install --upgrade --no-cache-dir pip && pip3 install poetry && poetry install --only main",
        ),
    ]


@pytest.mark.optional
def test_dockerfile(dockerfile_validation):
    dockerfile_name = "Dockerfile"
    dockerfile_parsed = dockerfile.parse_file(dockerfile_name)

    assert len(dockerfile_parsed) == len(dockerfile_validation)

    for command, validation in zip(dockerfile_parsed, dockerfile_validation):
        assert command.cmd == validation[0]
        if command.cmd == "RUN":
            s = re.sub(r"\s+", " ", command.value[0])
            assert s == validation[1]
        elif len(command.flags) != 0:
            assert (
                " ".join([" ".join(command.flags), " ".join(command.value)])
                == validation[1]
            )
        else:
            assert " ".join(command.value) == validation[1]
