.PHONY: test_docker_compose test_dockerfile test_profiles_yml test_dag_and_task_basic test_dag_and_task_advanced test

# Our tests will re-create a SQLite copy of your postgres db inside the "temp" folder, and then test against this sqlite db
AIRFLOW_HOME=${PWD}/tests/temp
DBT_DIR=${PWD}/dbt_lewagon

test_docker_compose:
	pytest -v --disable-warnings tests/basic/test_docker_compose.py

test_dockerfile:
	pytest -v --disable-warnings tests/basic/test_dockerfile.py

test_profiles_yml:
	pytest -v --disable-warnings tests/basic/test_profiles_yml.py

test_dag_and_task_basic:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	export DBT_DIR=${DBT_DIR}; \
	airflow db migrate; \
	pytest -v --disable-warnings -m "not optional" tests/basic/test_dag_and_tasks_configs.py --color=yes

test_dag_and_task_advanced:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	export DBT_DIR=${DBT_DIR}; \
	airflow db migrate; \
	pytest -v --disable-warnings -m "not optional" tests/advanced/test_dag_config.py tests/advanced/test_tasks_configs.py --color=yes

test:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	export DBT_DIR=${DBT_DIR}; \
	airflow db migrate; \
	pytest -v --disable-warnings -m "not optional" tests 2>&1 > test_output.txt || echo "-- Some tests didn't pass, let's look at the output"; \
	pytest -v --disable-warnings -m "not optional" tests --color=yes
