# 🎯 Goal

If you are using dbt core, there is no built in way to orchestrate your dbt runs - Airflow can fill that need.
In this challenge, you'll use Airflow to orchestrate your dbt jobs.

For that purpose, we will *mount* a live copy of our DBT models *inside* the Airflow instance.

Like usual, will have 3 containers running:
- A postgres database storing Airflow metadata
- Two Airflow services (scheduler + webserver), in which we will **mount** our DBT folders.

The *actual datasets and models* will then be saved in Big Query.

<br>

# 1️⃣ Setup

We have already created a DBT project (`dbt_lewagon`) that contains the basic models coming from `dbt init`, which will be enough to test your setup.

## 1.1. Dockerfile
First, let's note that we already added `dbt-core` and `dbt-bigquery` to your `pyproject.toml`, that Airflow will use.

There are several environment variables to set so Airflow knows which folders to look up when running `dbt` commands. Open your `Dockerfile` and add the following lines after `ENV AIRFLOW_HOME=/app/airflow`:

```dockerfile
ENV DBT_DIR=$AIRFLOW_HOME/dbt_lewagon
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.9.1
```

You may have noticed that you set `$DBT_PROFILES` to `/app/airflow/dbt_lewagon`, which means that you will have to create a `profiles.yml` in this folder - don't do it for now, we'll go through this a bit further down.

🧪 Once you are confident with what you've done, check with:

```bash
make test_dockerfile
```

## 1.2. docker-compose.yml

You should just have to mount two volumes in your `airflow scheduler` to sync:
- your local `dbt_lewagon` folder to your docker container
- your local `.gcp_keys` folder to your docker container - you will probably have to set the entire path to your `.gcp_keys`, like `/Users/username/.gcp_keys:/app/airflow/.gcp_keys`

🧪 Once you are confident with what you've done, run the tests:

```bash
make test_docker_compose
```

Before moving to the next part, create and fill your `.env` file as usual with what's needed.

## 1.3. Setup files

In order to run DBT with its own configuration, Airflow needs a `profiles.yml` in the `dbt_lewagon` folder:
- It should contain a `dbt_lewagon` profile
- With a `dev` output like:
    ```yml
    dataset: dbt_write_your_name_here_day2
    job_execution_timeout_seconds: 300
    job_retries: 1
    location: EU
    method: service-account
    priority: interactive
    project: # Your google cloud project name
    threads: 1
    type: bigquery
    keyfile: /app/airflow/.gcp_keys/the_name_of_your_keyfile.json
    ```
- And `target` should point to `dev`

🧪 Once you are confident with what you've done, run the tests:

```bash
make test_profiles_yml
```

<br>

# 2️⃣ Basic DAG: dbt run ➡ dbt test

The goal is to have DBT installed on Airflow and to have a DAG with two tasks that trigger `dbt run` and `dbt test`.

## 2.1. `dags/basic/dbt_basic.py`

❓ Open the `dbt_basics.py` file and add 2 tasks inside the DAG:

1. `dbt_run` BashOperator that runs DBT models. Be careful, you will have to specify the dbt_dir folder, check the docs at [this link](https://docs.getdbt.com/dbt-cli/configure-your-profile#advanced-customizing-a-profile-directory).
2. `dbt_test` BashOperator that run DBT tests. Be careful, you will have to specify the dbt_dir folder, check the docs at [this link](https://docs.getdbt.com/dbt-cli/configure-your-profile#advanced-customizing-a-profile-directory).

## 2.2. Run it!

Run your DAG by unpausing it from the UI (or from the command line like a boss 😎, with `airflow dags unpause <dag_id>` but with in the correct docker context)

Check that your setup worked by opening your [BigQuery console](https://console.cloud.google.com/bigquery) and verify that you have a new dataset named `dbt_your_name_name_day2` that contains your two models.

When running this DAG you should have the `dbt_test` failing, but this is normal, remember that this is how the `dbt_init` was built. However, make sure that the error you have is the expected one:

```markdown
Failure in test not_null_my_first_dbt_model_id (models/example/schema.yml)
Got 1 result, configured to fail if != 0
```

🧪 Once you are confident with what you've done, run the tests:

```bash
make test_dag_and_task_basic
```

💡 This setup is a great foundation and would scale with any other DBT Project: Just replace the `dbt_lewagon` folder with the project that you've done in the previous day, make sure that it runs properly and go to BigQuery to check that your models have been created. It does lack some granularity though.

🧪 Run all tests at once and git add, commit, and push your code to github!

```bash
make test
```

<br>

# 3️⃣ Advanced DAG: one task per DBT model (Optional)

## 3.1. Setup

In previous section we integrated DBT to Airflow at a **project level**: One task was **run all dbt models**, while the second was just **run all dbt tests**. You could have done the same with a github action that would trigger `dbt test` on each pull-request.

In this exercise, you will integrate it at a **model level**. This means that your goal will be to have **a DAG containing one task per model**.

👉 First let's change our target bigquery database by **updating your dbt/profile.yml**:

```yml
dataset: dbt_..._day2_advanced
```

## 3.2. The DAG

**💡 How will we proceed?**

Currently we only have 2 DBT models, but imagine we had hundreds of them! We don't want to write hundreds of Airflow tasks manually!

Thankfully, DBT generates a **[`manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json)** file that we can parse!
- Have a look at `dbt_lewagon/manifest.json` - this file contains all models and their dependencies.
- PS: In reality, this manifest is much longer and is located at `dbt_lewagon/target/manifest.json`. We have trimmed down the `manifest.json` to keep only the needed parts and make it more readable.

👉 You won't manually declare your task but you will **generate them programmatically by using python functions**. To help you understand this new concept, called meta-programming, we already added the function calls and you just have to code the functions themselves.

❓ **Open the `dbt_advanced.py` file and check the DAG inside. Your goal is to fill the python functions accordingly.**

Here is a summary of the flow:
- The `load_manifest` function will be called first and will be given the `manifest.json` path that it will load as a `dict` and return
- The returned `dict` will be given to the `create_tasks` function that will build a dict containing the `nodes` of the `manifest.json` as keys and their corresponding BashOperators as values
- These `BashOperators` will be built by the `make_dbt_task` function that will be called by the `create_tasks` function for each node by giving the proper dbt verb (`run` or `test`)
- The `create_dags_dependencies` function will reuse this `dict` to create the Airflow tasks. To order them properly, this function will have to manipulate the `depends_on` field of the `manifest.json`

🏋🏽‍♂️ This exercise is quite challenging to implement, so do not hesitate to check with a teacher that you have properly understood the requirements before starting.

At the end, you should have a DAG that looks similar to this:
<img src="https://wagon-public-datasets.s3.amazonaws.com/data-engineering/W2D3/dbt_dag.png">

As always, once you're confident with what you have done, try to run it and see if it worked on BigQuery!

🧪 Then to check against the tests, run:

```bash
make test_dag_and_task_advanced
```

Again, do not hesitate to apply it to your own models!

<br>

# 🏁 Finishing Up

Congratulations! You have algorithmically parsed a DBT `manifest.json` to generate an Airflow execution graph. That is no small task! 🎉

🧪 Run all tests with:

```bash
make test
```

And don't forget to git add, commit, and push your code to Github to track your progress on Kitt!

<br>
