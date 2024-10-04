import dagster as dg
from databricks.sdk.service import jobs
from dagster_databricks import PipesDatabricksClient

@dg.asset
def databricks_notebook(context: dg.AssetExecutionContext,
    pipes_databricks_client: PipesDatabricksClient,) -> dg.MaterializeResult:
    # cluster config
    cluster_config = {
        "num_workers": 1,
        "spark_version": "11.2.x-scala2.12",
        "node_type_id": "i3.xlarge",
    }
    # task specification will be passed to Databricks as-is, except for the
    # injection of environment variables
    task = jobs.SubmitTask.from_dict(
        {
            "new_cluster": cluster_config,
            "libraries": [
                # must include dagster-pipes
                {"pypi": {"package": "dagster-pipes"}},
            ],
            "task_key": "dagster-launched",
            "spark_python_task": {
                "python_file": "dbfs:/FileStore/external_databricks_script.py",
                "source": jobs.Source.WORKSPACE,
            },
        }
    )
    # Arbitrary json-serializable data you want access to from the `PipesSession`
    # in the Databricks runtime. Assume `sample_rate` is a parameter used by
    # the target job's business logic.
    extras = {"sample_rate": 1.0}

    # synchronously execute the databricks job
    return pipes_databricks_client.run(
        task=task,
        context=context,
        extras=extras,
    ).get_materialize_result()


