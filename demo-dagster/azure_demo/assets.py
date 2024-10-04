import dagster as dg
from dagster_azure.adls2 import ADLS2Resource
from azure_demo.utils import launch_and_poll_job, launch_and_poll_databricks_job
from dagster_databricks import PipesDatabricksClient
from databricks.sdk.service import jobs

from azure_demo.resources import AzureFunctionResource, AzureDataFactoryResource


@dg.asset(
        group_name="workflow_a",
)
def task_1__invoke_adf_pipeline(context: dg.AssetExecutionContext,
                        adf_client: AzureDataFactoryResource) -> None:
    context.log.info("Invoking ADF pipeline")
    run = adf_client.create_run("DefaultResourceGroup-CCAN", "datafactoryjhcklxpqiqeg4", "ArmtemplateSampleCopyPipeline")
    context.log.info(f"ADF pipeline run: {run}")   
    run_response = adf_client.wait_for_run("DefaultResourceGroup-CCAN", "datafactoryjhcklxpqiqeg4", run.run_id)
    return run_response

@dg.asset(
        deps=[task_1__invoke_adf_pipeline],
        group_name="workflow_a",
        required_resource_keys={"databricks"}
        )
def task_2__invoke_databricks_workflow(context: dg.AssetExecutionContext) -> None:
    databricks = context.resources.databricks
    launch_and_poll_databricks_job(context, databricks, 733330858351118)



@dg.asset(
        deps=[task_1__invoke_adf_pipeline],
        group_name="workflow_a"
        )
def task_3__invoke_databricks_notebook( context: dg.AssetExecutionContext,
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


@dg.asset
def adls2_file_asset(context: dg.AssetExecutionContext,
                      adls2_resource: ADLS2Resource) -> None:
    # List files in the specified directory
    service_client = adls2_resource.adls2_client
    file_system_client = service_client.get_file_system_client("test-container")
    paths = file_system_client.get_paths()
    for path in paths:
        context.log.info(f"Path: {path.name}, Last modified: {path.last_modified}")

@dg.asset_check(
       asset=adls2_file_asset 
)
def check_country_stats(adls2_file_asset):
    #put logic here
    return dg.AssetCheckResult(passed=True)

@dg.asset(
        deps=[task_2__invoke_databricks_workflow, task_3__invoke_databricks_notebook],
        group_name="workflow_a",
        )
def task_4__azure_function_asset(context: dg.AssetExecutionContext, azure_function: AzureFunctionResource) -> None:
    try:
        azure_return_value = azure_function.invoke_azure_function(
            function_app_name="returnoneorzerodemo", function_name="random_choice"
        )
        context.log.info(f"Random choice: {azure_return_value.text}")
        if int(azure_return_value.text) == 0:
            raise ValueError("Random exception")
        pass
    except ValueError as e:
        context.log.info("Invoking Azure function on retry")
        raise dg.RetryRequested(max_retries=3) from e        
        

def task_1__invoke_azure_function(context: dg.AssetExecutionContext,
                            azure_function: AzureFunctionResource) -> None:
    azure_return_value = azure_function.invoke_azure_function(
            function_app_name="returnoneorzerodemo", function_name="random_choice"
        )
    context.log.info(f"Random choice: {azure_return_value.text}")
    return int(azure_return_value.text)

def task_2__invoke_workflow_d(context: dg.AssetExecutionContext) -> str:
    launch_and_poll_job(context=context, job_name="workflow_d")
    return "success"

def task_3__databricks_notebook_b1_north_eu(context: dg.AssetExecutionContext) -> str:
    databricks = context.resources.databricks
    launch_and_poll_databricks_job(context, databricks, 982908164270868)
    return "success"


class AzureFunctionOverrideConfig(dg.Config):
    simulated_azure_function_return_value: int = 3


@dg.multi_asset(  
        group_name="workflow_b",
        specs=[
            dg.AssetSpec("task_1__invoke_azure_function", skippable=True),
            dg.AssetSpec("task_2__invoke_workflow_d", deps=["task_1__invoke_azure_function"], skippable=True),
            dg.AssetSpec("task_3__databricks_notebook_b1_north_eu", deps=["task_1__invoke_azure_function"], skippable=True),
            ],
        can_subset=True,
        required_resource_keys={"azure_function", "databricks"},
)
def workflow_b_assets(context: dg.AssetExecutionContext, config:AzureFunctionOverrideConfig) -> None:
    
    if dg.AssetKey("task_1__invoke_azure_function") in context.op_execution_context.selected_asset_keys:
        task_1_result = task_1__invoke_azure_function(context, azure_function=context.resources.azure_function)
        yield dg.MaterializeResult(asset_key="task_1__invoke_azure_function", metadata={"value": task_1_result})

    if config.simulated_azure_function_return_value != 3:
        task_1_result = config.simulated_azure_function_return_value
    task_2_result = None
    task_3_result = None

    if task_1_result == 1 and dg.AssetKey("task_2__invoke_workflow_d") in context.op_execution_context.selected_asset_keys:
        task_2_result = task_2__invoke_workflow_d(context)
        yield dg.MaterializeResult(asset_key="task_2__invoke_workflow_d")

    if task_1_result == 0 and dg.AssetKey("task_3__databricks_notebook_b1_north_eu") in context.op_execution_context.selected_asset_keys:
        task_3_result = task_3__databricks_notebook_b1_north_eu(context)
        yield dg.MaterializeResult(asset_key="task_3__databricks_notebook_b1_north_eu")

    if "success" in [task_2_result, task_3_result]:
        context.log_event(
        dg.AssetMaterialization(
            asset_key="dashboard/hooli_example",
            description="External asset materialized",
        )
    )



@dg.asset(
        group_name="workflow_d",
        required_resource_keys={"databricks"}
        )
def task_1__invoke_databricks_workflow(context: dg.AssetExecutionContext) -> None:
    databricks = context.resources.databricks
    launch_and_poll_databricks_job(context, databricks, 733330858351118)