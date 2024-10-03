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
    #context.log.info(f"ADF pipeline run status: {run_response.status}")
    return run_response

# Upon successful completion of the ADF pipeline, Task 2 invokes an Azure Databricks Workflow. 
# Databricks Workflow should be in Databricks Workspace A1 and in the East US region.
@dg.asset(
        deps=[task_1__invoke_adf_pipeline],
        group_name="workflow_a",
        required_resource_keys={"databricks"}
        )
def task_2__invoke_databricks_workflow(context: dg.AssetExecutionContext) -> None:
    databricks = context.resources.databricks
    launch_and_poll_databricks_job(context, databricks, 733330858351118)


# In parallel to Task 2, also upon successful completion of the ADF pipeline, 
# Task 3 should run a Databricks Notebook using ad hoc job compute 
# (i.e., where job doesn't already exist).
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
    # service_client = DataLakeServiceClient(
    # account_url="https://storagejhcklxpqiqeg4.dfs.core.windows.net",
    # credential=os.getenv("AZURE_ADLS2_SAS_TOKEN")
    # )
    file_system_client = service_client.get_file_system_client("test-container")
    #file_system_client = adls2_resource.adls2_client.get_file_system_client("test-container")
    paths = file_system_client.get_paths()
    for path in paths:
        context.log.info(f"Path: {path.name}, Last modified: {path.last_modified}")

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
    simulated_azure_function_return_value: int


@dg.multi_asset(  
        group_name="workflow_b",
        specs=[
            dg.AssetSpec("task_1__invoke_azure_function", skippable=False),
            dg.AssetSpec("task_2__invoke_workflow_d", deps=["task_1__invoke_azure_function"], skippable=True),
            dg.AssetSpec("task_3__databricks_notebook_b1_north_eu", deps=["task_1__invoke_azure_function"], skippable=True),
            ],
        required_resource_keys={"azure_function", "databricks"},
        #resource_defs={"azure_function": AzureFunctionResource},
        # outs={"task_1__invoke_azure_function": dg.AssetOut(is_required=True),
        #       "task_2__invoke_workflow_d": dg.AssetOut(is_required=False),
        # }
)
# , 
def workflow_b_assets(context: dg.AssetExecutionContext, config:AzureFunctionOverrideConfig) -> None:
    task_1_result = task_1__invoke_azure_function(context, azure_function=context.resources.azure_function)
    
    yield dg.MaterializeResult(asset_key="task_1__invoke_azure_function", metadata={"value": task_1_result})

    if config.simulated_azure_function_return_value is not None:
        task_1_result = int(config.simulated_azure_function_return_value)
    task_2_result = None
    task_3_result = None

    if task_1_result == 1:
        task_2_result = task_2__invoke_workflow_d(context)
        yield dg.MaterializeResult(asset_key="task_2__invoke_workflow_d")
    
    if task_1_result == 0:
        task_3_result = task_3__databricks_notebook_b1_north_eu(context)
        yield dg.MaterializeResult(asset_key="task_3__databricks_notebook_b1_north_eu")

    if "success" in [task_2_result, task_3_result]:
        context.log_event(
        dg.AssetMaterialization(
            asset_key="dashboard/hooli_example",
            description="External asset materialized",
        )
    )

# @dg.asset(
#         group_name="workflow_b",
#         )
# def task_1__invoke_azure_function(context: dg.AssetExecutionContext,
#                             azure_function: AzureFunctionResource) -> None:
#     azure_return_value = azure_function.invoke_azure_function(
#             function_app_name="returnoneorzerodemo", function_name="random_choice"
#         )
#     context.log.info(f"Random choice: {azure_return_value.text}")
#     yield dg.Output(azure_return_value.text, metadata={"value": int(azure_return_value.text)})

# @dg.asset(
#         group_name="workflow_b",
#         )
# def task_2__invoke_workflow_d(context: dg.AssetExecutionContext) -> None:
#     launch_and_poll_job(context=context, job_name="workflow_d")

# task_2__run_workflow_b 
# workflow_ 

# used in workflow_b
# If the Azure Function returns 0, run Task 3 which should invoke a Databricks Job 
# in a different Azure Databricks Workspace 
# and region than used in Workflow A. 
# This Databricks job should be in Databricks Workspace B1 and in the North EU region.
# @dg.asset(
#         required_resource_keys={"databricks"}
# )
# def task_3__databricks_notebook_b1_north_eu(context: dg.AssetExecutionContext) -> None:
#     databricks = context.resources.databricks
#     launch_and_poll_databricks_job(context, databricks, 982908164270868)

# used in workflow_b dependent on the successful completion of either Task 2 or Task 3. 
# Task 4 should trigger the refresh of a PowerBI report.
@dg.asset
def task_4__powerbi_report(context: dg.AssetExecutionContext) -> None:
    context.log.info("Invoking PowerBI report")


@dg.asset(
        group_name="workflow_d",
        )
def task_1__invoke_databricks_workflow(context: dg.AssetExecutionContext) -> None:
    # use to demonstrate error handling
    #raise Exception("Error Invoking Databricks workflow for workflow d")
    context.log.info("Invoking Databricks workflow for workflow d")
