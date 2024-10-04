
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
