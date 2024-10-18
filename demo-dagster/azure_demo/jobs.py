import dagster as dg

workflow_a = dg.define_asset_job(
    name="workflow_a",
    selection=dg.AssetSelection.assets(["task_1__invoke_adf_pipeline"]).downstream()
)

workflow_b = dg.define_asset_job( 
    name="workflow_b",
    selection=dg.AssetSelection.assets(["task_1__invoke_azure_function"]).materializable().downstream()
)

workflow_d = dg.define_asset_job(
    name="workflow_d",
    selection=dg.AssetSelection.assets(["task_1__invoke_databricks_workflow"])
)