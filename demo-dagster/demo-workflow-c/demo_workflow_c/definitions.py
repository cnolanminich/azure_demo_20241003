import dagster as dg
from demo_workflow_c import assets  # type: ignore
from datetime import datetime, timedelta

all_assets = dg.load_assets_from_modules([assets])

workflow_c = dg.define_asset_job(name = "workflow_c", selection = "databricks_notebook")


# Define the run status sensor
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[dg.JobSelector(
            location_name="azure_demo",
            #repository_name="code_location_a",
            job_name="workflow_a",
        ),
        dg.JobSelector(
            location_name="azure_demo",
            #repository_name="code_location_a",
            job_name="workflow_b",
        )],
    request_job=workflow_c,
)
def wait_for_both_upstreams_sensor(context):
    # Get the instance of Dagster
    instance = context.instance

    # Calculate the timestamp for 24 hours ago
    one_day_ago = datetime.now() - timedelta(days=1)

    # Check if both upstream jobs have completed successfully in the past day
    upstream_1_runs = instance.get_runs(filters=dg.RunsFilter(
        job_name="workflow_a",
        statuses=[dg.DagsterRunStatus.SUCCESS],
        created_after=one_day_ago
    ))

    upstream_2_runs = instance.get_runs(filters=dg.RunsFilter(
        job_name="workflow_b",
        statuses=[dg.DagsterRunStatus.SUCCESS],
        created_after=one_day_ago
    ))
    
    if not (upstream_1_runs and upstream_2_runs):
        # One or both upstream jobs have not completed successfully in the past day, skip triggering the downstream job
        return dg.SkipReason("One or both upstream jobs have not completed successfully in the past day")
    # Both upstream jobs have completed successfully in the past day, trigger the downstream job
    return dg.RunRequest(run_key=f"{upstream_1_runs[0].run_id}-{upstream_2_runs[0].run_id}", job_name="workflow_c")

# Add the sensor to the Definitions object
defs = dg.Definitions(
    assets = [*all_assets],
    jobs=[workflow_c],
    sensors=[wait_for_both_upstreams_sensor]
)
