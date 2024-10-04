import dagster as dg
import json
from datetime import datetime, timezone

workflow_a_schedule = dg.ScheduleDefinition(
    job_name="workflow_a",
    cron_schedule="0 14 * * *"
)
@dg.sensor(job_name="workflow_d", default_status=dg.DefaultSensorStatus.RUNNING)
def sub_workflow_sensor(context):
    # Check for the latest materialization of the asset
    asset_key = dg.AssetKey(["invoke_azure_function_2"])
    materialization = context.instance.get_latest_materialization_event(asset_key)
    if materialization:
        # Condition to trigger the run request
        if materialization.asset_materialization.metadata["value"].value == 0:
            materialization_json = materialization.to_json()
            materialization_metadata = json.loads(materialization_json)
            #context.log.info(f"Materialization metadata: {materialization_metadata["run_id"]}")
            context.log.info("Condition met, submitting run request")
            yield dg.RunRequest(
                run_key=materialization_metadata["run_id"],#materialization.run_id,
                tags={"triggered_by": "sub_workflow_sensor"}
            )


@dg.sensor(job_name = "workflow_b",
           required_resource_keys={"adls2_resource"})
def adls2_file_sensor(context: dg.SensorEvaluationContext):
    service_client = context.resources.adls2_resource.adls2_client
    file_system_client = service_client.get_file_system_client("test-container")
    paths = file_system_client.get_paths()

    last_checked = context.cursor or datetime.min.isoformat()
    context.log.info(f"last_checked: {last_checked}")
    last_checked_datetime = datetime.fromisoformat(last_checked)
    last_checked_datetime = last_checked_datetime.replace(tzinfo=timezone.utc)
    #datetime.strptime(last_checked, "%Y-%m-%dT%H:%M:%S")
    
    new_files = []
    for path in paths:
        context.log.info(f"Path: {path.name}, Last modified: {path.last_modified}")
        #path_last_modified = datetime.strptime(path.last_modified, "%Y-%m-%d %H:%M:%S")

        if path.is_directory:
            continue
        if path.last_modified.replace(tzinfo=timezone.utc) > last_checked_datetime:
            context.log.info(f"last_checked: {last_checked}")
            new_files.append(path.name)
    if new_files:
        # Update the cursor to the latest file's last modified time
        latest_file = max(new_files, key=lambda x: file_system_client.get_file_client(x).get_file_properties().last_modified)
        latest_file_properties = file_system_client.get_file_client(latest_file).get_file_properties()
        context.update_cursor(latest_file_properties.last_modified.isoformat())
        context.log.info(f"new files or modified files: {new_files}")
        return dg.RunRequest(tags={"triggered_by": "adls2_file_sensor"})
    return []