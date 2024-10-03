from dagster_graphql import DagsterGraphQLClient
import time
import dagster as dg

def launch_and_poll_databricks_job(context, client, job_id):
    jobs_service = client.workspace_client.jobs

    run = jobs_service.run_now(
        job_id=job_id,
        # original job
        #917365194082799
        # in Azure
        #185516457983272,
    )
    run_id = run.bind()["run_id"]

    get_run_response = jobs_service.get_run(run_id=run_id)

    context.log.info(
        f"Launched databricks job run for '{get_run_response.run_name}' (`{run_id}`). URL:"
        f" {get_run_response.run_page_url}. Waiting to run to complete."
    )

    client.wait_for_run_to_complete(
        logger=context.log,
        databricks_run_id=run_id,
        poll_interval_sec=5,
        max_wait_time_sec=600,
    )

#todo add headers and add org
def launch_and_poll_job(context, job_name):
    client = DagsterGraphQLClient("localhost", port_number=3000)
    # Launch the job
    launch_response = client.submit_job_execution(job_name=job_name)
    context.log.info(f"Launch response: {launch_response}") 
    #run_id = launch_response['data']['launchPipelineExecution']['run']['runId']

    # Poll the job status
    while True:
        status_response = client.get_run_status(run_id=launch_response)
        context.log.info(f"status response : {status_response}")

        if status_response in [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE]:
            break
        time.sleep(5)  # Poll every 5 seconds
    if status_response == dg.DagsterRunStatus.FAILURE:
        raise Exception(f"Job {job_name} failed")
    return status_response