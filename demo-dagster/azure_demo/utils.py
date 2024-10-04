from dagster_graphql import DagsterGraphQLClient
import time
import dagster as dg
import os
from gql.transport.requests import RequestsHTTPTransport


def launch_and_poll_databricks_job(context, client, job_id):
    jobs_service = client.workspace_client.jobs

    run = jobs_service.run_now(
        job_id=job_id,
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

def launch_and_poll_job(context, job_name):
    token = os.getenv("GRAPHQL_JOB_AGENT_TOKEN")
    url="https://christians-new-org.dagster.cloud/"
    transport = RequestsHTTPTransport(
    url=f"{url}/graphql",
    headers={"Dagster-Cloud-Api-Token": token}
)   
    client = DagsterGraphQLClient(hostname=url, port_number=443, transport=transport)
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