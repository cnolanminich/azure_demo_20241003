from dagster import Definitions, load_assets_from_modules, EnvVar

from azure_demo import assets
from azure_demo.resources import AzureFunctionResource, AzureDataFactoryResource, adls2_resource, databricks_client_instance, pipes_databricks_client
from azure_demo.automations import workflow_a_schedule, sub_workflow_sensor, adls2_file_sensor
from azure_demo.jobs import workflow_a, workflow_b, workflow_d
from dagster_databricks import PipesDatabricksClient

import uuid
from http import client
from typing import cast

from dagster_powerbi import PowerBIServicePrincipal, PowerBIToken, PowerBIWorkspace


all_assets = load_assets_from_modules([assets])



# Connect using a service principal
resource = PowerBIWorkspace(
    credentials=PowerBIServicePrincipal(
        client_id="c25cd09d-2e84-4da7-b6c1-95df1217ed32",
        client_secret="wXA8Q~q40DUUNsRiYbsHmBn1sDLgFAOdTCKYncoW", # secret id 91091059-339d-4453-9052-d8886b97faf2
        tenant_id="7c564e53-da36-4073-97c5-ce43e046d8fe",
    ),
    workspace_id="ac965d1a-31d2-41f2-a767-412fef6105d6",
)



initial_definitions = Definitions(
    assets=all_assets,
    jobs=[workflow_a, workflow_b, workflow_d],
    schedules=[workflow_a_schedule],
    sensors=[sub_workflow_sensor, adls2_file_sensor],
    resources={"azure_function": AzureFunctionResource(function_key=EnvVar("AZURE_FUNCTION_KEY")),
               "adf_client": AzureDataFactoryResource(
                    client_id=EnvVar("AZURE_CLIENT_ID"),
                    client_secret=EnvVar("AZURE_CLIENT_SECRET_KEY"),
                    tenant_id=EnvVar("AZURE_TENANT_ID"),
                    subscription_id=EnvVar("AZURE_SUBSCRIPTION_ID")
               ),
                "adls2_resource": adls2_resource,
                "databricks": databricks_client_instance,
                "pipes_databricks_client": PipesDatabricksClient(pipes_databricks_client)
},
)

defs = Definitions.merge(initial_definitions, resource.build_defs())
