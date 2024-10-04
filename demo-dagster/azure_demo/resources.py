import dagster as dg
import requests
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
import time
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken
from dagster_databricks import databricks_client
from databricks.sdk import WorkspaceClient


# Configure the Databricks client resource
databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": "DATABRICKS_HOST_A1"},
        "token": {"env": "DATABRICKS_TOKEN_A1"},
    }
)


pipes_databricks_client = WorkspaceClient(
    host=dg.EnvVar("DATABRICKS_HOST").get_value(),
    token=dg.EnvVar("DATABRICKS_TOKEN").get_value(),
)


adls2_resource = ADLS2Resource(
                    storage_account="storagejhcklxpqiqeg4",
                    credential=ADLS2SASToken(token=dg.EnvVar("AZURE_ADLS2_SAS_TOKEN"))
                )

class AzureFunctionResource(dg.ConfigurableResource):
    function_key: str

    def invoke_azure_function(self, function_app_name, function_name):
        url = f"https://{function_app_name}.azurewebsites.net/api/{function_name}?code={self.function_key}"
        response = requests.get(url)
        return response
    
class AzureDataFactoryResource(dg.ConfigurableResource):
    subscription_id: str
    tenant_id: str
    client_id: str
    client_secret: str
    subscription_id: str


    def create_run(self, resource_group_name, factory_name, pipeline_name):
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        client = DataFactoryManagementClient(credential, self.subscription_id)

        run_response = client.pipelines.create_run(resource_group_name, factory_name, pipeline_name)
        return run_response
    
    def get_run(self, resource_group_name, factory_name, run_id):
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        client = DataFactoryManagementClient(credential, self.subscription_id)
        run_response = client.pipeline_runs.get(resource_group_name, factory_name, run_id)
        return run_response
    
    def cancel_run(self, resource_group_name, factory_name, run_id):
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        client = DataFactoryManagementClient(credential, self.subscription_id)
        client.pipeline_runs.cancel(resource_group_name, factory_name, run_id)
        
    def wait_for_run(self, resource_group_name, factory_name, run_id):
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        client = DataFactoryManagementClient(credential, self.subscription_id)
        run_response = client.pipeline_runs.get(resource_group_name, factory_name, run_id)
        while run_response.status != "Succeeded":
            run_response = client.pipeline_runs.get(resource_group_name, factory_name, run_id)
            print("Waiting for run to complete")
            time.sleep(10)
