import dagster as dg

@dg.asset
def databricks_notebook(context: dg.AssetExecutionContext) -> None:
    context.log.info("Invoking Databricks notebook")

