from setuptools import find_packages, setup

setup(
    name="azure_demo",
    packages=find_packages(exclude=["azure_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "requests",
        "azure-identity",
        "azure-mgmt-datafactory",
        "dagster-azure",
        "dagster-graphql",
        "dagster-databricks",
        "dagster-powerbi==0.0.10"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "azure-functions"]},
)
