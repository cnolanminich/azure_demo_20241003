from setuptools import find_packages, setup

setup(
    name="azure_demo",
    packages=find_packages(exclude=["azure_demo_tests"]),
    install_requires=[
        "dagster==1.8.7",
        "dagster-cloud",
        "requests",
        "azure-identity",
        "azure-mgmt-datafactory",
        "dagster-azure",
        "dagster-graphql",
        "dagster-databricks",
        "dagster-powerbi==0.0.7" #@ git+https://github.com/dagster-io/dagster.git@dagster-powerbi/v0.0.8#egg=dagster-powerbi&subdirectory=python_modules/libraries/dagster-powerbi
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "azure-functions"]},
)
