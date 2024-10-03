from setuptools import find_packages, setup

setup(
    name="demo_workflow_c",
    packages=find_packages(exclude=["demo_workflow_c_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
