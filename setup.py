from setuptools import find_packages, setup

setup(
    name="analytics_platform_dagster",
    packages=find_packages(exclude=["analytics_platform_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "duckdb"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
