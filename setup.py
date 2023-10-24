from setuptools import find_packages, setup

setup(
    name="duck_lake",
    packages=find_packages(exclude=["duck_lake_tests"]),
    install_requires=[
        "dagster",
        "duckdb",
        "dagster-duckdb-pandas",
        "dagster-duckdb",
        "dagster-gcp",
        "pandas",
        "pyarrow",
        "dbt-core",
        "dagster-dbt",
        "dbt-duckdb", 
        "fsspec",
        "gcsfs",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
