from datetime import timedelta
from pathlib import Path

import pandas as pd
import pygit2
from great_expectations.core.batch import RuntimeBatchRequest
from prefect import Flow, Parameter, task
from prefect.tasks.great_expectations import RunGreatExpectationsValidation
from prefect.tasks.secrets.base import PrefectSecret

run_validation = RunGreatExpectationsValidation()


@task(name="Clone GE project", max_retries=3, retry_delay=timedelta(seconds=5))
def clone_ge_project(github_organization, repo_name, github_access_token):
    repo_url = f"https://{github_access_token}:x-oath-basic@github.com/{github_organization}/{repo_name}"
    repo = pygit2.clone_repository(repo_url, repo_name)
    # Return the path to use as the context_root_dir during validation
    return str(Path(repo.path).parent / "great_expectations")


@task(name="Get data from S3", max_retries=3, retry_delay=timedelta(seconds=5))
def fetch_data(aws_credentials, bucket_name, file_key):
    return pd.read_csv(
        f"s3://{bucket_name}/{file_key}",
        storage_options=dict(
            key=aws_credentials.get("ACCESS_KEY"),
            secret=aws_credentials.get("SECRET_ACCESS_KEY"),
            token=aws_credentials.get("SESSION_TOKEN"),
        ),
    )


@task(name="Adjust passenger count")
def adjust_passenger_count(df):
    df["passenger_count"] = df["passenger_count"] + 1
    return df


@task(name="Construct runtime batch request")
def create_runtime_batch_request(df):
    return RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2019-02_df",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "default_identifier_name": "passenger_count_batch",
        },
    )


with Flow("ge_example") as flow:
    # Parameters 🎛
    github_organization = Parameter("github_organization")
    ge_project_repo = Parameter("ge_project_repo")
    data_bucket_name = Parameter("data_bucket_name")
    data_file_name = Parameter("data_file_name")

    # Secrets 🤫
    github_access_token = PrefectSecret("GITHUB_ACCESS_TOKEN")
    aws_credentials = PrefectSecret("AWS_CREDENTIALS")

    # Load data from S3
    passenger_df = fetch_data(aws_credentials, data_bucket_name, data_file_name)

    # Transform the data
    adjusted_passenger_df = adjust_passenger_count(passenger_df)

    # Create RuntimeBatchRequest to allow validation against our DataFrame
    runtime_batch_request = create_runtime_batch_request(adjusted_passenger_df)

    # Fetch Great Expectations project
    ge_project_path = clone_ge_project(
        github_organization, ge_project_repo, github_access_token
    )

    # Validate the data ✅
    run_validation(
        checkpoint_name="my_checkpoint",
        context_root_dir=ge_project_path,
        checkpoint_kwargs=dict(
            validations=[
                dict(
                    batch_request=runtime_batch_request,
                    expectation_suite_name="expectation_suite_taxi.demo",
                )
            ],
        ),
    )
