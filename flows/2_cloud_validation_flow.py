import shutil
from datetime import timedelta
from pathlib import Path

import pygit2
from prefect import Flow, Parameter, task
from prefect.tasks.great_expectations import RunGreatExpectationsValidation
from prefect.tasks.secrets.base import PrefectSecret

run_validation = RunGreatExpectationsValidation()


@task(name="Clone GE project", max_retries=3, retry_delay=timedelta(seconds=5))
def clone_ge_project(github_organization, repo_name, github_access_token):
    # Ensure path doesn't exist before cloning
    shutil.rmtree("prefect-great-expectations-demo", ignore_errors=True)
    repo_url = f"https://{github_access_token}:x-oath-basic@github.com/{github_organization}/{repo_name}"
    repo = pygit2.clone_repository(repo_url, repo_name)
    # Return the path to use as the context_root_dir during validation
    return str(Path(repo.path).parent / "great_expectations")


with Flow("Prefect Cloud Validation") as flow:
    # Parameters 🎛
    github_organization = Parameter("github_organization")
    ge_project_repo = Parameter("ge_project_repo")

    # Secrets 🤫
    github_access_token = PrefectSecret("GITHUB_ACCESS_TOKEN")

    # Fetch Great Expectations project
    ge_project_path = clone_ge_project(
        github_organization, ge_project_repo, github_access_token
    )

    # Validate the data ✅
    run_validation(
        checkpoint_name="my_checkpoint",
        context_root_dir=ge_project_path,
    )
