import importlib

from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs_github import etl_web_to_gcs
github_block = GitHub.load("github")

github_deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="web-to-gc-bucket-gh",
    storage=github_block,
    parameters={"color": "green", "year": 2020, "months": [11]},
    entrypoint="week_2_workflow_orchestration/flows/homework/etl_web_to_gcs_github.py:etl_web_to_gcs",
)

# github_deployment.apply()


if __name__ == "__main__":
    github_deployment.apply()