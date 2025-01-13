"""
This is simple script to inject the environment variables and secret in the
Fargate task definition files as well as replace some key variables.
Without this script we would have to maintain the same environment variable and
secrets for every container in the Fargate Task

Example Usage:

python3 deployment/render.py deployment/ecs/env.json \
deployment/ecs/env.json deployment/ecs/web-task-definition.json \
--account 024759284207 --region us-east-2 --app insights_backend --env prod
"""

import argparse
import json
from string import Template

parser = argparse.ArgumentParser(description="Render task definition files")
parser.add_argument(
    "env_file",
    metavar="env.json",
    type=str,
    help="Env file containing envs and secrets",
)
parser.add_argument("files", metavar="F", type=str, nargs="+", help="task definitions files")

parser.add_argument(
    "--region",
    type=str,
    metavar="us-east-2",
    dest="region",
    help="AWS Region",
    required=True,
)
parser.add_argument(
    "--account",
    type=str,
    metavar="1234567",
    dest="account",
    help="AWS Account ID",
    required=True,
)
parser.add_argument(
    "--app",
    type=str,
    metavar="app",
    dest="app",
    help="The application name",
    required=True,
)
parser.add_argument(
    "--env", type=str, metavar="prod", dest="env", help="The environment name", required=False, default="dev"
)


def render(env_file, files, region, account, app, env="dev"):
    # Replace app, region, account and env if provided
    variables = dict(app=app, region=region, account=account, env=env)
    print(f"Replacing Variables in env file: {env_file}")
    with open(env_file, encoding="utf8") as env_f:
        env_template = Template(env_f.read())
    env_json_str = env_template.substitute(**variables)
    data = json.loads(env_json_str)

    # process each file for replacing vars and render env & secrets
    for file in files:
        print(f"Replacing Variables in file: {file}")
        with open(file, encoding="utf8") as f:
            template = Template(f.read())
        file_json_str = template.substitute(**variables)
        print(f"Rendering Environment Variables and Secrets in file: {file}")
        content = json.loads(file_json_str)

        # add env data to container definitions
        for container_definition in content.get("containerDefinitions", []):
            container_definition.update(data)

        # store the updated dict back to json file
        with open(file, "w", encoding="utf8") as f:
            f.write(json.dumps(content, indent=2) + "\n")

        print(f"Updated file: {file}")


if __name__ == "__main__":
    args = parser.parse_args()
    render(
        args.env_file,
        args.files,
        region=args.region,
        account=args.account,
        app=args.app,
        env=args.env,
    )
