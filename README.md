# Fulcrum Intelligence Engine

## Table of Contents

- [Overview](#overview)
- [Setup Guide](#setup-guide)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
    - [Analysis Manager `.env` File](#analysis-manager-env-file)
    - [Query Manager `.env` File](#query-manager-env-file)
    - [Setting Up the Project](#setting-up-the-project)
    - [Setting up pre-commit hooks](#setting-up-pre-commit-hooks)
  - [Running the Project](#running-the-project)
    - [Running with Docker Compose](#running-with-docker-compose)
    - [Running with Make Command](#running-with-make-command)
    - [Accessing the Services](#accessing-the-services)
  - [Cli](#cli)



## Overview

Fulcrum Intelligence Engine is a project that includes several services such as core, query manager & analysis manager.

## Setup Guide

### Prerequisites
- `Python 3.10+`
- `Poetry 1.2+`
- `Postgresql 15+`
- `make` # https://formulae.brew.sh/formula/make

### Environment Variables

This project requires certain environment variables to be set. These variables are loaded from `.env` files located in
the `analysis_manager` and `query_manager` directories.

#### Auth0 Creds (envs)
The auth0 creds are used to authenticate the user and get the access token.
Below are the variables that you need to set in the `.env` file for each service.
```
AUTH0_API_AUDIENCE= // The auth0 api audience
AUTH0_ISSUER= // The auth0 issuer
AUTH0_CLIENT_ID= // The auth0 client id
AUTH0_CLIENT_SECRET= // The auth0 client secret
```

### Multitenancy Support

To Enable the multitenancy Support, Kindly execute the following scripts in the Database for each application schema.

```
DO $$
    BEGIN
    IF NOT EXISTS(SELECT * FROM pg_roles WHERE rolname = 'tenant_user') THEN
            CREATE ROLE tenant_user;
            GRANT USAGE ON SCHEMA {{ your schema }} TO tenant_user;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{ your schema }} TO tenant_user;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {{ your schema }} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO tenant_user;
            GRANT USAGE ON ALL SEQUENCES IN SCHEMA {{ your schema }} TO tenant_user;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {{ your schema }} GRANT USAGE ON SEQUENCES TO tenant_user;
    END IF;
    END
$$
```

#### Analysis Manager `.env` File

Navigate to the `analysis_manager` directory and create a `.env` file with the following variables:

```env
ENV=dev
DEBUG=True
SERVER_HOST=http://localhost:8000
SECRET_KEY=<secret>
BACKEND_CORS_ORIGINS=["http://localhost"]
DATABASE_URL=postgresql+asyncpg://<user>:<password>@localhost/fulcrum
QUERY_MANAGER_SERVER_HOST=http://localhost:8001/v1/
DSENSEI_BASE_URL=http://localhost:5001/
```

#### Query Manager `.env` File

Navigate to the `query_manager` directory and create a `.env` file with the following variables:

```env
ENV=dev
DEBUG=True
SERVER_HOST=http://localhost:8001
SECRET_KEY=<secret>
BACKEND_CORS_ORIGINS=["http://localhost"]
```

### Setting Up the Project

After you've completed the prerequisites, you can set up python environment and install the project dependencies:

```bash
make setup
```

This will install pyenv, set up the python environment, and install the project dependencies.

### Setting up pre-commit hooks

This project uses pre-commit hooks to ensure that the code is formatted correctly and that there are no linting errors.
To set up the pre-commit hooks, run the following command:

```bash
make setup-pre-commit
```

## Running the Project

You can run the project using either Docker Compose or the Make command.

### Running with Docker Compose
**NOTE**: Currently docker-compose is not working as expected, so please use the Make command to run the project.

If you have Docker installed, you can use Docker Compose to run the project. Here's how:

1. Build the Docker images:
    ```bash
    docker-compose build
    ```

2. Run the Docker containers:

    ```bash
    docker-compose up
    ```
This will start all the services defined in your docker-compose.yml file.

### Running with Make Command

If you prefer not to use Docker, you can also run the project using the Make command. Here's how:
You need to make sure that postgres is running and the database is created.
1. Activate the virtual environment:

      ```bash
      source venv/bin/activate
      ```
2. Run the server:

    ```bash
    make run
    ```


### Accessing the Services

You can access the services at the following URLs:

Analysis Manager: http://localhost:8000/docs

Query Manager: http://localhost:8001/docs


### Cli

There is a manage.py file at the root of the project, it contains a basic cli to hopefully
help you manage your project more easily. To get all available commands type this:

```shell
python manage.py --help
```
