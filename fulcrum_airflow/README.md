# Fulcrum Airflow Service

## Basic Information

The Fulcrum Airflow service is designed to manage and execute data pipelines using Apache Airflow. This service includes various DAGs (Directed Acyclic Graphs) for different story groups, which are used to generate and manage stories based on different metrics and grains.

## Setup Steps

### Prerequisites

1. **Python**: Ensure Python 3.10 is installed.
2. **Docker**: Install Docker from [here](https://docs.docker.com/get-docker/).
3. **Docker Compose**: Install Docker Compose from [here](https://docs.docker.com/compose/install/).
4. **Poetry**: Install Poetry for dependency management from [here](https://python-poetry.org/docs/#installation).

### Environment Setup
Ensure the following environment variables are set. You can create a `.env` file in the root directory and add these variables:

```env
SECRET_KEY=some-secret-key
SERVER_HOST=http://localhost:8003/v1/
ANALYSIS_MANAGER_SERVER_HOST=http://host.docker.internal:8001/v1/
STORY_MANAGER_SERVER_HOST=http://host.docker.internal:8003/v1/
QUERY_MANAGER_SERVER_HOST=http://host.docker.internal:8002/v1/
DSENSEI_BASE_URL=http://host.docker.internal:5001
DATABASE_URL=postgresql+asyncpg://postgres:passwordpw@host.docker.internal:5432/fulcrum_db
DOCKER_HOST=unix://var/run/docker.sock
```

Also Keep a .env file in the same directory as docker-compose.yaml, this file should have auth0 creds
```dotenv
AUTH0_CLIENT_ID='------ client_id ------'
AUTH0_CLIENT_SECRET='-------- client_secret -------'
```

### Database Setup

1. **Start PostgreSQL Database**:
    Ensure you have a PostgreSQL database running. You can use Docker to start a PostgreSQL container:
    ```sh
    docker run --name fulcrum_db -e POSTGRES_PASSWORD=passwordpw -e POSTGRES_DB=fulcrum_db -p 5432:5432 -d postgres
    ```
   another database airflow_db is also required, do the same for it as well

### Building Story Builder Docker Image

Use the `Makefile` to build the story-builder Docker image:
```sh
make build-story-builder-image
```

### Running Airflow

1. **Start Airflow Services**:
    Use Docker Compose to start the Airflow services. The `docker-compose.yaml` file is configured to start the necessary Airflow components.
    ```sh
    docker-compose up -d
    ```

2. **Access Airflow Web UI**:
    Once the services are up, you can access the Airflow web UI at `http://localhost:8080`. The default credentials are:
    - Username: `airflow`
    - Password: `airflow`

### Running the Application

1. **Run All Applications**:
    Use the `Makefile` to run all applications:
    ```sh
    make run-all
    ```

2. **Run Specific Application**:
    To run a specific application, use the `run` target with the `app` and `port` parameters:
    ```sh
    make run app=story_manager port=8003
    make run app=analysis_manager port=8001
    make run app=query_manager port=8002
    ```

### Formatting and Linting

1. **Format Code**:
    Use the `Makefile` to format the code:
    ```sh
    make format-all
    ```

2. **Lint Code**:
    Use the `Makefile` to lint the code:
    ```sh
    make lint-all
    ```

By following these steps, you should be able to set up, build, and run the Fulcrum Airflow service successfully.
