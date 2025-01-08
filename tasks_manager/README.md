# Tasks Manager
Fulcrum Prefect is a scalable workflow and job manager application designed to streamline the generation of stories
across various metrics and groups. Leveraging [Prefect](https://www.prefect.io/) for orchestrating complex workflows,
this project ensures efficient task management, deployment, and scalability.

## Features

- **Scalable Workflows:** Utilizes Prefect's orchestration capabilities to manage complex workflows efficiently.
- **Asynchronous Operations:** Implements async functions for non-blocking I/O operations, ensuring optimal performance.
- **Configurable Deployments:** Supports Docker and AWS ECS deployments for flexible infrastructure management.
- **Robust Error Handling:** Includes comprehensive error handling and logging mechanisms for reliability.

## Local Development Setup

1. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install the dependencies:

```bash
cd tasks_manager
poetry install
```

3. Start the Prefect server:

```bash
prefect server start
```

This will start the server on port 4200. You can access the UI at http://localhost:4200.

4. Configure Prefect API URL (in a new terminal):

```bash
prefect config set PREFECT_API_URL=http://localhost:4200/api
```

5. Deploy the app config block to configure secrets used by the flows.

```bash
cd tasks_manager
prefect blocks register --file tasks_manager/config.py
```

6. Create a 'default' AppConfig block in the UI.

- Open http://127.0.0.1:4200
- Navigate to Blocks
- Click "Create block"
- Select "AppConfig"
- Provide a name as 'default'
- Enter the required fields
- Click "Create block"

## Creating and Deploying Flows

1. Deploy the flow using the CLI:
   ```bash
   prefect deployment build tasks_manager/stories.py:generate_stories -n story-generator
   prefect deployment apply story-generator-deployment.yaml
   ```
2. Start a worker to execute flows:
   ```bash
   prefect worker start -p default-agent-pool
   ```

3. Run the flow in one of these ways:

   Via Python:
   ```python
   from tasks_manager.gen_stories import generate_stories

   # Run the flow with a specific group
   await generate_stories(group="daily")
   ```

   Via CLI:
   ```bash
   # Run with parameters
   prefect deployment run 'Generate Stories/story-generator' -p group="daily"
   ```

   Via UI:
    1. Open http://127.0.0.1:4200
    2. Navigate to Deployments
    3. Find "Generate Stories/story-generator"
    4. Click "Run"
    5. Enter parameters:
        - group: "daily" (or other valid group value)
    6. Click "Run deployment"

## Adding new flows for Production

To add a new flow for production:

1. Create flow and task definitions:
   ```python
   # tasks_manager/flows/your_flow.py
   from prefect import flow, task
   from typing import Dict, Any

   @task
   async def your_task(data: Dict[str, Any]) -> Dict[str, Any]:
       # Task implementation
       return processed_data

   @flow
   async def your_flow(group: str) -> None:
       # Flow implementation using tasks
       result = await your_task({"group": group})
   ```

2. Add deployment configuration to `prefect.yaml`:
   ```yaml
   deployments:
     - name: "Your Flow Name"
       version: "1.0.0"
       tags:
         - daily
       description: "Generate stories for Your Flow"
       entrypoint: tasks_manager.flows.your_flow:your_flow
       parameters:
         group: "YOUR_GROUP"
       work_pool: *ecs_work_pool
       schedules: *daily_midnight
   ```

3. Deploy the new flow (managed via Github Actions)
   ```bash
   prefect deploy --all
   ```

4. Verify the deployment in the Prefect UI at http://127.0.0.1:4200

Best Practices:

- Keep flow and task definitions in separate files under `flows/` directory
- Follow consistent naming conventions for groups and flow names
- Include appropriate tags for filtering and organization
- Add comprehensive logging and error handling
- Test flows locally before deploying to production

## Monitoring and Management

- **Dashboard:** Access the Prefect UI at http://127.0.0.1:4200
- **API Docs:** View API documentation at http://127.0.0.1:4200/docs
- **Logs:** Monitor flow runs and task execution in the UI

## Troubleshooting

- If the server doesn't start, check if port 4200 is available
- Ensure your Python environment has all required dependencies
- Check the Prefect logs for detailed error messages
