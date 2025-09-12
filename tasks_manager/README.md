# Tasks Manager
Fulcrum Prefect is a scalable workflow and job manager application designed to streamline the generation of stories
across various metrics and groups. Leveraging [Prefect](https://www.prefect.io/) for orchestrating complex workflows,
this project ensures efficient task management, deployment, and scalability.

## Features

- **Scalable Workflows:** Utilizes Prefect's orchestration capabilities to manage complex workflows efficiently.
- **Asynchronous Operations:** Implements async functions for non-blocking I/O operations, ensuring optimal performance.
- **Managed Worker Deployments:** Uses Prefect managed workers with git-based code deployment for simplified infrastructure.
- **Git-based Deployment:** Automatic code pulling and environment setup for seamless deployments.
- **Robust Error Handling:** Includes comprehensive error handling and logging mechanisms for reliability.

## Local Development Setup

### Prerequisites
- Python 3.10 or higher
- Poetry for dependency management
- Git

### Quick Start (Recommended)

The project includes a **project-isolated Prefect setup** to avoid conflicts with other Prefect projects:

```bash
cd tasks_manager

# Complete setup in one command
make setup-local
```

This will:
1. Install dependencies
2. Initialize project-specific Prefect database
3. Create work pools and queues
4. Register configuration blocks
5. Deploy flows for local development

### Manual Setup Steps

If you prefer to run each step manually:

1. **Install dependencies:**

```bash
cd tasks_manager
poetry install
```

2. **Setup project-specific Prefect instance:**

```bash
make setup
```

This creates a local SQLite database at `./prefect_data/prefect.db` to avoid conflicts with other projects.

3. **Start the Prefect server:**

```bash
make server
```

This will start the server on port 4200. You can access the UI at http://localhost:4200.

4. **Create work pool and queues (in a new terminal):**

```bash
make create-pool
```

5. **Register configuration blocks:**

```bash
make register-blocks
```

6. **Create a 'default' AppConfig block in the UI:**

- Open http://127.0.0.1:4200
- Navigate to Blocks
- Click "Create block"
- Select "AppConfig"
- Provide a name as 'default'
- Enter the required fields
- Click "Create block"

7. **Deploy flows for local development:**

```bash
make deploy-local
```

8. **Start a worker to execute flows:**

```bash
make worker
```

### Project Isolation Features

This setup provides **complete project isolation**:

- **Separate Database**: Uses `./prefect_data/prefect.db` (SQLite) specific to this project
- **Custom Configuration**: `.prefectconfig` file loads project-specific settings
- **No Global Conflicts**: Won't interfere with other Prefect projects on your machine
- **Easy Cleanup**: `make clean-prefect` removes all local Prefect data

## Production Deployment

### Prerequisites for Production
- Prefect Cloud account
- GitHub repository access token
- Prefect managed work pool

### Setup Production Work Pool

1. **Login to Prefect Cloud:**

```bash
# Login to Prefect Cloud
prefect cloud login
```

2. **Create a managed work pool in Prefect Cloud:**

```bash
prefect work-pool create --type prefect:managed tasks-manager-managed-pool
```

3. **Setup GitHub Integration:**

Follow the [Prefect Cloud GitHub Integration guide](https://docs.prefect.io/v3/how-to-guides/deployments/store-flow-code#prefect-cloud-github-integration) to set up GitHub access:

- Go to your Prefect Cloud workspace
- Navigate to Settings → Integrations → GitHub
- Click "Install GitHub App" and authorize access to the `Levers-Labs/fulcrum-intelligence` repository
- Ensure the integration has access to the repository containing your flow code

4. **Setup Database Network Access (if required):**

For Prefect Managed infrastructure to connect to your database, you may need to whitelist the static outbound IP addresses.

**Static Outbound IP Addresses:**
According to [Prefect documentation](https://docs.prefect.io/v3/how-to-guides/deployment_infra/managed), flows running on Prefect Managed infrastructure use these source addresses:
- `184.73.85.134`
- `52.4.218.198`
- `44.217.117.74`

**Database Service Configuration:**
Add these IP addresses to your database service's allowlist/firewall rules:
- **Supabase**: Add IPs to "Network Restrictions" in your project settings
- **AWS RDS**: Add IPs to security group inbound rules
- **Google Cloud SQL**: Add IPs to authorized networks
- **Azure Database**: Add IPs to firewall rules
- **Other services**: Consult your provider's documentation for IP allowlisting

5. **Create Configuration Blocks:**

Create the following blocks in Prefect Cloud (via UI or CLI):

**Secret Blocks** (for sensitive data):
- `app-config-default`: Your application configuration secrets

**Variable Blocks** (for configuration):
- Additional configuration variables as needed for your flows

### Deployment Process

Production deployments are handled automatically via GitHub Actions when code is pushed to the main branch. The workflow:

1. **Triggers**: Automatically on push to main branch affecting relevant paths
2. **Dependencies**: Installs Poetry and project dependencies
3. **Block Registration**: Registers Prefect configuration blocks
4. **Deployment**: Deploys all flows to Prefect Cloud using managed workers

### Manual Deployment

To deploy manually:

```bash
# Ensure you're connected to Prefect Cloud
prefect cloud login

# Deploy all flows
prefect deploy --all
```

## Running Flows

### Via Prefect UI
1. Open your Prefect Cloud dashboard
2. Navigate to Deployments
3. Find the desired deployment (e.g., "process-story-alerts")
4. Click "Run"
5. Enter any required parameters
6. Click "Run deployment"

### Via CLI
```bash
# Run the alerts flow deployment directly
prefect deployment run 'process-story-alerts' --param metric_id="ct_inquiries" --param tenant_id="1" --param story_date="2025-09-10"
```

## Adding New Flows

To add a new flow for production:

1. **Create flow and task definitions:**
   ```python
   # tasks_manager/flows/your_flow.py
   from prefect import flow, task
   from typing import Dict, Any

   @task
   async def your_task(data: Dict[str, Any]) -> Dict[str, Any]:
       # Task implementation
       return {"processed_data": "data"}

   @flow
   async def your_flow(group: str) -> None:
       # Flow implementation using tasks
       result = await your_task({"group": group})
   ```

2. **Add deployment configuration to `prefect.yaml`:**
   ```yaml
   - name: "your-flow-name"
     version: "1.0.0"
     tags:
       - daily
     description: "Description of your flow"
     entrypoint: tasks_manager.flows.your_flow:your_flow
     parameters:
       group: "YOUR_GROUP"
     work_pool: *managed_work_pool
     schedule: *daily_midnight
   ```

3. **Test locally first:**
   ```bash
   # Update local configuration
   prefect deploy --all --prefect-file prefect.local.yaml

   # Test the flow
   prefect deployment run 'your-flow-name' --param group="test"
   ```

4. **Deploy to production:**
   - Push changes to main branch
   - GitHub Actions will automatically deploy
   - Verify deployment in Prefect Cloud UI

### Best Practices

- **Modular Design**: Keep flow and task definitions in separate files under `flows/` directory
- **Naming Conventions**: Use consistent naming for flows, parameters, and deployments
- **Tagging**: Include appropriate tags for filtering and organization
- **Error Handling**: Add comprehensive logging and error handling
- **Testing**: Always test flows locally before deploying to production
- **Documentation**: Document flow parameters and expected behavior

## Makefile Commands

The project includes a comprehensive Makefile for common tasks:

### Development Commands
```bash
make help           # Show all available commands
make install        # Install dependencies using Poetry
make format         # Format code with black and isort
make lint           # Lint code with ruff and mypy
make test           # Run tests with coverage
make clean          # Clean up generated files and caches
```

### Local Development Setup
```bash
make setup-local    # Complete local development setup
# This runs: install, setup, create-pool, register-blocks, deploy-local

# Or run individual commands:
make setup          # Initialize project-specific Prefect instance
make server         # Start Prefect server with project configuration
make config         # View Prefect configuration (loaded from .prefectconfig)
make create-pool    # Create local process work pool and queues
make register-blocks # Register Prefect blocks
make deploy-local   # Deploy flows for local development
make worker         # Start a Prefect worker
make clean-prefect  # Clean Prefect data and reset local database
```

### Running Flows
```bash
# Run a specific deployment
make run-flow DEPLOYMENT=semantic-data-sync-daily PARAMS='grain="day"'

# Examples:
make run-flow DEPLOYMENT=semantic-data-sync-daily
make run-flow DEPLOYMENT=pattern-analysis-metric PARAMS='tenant_id_str="1" metric_id="test" grain="day"'
```

### Production Deployment
```bash
make setup-prod     # Complete production deployment setup
make deploy         # Deploy all flows to Prefect Cloud
```

## Monitoring and Management

### Local Development
- **Dashboard:** Access the Prefect UI at http://127.0.0.1:4200
- **API Docs:** View API documentation at http://127.0.0.1:4200/docs
- **Logs:** Monitor flow runs and task execution in the UI

### Production
- **Prefect Cloud:** Access your Prefect Cloud dashboard
- **Work Pools:** Monitor managed worker pools and queues
- **Deployments:** View and manage all deployed flows
- **Flow Runs:** Track execution history and performance

## Migration from ECS Workers

This project has been migrated from ECS workers to Prefect managed workers. Key changes include:

### What Changed
- **Infrastructure**: Removed Docker/ECS dependencies
- **Deployment**: Git-based code pulling instead of Docker images
- **Work Pools**: Using managed workers instead of ECS tasks
- **Environment Setup**: Automatic dependency installation via script

### Benefits
- **Simplified Deployment**: No Docker image building required
- **Faster Deployments**: Direct git-based code pulling
- **Reduced Costs**: No ECS infrastructure to maintain
- **Better Developer Experience**: Consistent local/production environments

## Troubleshooting

### Local Development Issues

**Server won't start:**
```bash
# Check if port 4200 is available
lsof -i :4200

# Kill existing Prefect processes
pkill -f "prefect server"

# Clean and restart with project isolation
make clean-prefect
make setup
make server
```

**Worker not processing tasks:**
```bash
# Check work pool status
prefect work-pool ls

# Verify worker is running
prefect worker ls

# Restart worker
make worker
```

**Deployment not found:**
```bash
# List available deployments
prefect deployment ls

# Redeploy if missing
make deploy-local
```

**Import errors or dependency issues:**
```bash
# Reinstall dependencies
make install

# Check Python path
python -c "import tasks_manager; print('✓ Module loaded successfully')"
```

### Production Issues

**GitHub Integration not working:**
```bash
# Verify GitHub App installation in Prefect Cloud settings
# Check repository access permissions
# Ensure correct repository name in prefect.yaml pull steps
```

**Blocks not found:**
```bash
# List all blocks
prefect block ls

# Create missing blocks through Prefect Cloud UI
# Verify block names match those referenced in flows
```

**Worker not connecting (Managed Pools):**
```bash
# Check work pool exists and is managed type
prefect work-pool ls

# For managed pools, workers are handled automatically by Prefect Cloud
# Check work pool status in Prefect Cloud UI
```

**Database connection issues:**
```bash
# Verify database service allows Prefect managed IPs (Supabase, RDS, etc.)
# Check database credentials in Secret blocks
# Test database connectivity from a known working environment
# For Supabase: Check "Network Restrictions" in project settings
```

### Debug Commands

**Check Prefect configuration:**
```bash
# View current Prefect configuration
prefect config view

# Check API connection
prefect cloud workspace ls
```

**Test flow execution locally:**
```bash
# Example quick test of alerts execution
python -c "
import asyncio, json
from tasks_manager.flows.alerts import execute_alert
asyncio.run(execute_alert(tenant_id="1", alert_id="42", trigger_params_str=json.dumps({"grain": "day"})))
"
```

**Monitor flow runs:**
```bash
# List recent flow runs
prefect flow-run ls --limit 10

# View specific flow run logs
prefect flow-run logs <flow-run-id>

# Watch flow runs in real-time
prefect flow-run ls --watch
```

**Debug deployment issues:**
```bash
# Check deployment status
prefect deployment ls

# Inspect specific deployment
prefect deployment inspect <deployment-name>

# Test deployment trigger
prefect deployment run <deployment-name> --param key=value
```

**Check work pool and worker status:**
```bash
# List work pools
prefect work-pool ls

# Inspect work pool details
prefect work-pool inspect <pool-name>

# For local development, check worker status
prefect worker ls
```

**Debug GitHub integration:**
```bash
# Test git clone step manually
git clone https://github.com/Levers-Labs/fulcrum-intelligence.git test-clone
cd test-clone/tasks_manager
poetry install --no-dev --no-interaction
```

### Environment-Specific Issues

**Local Development:**
- **Port conflicts**: Change Prefect server port if 4200 is occupied
- **Path issues**: Ensure you're running commands from the correct directory
- **Permission errors**: Check file permissions for scripts and config files
- **Database conflicts**: Use `make clean-prefect` to reset project-specific database
- **Configuration issues**: Verify `.prefectconfig` file exists and is properly formatted

**Production:**
- **API rate limits**: Monitor GitHub API usage and tokens
- **Network access**: Verify database service network configurations and IP allowlists
- **Resource limits**: Check Prefect Cloud usage and quotas
- **Deployment conflicts**: Ensure no concurrent deployments are running

### Getting Help

**Debug information to collect:**
```bash
# System information
python --version
poetry --version
prefect version

# Prefect configuration
prefect config view

# Recent logs
prefect flow-run ls --limit 5
```

**Log locations:**
- **Local**: Check Prefect UI logs at http://localhost:4200
- **Production**: View logs in Prefect Cloud dashboard
- **GitHub Actions**: Check workflow run logs in GitHub

**Common error patterns:**
- **ModuleNotFoundError**: Run `make install` and verify Python path
- **Connection refused**: Check API URLs and network connectivity
- **Authentication failed**: Verify API keys and login status
- **Block not found**: Create missing blocks in Prefect Cloud UI
