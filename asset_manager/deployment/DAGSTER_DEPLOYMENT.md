# Dagster Self-Hosted Deployment Guide

This guide covers deploying Dagster in a self-hosted environment on AWS ECS, following Dagster OSS best practices for production deployments.

## Architecture Overview

### Components

- **Dagster Webserver**: UI and GraphQL API (Port 3000)
- **Dagster Daemon**: Schedules, sensors, and run coordination
- **Run Launcher**: ECS tasks for individual pipeline runs
- **Storage**: PostgreSQL for run/event storage, S3 for compute logs and artifacts

### Data Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Dagster Web    │    │ Dagster Daemon  │    │   ECS Run       │
│  (UI/GraphQL)   │    │ (Schedules)     │    │   Tasks         │
│  Port 3000      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                ┌────────────────────────────────┐
                │     PostgreSQL Storage         │
                │   (Run/Event Store)           │
                │     + S3 Compute Logs         │
                └────────────────────────────────┘
```

## Application Architecture

### Asset Pipeline Overview

The Asset Manager implements a two-stage pipeline for Snowflake cache synchronization:

```
┌─────────────────────────┐    ┌──────────────────────────┐
│  metric_semantic_values │───▶│  snowflake_metric_cache  │
│                         │    │                          │
│ • Extract from Cube API │    │ • Load to Snowflake      │
│ • Per tenant/metric/    │    │ • Cache tables per       │
│   grain partition       │    │   tenant/metric/grain    │
└─────────────────────────┘    └──────────────────────────┘
```

### Dynamic Partitioning

The pipeline uses a single dynamic partition dimension that combines:
- **Tenant identifier**: `tenant123`
- **Granularity**: `day`, `week`, `month`
- **Metric ID**: `metric_revenue`

Combined as: `tenant123::day::metric_revenue`

### Scheduling Strategy

- **Daily Schedule** (3 AM UTC): Processes all `day` grain partitions
- **Weekly Schedule** (Monday 6 AM UTC): Processes all `week` grain partitions
- **Monthly Schedule** (1st of month 8 AM UTC): Processes all `month` grain partitions
- **Partition Sync Sensor**: Updates partition registry every 5 minutes

### Multi-Tenant Support

Each tenant has independent:
- **Metric configurations**: Enabled metrics for caching
- **Grain configurations**: Enabled granularities with sync periods
- **Snowflake schemas**: Isolated cache tables

## Dagster Configuration

### Instance Configuration (dagster-prod.yaml)

The `dagster-prod.yaml` file configures how Dagster operates in production:

#### Storage Backend

```yaml
storage:
  postgres:
    postgres_db:
      username: { env: DAGSTER_PG_USER }
      password: { env: DAGSTER_PG_PASSWORD }
      hostname: { env: DAGSTER_PG_HOST }
      db_name: { env: DAGSTER_PG_DB }
      port: { env: DAGSTER_PG_PORT }
```

**Why PostgreSQL**: Provides ACID transactions, concurrent access, and scalability for run metadata, event logs, and asset materializations.

#### Compute Logs

```yaml
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket: { env: DAGSTER_S3_BUCKET }
    prefix: dagster/compute-logs
    upload_interval: 30
    skip_empty_files: true
    local_dir: "/tmp/dagster-logs"
    show_url_only: true
```

**Why S3**: Durable storage for stdout/stderr from pipeline runs, accessible from Dagster UI, automatic retention policies. Configuration includes optimized upload intervals and local staging for performance.

#### IO Management

IO managers are configured in Python code within the `Definitions` object, not in the instance configuration file. The S3PickleIOManager is conditionally configured for production environments when `DAGSTER_S3_BUCKET` is provided:

```python
# Add S3 IO Manager only if S3 bucket is configured (prod environment)
if app_config.settings.dagster_s3_bucket:
    resources["io_manager"] = S3PickleIOManager(
        s3_bucket=app_config.settings.dagster_s3_bucket,
        s3_prefix="dagster/io",
    )
```

**Environment Behavior**:

- **Development**: Uses default filesystem IO manager when `DAGSTER_S3_BUCKET` is not set
- **Production**: Uses S3PickleIOManager when `DAGSTER_S3_BUCKET` environment variable is provided

**Why S3PickleIOManager**: Enables durable asset output storage across different compute environments, provides isolation between asset executions, and supports scalable data persistence for production workflows.

#### Run Coordination

```yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    dequeue_use_threads: true
    dequeue_num_workers: 4
    dequeue_interval_seconds: 2
    max_concurrent_runs: 35
```

**Purpose**: Manages run queue, prevents resource exhaustion, enables backpressure control. Multi-threaded dequeue processing improves throughput with 4 worker threads and 2-second polling intervals.

#### Run Launcher

```yaml
run_launcher:
  module: 'dagster_aws.ecs'
  class: 'EcsRunLauncher'
  config:
    task_definition: 'asset-manager-run'
    container_name: 'asset-manager-run'
```

**Why ECS Run Launcher**:

- Isolation: Each run gets its own container
- Scalability: Automatic task provisioning
- Cost efficiency: Pay only for run duration
- Resource control: CPU/memory limits per run

#### Worker Process Configuration

```yaml
sensors:
  use_threads: true
  num_workers: 2
  num_submit_workers: 2

schedules:
  use_threads: true
  num_workers: 6
  num_submit_workers: 4

backfills:
  use_threads: true
  num_workers: 2
  num_submit_workers: 2
```

**Purpose**: Configures multi-threaded workers for sensors, schedules, and backfills to improve daemon performance and concurrent processing capabilities.

#### Additional Configuration

```yaml
code_servers:
  local_startup_timeout: 360

run_monitoring:
  enabled: true
  poll_interval_seconds: 45

telemetry:
  enabled: false
```

**Features**:
- Extended code server startup timeout for complex asset loading
- Run monitoring for automatic run status updates
- Telemetry disabled for privacy

## Authentication

### ALB + Auth0 Integration

Dagster Web UI is secured using AWS Application Load Balancer (ALB) OpenID Connect (OIDC) authentication with Auth0 as the identity provider. This provides enterprise-grade authentication without requiring modifications to the Dagster application itself.

#### Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Browser       │    │      ALB        │    │  Dagster Web    │
│                 │    │  (Auth Layer)   │    │   (Port 3000)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │  1. Request           │                       │
         ├──────────────────────►│                       │
         │                       │                       │
         │  2. Redirect to Auth0 │                       │
         ◄──────────────────────┤                       │
         │                       │                       │
         │  3. Auth0 Login       │                       │
         ├───────────────────────┼───────────────────────│
         │                       │                       │
         │  4. Callback          │                       │
         ├──────────────────────►│                       │
         │                       │  5. Forward Request   │
         │                       ├──────────────────────►│
         │                       │                       │
         │  6. Response          │  7. Response          │
         ◄──────────────────────┼◄──────────────────────┤
```

#### Authentication Flow

1. **User Request**: User navigates to `https://dg.leverslabs.com`
2. **ALB Authentication**: ALB checks for valid session cookie
3. **Auth0 Redirect**: If no valid session, ALB redirects to Auth0
4. **User Authentication**: User logs in via Auth0
5. **Callback Processing**: Auth0 redirects back to ALB callback URL
6. **Session Creation**: ALB creates session cookie and forwards to Dagster
7. **Dagster Access**: Subsequent requests include session cookie for seamless access

#### Session Management

- **Session Cookie**: `AWSELBAuthSessionCookie`
- **Session Timeout**: 7 days (configurable)
- **Session Reset**: Visit `https://leverslabs.us.auth0.com/v2/logout` to clear session
- **Scope**: `openid email profile` - provides user identity information

#### Security Benefits

- **No Application Changes**: Authentication handled at infrastructure level
- **Enterprise Integration**: Supports SAML, LDAP, Active Directory via Auth0
- **Multi-Factor Authentication**: Optional MFA enforcement
- **Audit Logging**: Complete authentication audit trail
- **Session Management**: Centralized session control and timeout policies

## Deployment Patterns

### Service Architecture

#### Long-Running Services

1. **Webserver Service**

   - Purpose: Serves UI and GraphQL API
   - Scaling: 1-2 instances for HA
   - Load Balancer: ALB with health checks on `/server_info`
   - Resource Requirements: 0.5 vCPU, 1GB RAM

2. **Daemon Service**
   - Purpose: Schedules, sensors, run queue management
   - Scaling: Exactly 1 instance (singleton)
   - No Load Balancer: Internal service only
   - Resource Requirements: 0.5 vCPU, 1GB RAM

#### Ephemeral Run Tasks

- **Purpose**: Execute individual pipeline runs
- **Lifecycle**: Created per run, destroyed on completion
- **Isolation**: Each run in separate container
- **Resource Allocation**: Configurable per job via tags

### Environment Configuration

#### Required Environment Variables

```bash
# Core Application Settings
ENV=prod
DEBUG=true
PYTHONUNBUFFERED=1
AWS_REGION=us-west-1
PGSSLMODE=require

# Dagster Configuration
DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300
DAGSTER_GRPC_TIMEOUT_SECONDS=300
DAGSTER_GRPC_MAX_WORKERS=4

# Database Connection (via SSM)
DAGSTER_PG_HOST=your-db-host
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=postgres
DAGSTER_PG_USER=dagster_user
DAGSTER_PG_PASSWORD=secure_password

# AWS Resources
DAGSTER_S3_BUCKET=your-dagster-bucket

# Application Services (via SSM)
SERVER_HOST=https://dg.leverslabs.com
STORY_MANAGER_SERVER_HOST=https://story-manager.domain.com
ANALYSIS_MANAGER_SERVER_HOST=https://analysis-manager.domain.com
QUERY_MANAGER_SERVER_HOST=https://query-manager.domain.com
INSIGHTS_BACKEND_SERVER_HOST=https://insights-backend.domain.com

# Auth0 Configuration (via SSM)
AUTH0_API_AUDIENCE=your-audience
AUTH0_ISSUER=https://your-domain.auth0.com/
AUTH0_CLIENT_ID=your-client-id
AUTH0_CLIENT_SECRET=your-client-secret

# Fulcrum Platform (via SSM)
DATABASE_URL=postgresql://user:pass@host:port/db
SECRET_KEY=your-secret-key
```

#### Secrets Management

- **SSM Parameter Store**: Centralized secret storage
- **ECS Integration**: Automatic secret injection via `valueFrom`
- **IAM Permissions**: Task roles with least-privilege access
- **Encryption**: SecureString parameters with KMS encryption

## Operational Considerations

### Monitoring and Observability

#### Health Checks

1. **Webserver**: `GET /server_info` - Returns Dagster instance status
2. **Daemon**: Process health via ECS service checks
3. **Database**: Connection pool monitoring
4. **S3**: Bucket accessibility and permissions

#### Logging Strategy

```
CloudWatch Log Groups:
├── asset-manager/web      (Webserver logs)
├── asset-manager/daemon   (Daemon logs)
└── asset-manager/runs     (Individual run logs)

S3 Compute Logs:
└── s3://bucket/dagster/compute-logs/  (Structured run output)
```

#### Metrics and Alerts

- **Run Success Rate**: Alert on failure rate > 10%
- **Queue Depth**: Alert on pending runs > threshold
- **Resource Utilization**: CPU/Memory usage monitoring
- **Database Connections**: Connection pool exhaustion alerts

### Scaling Strategies

#### Horizontal Scaling

- **Webserver**: Scale up for UI load (typically 1-2 instances sufficient)
- **Daemon**: Keep at 1 (singleton architecture)
- **Run Tasks**: Auto-scaling via ECS based on queue depth

#### Vertical Scaling

- **Run Resources**: Configure per job via tags

```python
@job(tags={"ecs/cpu": "1024", "ecs/memory": "2048"})
def heavy_computation_job():
    pass
```

#### Resource Optimization

- **Spot Instances**: Use for non-critical runs
- **Resource Requests**: Right-size based on profiling
- **Concurrency Limits**: Prevent resource exhaustion

### Security Best Practices

#### Network Security

- **Private Subnets**: ECS tasks in private subnets
- **Security Groups**: Restrictive ingress/egress rules
- **ALB**: HTTPS termination with ACM certificates
- **Database**: SSL/TLS encryption in transit

#### Access Control

- **IAM Roles**: Service-specific task roles
- **SSM Permissions**: Least-privilege parameter access
- **S3 Policies**: Bucket-specific permissions
- **Database Users**: Dedicated Dagster user with minimal privileges
- **Auth0 Authentication**: OIDC-based user authentication at ALB level
- **User Management**: Centralized user provisioning and deprovisioning
- **Role-Based Access**: Auth0 rules for granular permission control

#### Data Protection

- **Encryption at Rest**: S3 server-side encryption
- **Encryption in Transit**: HTTPS/SSL for all connections
- **Secret Rotation**: Regular rotation of database passwords
- **Audit Logging**: CloudTrail for AWS API calls

### Backup and Disaster Recovery

#### Database Backups

- **Automated Backups**: Daily PostgreSQL backups
- **Point-in-Time Recovery**: WAL archiving for PITR
- **Cross-Region Replication**: For disaster recovery

#### S3 Data Protection

- **Versioning**: Enable S3 versioning for compute logs
- **Cross-Region Replication**: For critical artifacts
- **Lifecycle Policies**: Automatic cleanup of old logs

#### Recovery Procedures

1. **Database Recovery**: Restore from backup + replay WAL
2. **Service Recovery**: Redeploy ECS services from task definitions
3. **Data Recovery**: Restore S3 objects from backups or versioning

### Performance Tuning

#### Database Optimization

- **Connection Pooling**: Configure appropriate pool sizes
- **Query Optimization**: Index on frequently queried columns
- **Maintenance**: Regular VACUUM and ANALYZE operations

#### S3 Performance

- **Request Patterns**: Use appropriate prefixes for high-throughput
- **Transfer Acceleration**: Enable for large artifacts
- **Multipart Uploads**: For large files

#### ECS Optimization

- **Task Placement**: Spread across AZs for HA
- **Resource Allocation**: Monitor and adjust based on usage
- **Image Optimization**: Use multi-stage builds to reduce size

## Troubleshooting Guide

### Common Issues

#### Run Tasks Not Starting

**Symptoms**: Runs stuck in STARTING state
**Causes**:

- ECS capacity issues
- Task definition errors
- Network connectivity problems
  **Solutions**:
- Check ECS cluster capacity
- Validate task definition JSON
- Verify security group rules

#### Database Connection Failures

**Symptoms**: "Connection refused" errors
**Causes**:

- Incorrect connection parameters
- Network connectivity issues
- SSL configuration problems
  **Solutions**:
- Verify DAGSTER*PG*\* environment variables
- Check security group rules for port 5432
- Ensure PGSSLMODE=require for Supabase

#### S3 Access Issues

**Symptoms**: "Access Denied" for compute logs
**Causes**:

- IAM permission issues
- Bucket policy restrictions
- KMS key access problems
  **Solutions**:
- Verify task role has S3 permissions
- Check bucket policy allows ECS task role
- Ensure KMS key policy includes task role

### Debug Commands

```bash
# Check ECS service status
aws ecs describe-services --cluster fulcrum-cluster --services asset-manager-web

# View recent logs
aws logs tail dagster/asset_manager/web --follow

# List running tasks
aws ecs list-tasks --cluster fulcrum-cluster --service-name asset-manager-web

# Describe task details
aws ecs describe-tasks --cluster fulcrum-cluster --tasks task-id

# Check task definition
aws ecs describe-task-definition --task-definition asset-manager-run

# Authentication debugging
# Test DNS resolution
nslookup dg.leverslabs.com

# Test HTTPS certificate
curl -I https://dg.leverslabs.com
# Should return 302 with Auth0 redirect location

# Check ALB listener configuration
aws elbv2 describe-listeners --load-balancer-arn $ALB_ARN

# Check ALB target health
aws elbv2 describe-target-health --target-group-arn $TARGET_GROUP_ARN

# Reset authentication session for testing
# Visit: https://leverslabs.us.auth0.com/v2/logout

# Verify Auth0 configuration endpoints
curl https://leverslabs.us.auth0.com/.well-known/openid_configuration
```

### Log Analysis

#### Webserver Logs

```
Look for:
- "Serving on http://0.0.0.0:3000" (successful startup)
- Database connection errors
- Asset loading issues
```

#### Daemon Logs

```
Look for:
- "dagster-daemon starting" (successful startup)
- Schedule/sensor execution logs
- Run queue management messages
```

#### Run Task Logs

```
Look for:
- Asset materialization progress
- Error stack traces
- Resource utilization warnings
```

This deployment guide ensures a robust, scalable, and maintainable Dagster installation following industry best practices for self-hosted deployments.
