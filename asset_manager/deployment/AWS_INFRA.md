# Asset Manager (Dagster) – AWS ECS Infrastructure Guide

This guide provides manual steps to deploy the Asset Manager (Dagster) on AWS ECS Fargate using existing infrastructure.

## Prerequisites

### AWS Resources (Assumed Existing)
- **VPC**: With private subnets (2+ AZs) for ECS tasks, public subnets for ALB, NAT gateways
- **ECS Cluster**: `asset-manager` (Fargate enabled)
- **ECR Repository**: `asset-manager`
- **IAM Roles**:
  - `fulcrum-ecs-execution-role`: ECR pull, CloudWatch logs write
  - `fulcrum-ecs-task-role`: SSM parameter read, S3 bucket access
- **Route53**: Domain with ACM certificate

### New Resources Needed

#### Security Groups
Create dedicated security groups for the Asset Manager:

```bash
# Get VPC ID
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=fulcrum-vpc" --query 'Vpcs[0].VpcId' --output text)

# Create ALB Security Group
ALB_SG_ID=$(aws ec2 create-security-group \
  --group-name asset-manager-alb-sg \
  --description "Security group for Asset Manager ALB" \
  --vpc-id $VPC_ID \
  --query 'GroupId' --output text)

# Allow HTTPS traffic from internet to ALB
aws ec2 authorize-security-group-ingress \
  --group-id $ALB_SG_ID \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0

# Allow HTTP traffic from internet to ALB (for redirect)
aws ec2 authorize-security-group-ingress \
  --group-id $ALB_SG_ID \
  --protocol tcp \
  --port 80 \
  --cidr 0.0.0.0/0

# Create ECS Services Security Group
ECS_SG_ID=$(aws ec2 create-security-group \
  --group-name asset-manager-services-sg \
  --description "Security group for all Asset Manager ECS services" \
  --vpc-id $VPC_ID \
  --query 'GroupId' --output text)

# Allow traffic from ALB to ECS services on port 3000
aws ec2 authorize-security-group-ingress \
  --group-id $ECS_SG_ID \
  --protocol tcp \
  --port 3000 \
  --source-group $ALB_SG_ID

# Allow ECS services to communicate with each other
aws ec2 authorize-security-group-ingress \
  --group-id $ECS_SG_ID \
  --protocol tcp \
  --port 0-65535 \
  --source-group $ECS_SG_ID

# Allow outbound traffic to internet (for dependencies, APIs, etc.)
aws ec2 authorize-security-group-egress \
  --group-id $ECS_SG_ID \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0

aws ec2 authorize-security-group-egress \
  --group-id $ECS_SG_ID \
  --protocol tcp \
  --port 80 \
  --cidr 0.0.0.0/0

# Allow outbound traffic to Supabase (PostgreSQL)
aws ec2 authorize-security-group-egress \
  --group-id $ECS_SG_ID \
  --protocol tcp \
  --port 5432 \
  --cidr 0.0.0.0/0

echo "ALB Security Group ID: $ALB_SG_ID"
echo "ECS Services Security Group ID: $ECS_SG_ID"
```

#### Application Load Balancer
Create dedicated ALB for Asset Manager:

```bash
# Get subnet IDs for public subnets (space-separated for ALB command)
PUBLIC_SUBNET_IDS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Type,Values=public" \
  --query 'Subnets[*].SubnetId' --output text)

# Create Application Load Balancer
ALB_ARN=$(aws elbv2 create-load-balancer \
  --name asset-manager-alb \
  --subnets $PUBLIC_SUBNET_IDS \
  --security-groups $ALB_SG_ID \
  --scheme internet-facing \
  --type application \
  --ip-address-type ipv4 \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text)

# Get ACM certificate ARN (assuming you have one for your domain)
CERT_ARN=$(aws acm list-certificates \
  --query 'CertificateSummaryList[?DomainName==`*.leverslabs.com`].CertificateArn' \
  --output text)

# Create HTTPS listener
HTTPS_LISTENER_ARN=$(aws elbv2 create-listener \
  --load-balancer-arn $ALB_ARN \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=$CERT_ARN \
  --default-actions Type=fixed-response,FixedResponseConfig='{MessageBody="Not Found",StatusCode="404",ContentType="text/plain"}' \
  --query 'Listeners[0].ListenerArn' --output text)

# Create HTTP listener (redirect to HTTPS)
aws elbv2 create-listener \
  --load-balancer-arn $ALB_ARN \
  --protocol HTTP \
  --port 80 \
  --default-actions Type=redirect,RedirectConfig='{Protocol="HTTPS",Port="443",StatusCode="HTTP_301"}'

echo "Load Balancer ARN: $ALB_ARN"
echo "HTTPS Listener ARN: $HTTPS_LISTENER_ARN"
```

#### S3 Bucket
Create bucket for Dagster compute logs and IO artifacts:
```bash
aws s3 mb s3://asset-manager-artifacts
aws s3api put-bucket-encryption \
  --bucket asset-manager-artifacts \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }]
  }'
```

#### CloudWatch Log Groups
```bash
aws logs create-log-group --log-group-name "fulcrum/asset-manager/web"
aws logs create-log-group --log-group-name "fulcrum/asset-manager/daemon"
aws logs create-log-group --log-group-name "fulcrum/asset-manager/runs"
```

#### SSM Parameters
Replace `$account` and `$region` with your values:

```bash
# Service endpoints
aws ssm put-parameter --name "/asset-manager/SERVER_HOST" --value "https://dg.leverslabs.com" --type String
# Auth0 (service-specific)
aws ssm put-parameter --name "/asset-manager/AUTH0_CLIENT_ID" --value "your-auth0-client-id" --type String
aws ssm put-parameter --name "/asset-manager/AUTH0_CLIENT_SECRET" --value "your-auth0-client-secret" --type SecureString

# Dagster Postgres (Supabase)
aws ssm put-parameter --name "/asset-manager/DAGSTER_PG_HOST" --value "db.crzveraijklryabyrras.supabase.co" --type String
aws ssm put-parameter --name "/asset-manager/DAGSTER_PG_PORT" --value "5432" --type String
aws ssm put-parameter --name "/asset-manager/DAGSTER_PG_DB" --value "postgres" --type String
aws ssm put-parameter --name "/asset-manager/DAGSTER_PG_USER" --value "postgres" --type String
aws ssm put-parameter --name "/asset-manager/DAGSTER_PG_PASSWORD" --value "your-supabase-password" --type SecureString

# S3 bucket for Dagster
aws ssm put-parameter --name "/asset-manager/DAGSTER_S3_BUCKET" --value "asset-manager-artifacts" --type String
```

## Deployment Steps

### 1. Build and Push Docker Image

From the repository root:
```bash
# Login to ECR
aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin $account.dkr.ecr.us-west-1.amazonaws.com

# Build image
docker build -t $account.dkr.ecr.us-west-1.amazonaws.com/asset-manager:latest -f asset_manager/Dockerfile .

# Push image
docker push $account.dkr.ecr.us-west-1.amazonaws.com/asset-manager:latest
```

### 2. Prepare Task Definitions

Merge environment and secrets into task definitions:
```bash
cd asset_manager/deployment/ecs

# Replace placeholders in all JSON files
sed -i "s/\$account/$account/g" *.json
sed -i "s/\$region/us-west-1/g" *.json
sed -i "s/\$app/asset_manager/g" *.json
sed -i "s/\$env/prod/g" env.json

# Set ECR image in task definitions
IMAGE_URI="$account.dkr.ecr.us-west-1.amazonaws.com/asset-manager:latest"
jq --arg image "$IMAGE_URI" '.containerDefinitions[0].image = $image' web-task-definition.json > web-task-definition-final.json
jq --arg image "$IMAGE_URI" '.containerDefinitions[0].image = $image' daemon-task-definition.json > daemon-task-definition-final.json
jq --arg image "$IMAGE_URI" '.containerDefinitions[0].image = $image' run-task-definition.json > run-task-definition-final.json

# Merge env.json into task definitions
python3 << 'EOF'
import json

# Load env config
with open('env.json') as f:
    env_config = json.load(f)

# Update each task definition
for task_file in ['web-task-definition-final.json', 'daemon-task-definition-final.json', 'run-task-definition-final.json']:
    with open(task_file) as f:
        task_def = json.load(f)

    # Merge environment and secrets
    task_def['containerDefinitions'][0]['environment'] = env_config['environment']
    task_def['containerDefinitions'][0]['secrets'] = env_config['secrets']

    with open(task_file, 'w') as f:
        json.dump(task_def, f, indent=2)
EOF
```

### 3. Register Task Definitions

```bash
# Register all task definitions
aws ecs register-task-definition --cli-input-json file://web-task-definition-final.json
aws ecs register-task-definition --cli-input-json file://daemon-task-definition-final.json
aws ecs register-task-definition --cli-input-json file://run-task-definition-final.json

# Note the task definition ARNs for service creation
WEB_TASK_ARN=$(aws ecs describe-task-definition --task-definition asset_manager-web-task --query 'taskDefinition.taskDefinitionArn' --output text)
DAEMON_TASK_ARN=$(aws ecs describe-task-definition --task-definition asset_manager-daemon-task --query 'taskDefinition.taskDefinitionArn' --output text)
RUN_TASK_ARN=$(aws ecs describe-task-definition --task-definition asset-manager-run --query 'taskDefinition.taskDefinitionArn' --output text)

echo "Web Task Definition: $WEB_TASK_ARN"
echo "Daemon Task Definition: $DAEMON_TASK_ARN"
echo "Run Task Definition: $RUN_TASK_ARN"
```

### 4. Update Dagster Configuration

Update `asset_manager/dagster-prod.yaml` with the actual run task definition ARN:
```yaml
run_launcher:
  module: 'dagster_aws.ecs'
  class: 'EcsRunLauncher'
  config:
    task_definition: '$RUN_TASK_ARN'  # Replace with actual ARN from step 3
    container_name: 'asset-manager-run'
    # ... rest of config
```

### 5. Create Target Group and Listener Rules

```bash
# Get private subnet IDs for ECS services
PRIVATE_SUBNET_IDS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Type,Values=private" \
  --query 'Subnets[*].SubnetId' --output text | tr '\t' ' ')

# Create target group for web service
TG_ARN=$(aws elbv2 create-target-group \
  --name asset-manager-web-tg \
  --protocol HTTP \
  --port 3000 \
  --vpc-id $VPC_ID \
  --target-type ip \
  --health-check-path /server_info \
  --health-check-interval-seconds 30 \
  --health-check-timeout-seconds 5 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 3 \
  --query 'TargetGroups[0].TargetGroupArn' --output text)

# Add listener rule to ALB
aws elbv2 create-rule \
  --listener-arn $HTTPS_LISTENER_ARN \
  --priority 100 \
  --conditions Field=host-header,Values=dg.leverslabs.com \
  --actions Type=forward,TargetGroupArn=$TG_ARN

echo "Target Group ARN: $TG_ARN"
```

### 6. Create ECS Services

```bash
# Create web service (with ALB)
aws ecs create-service \
  --cluster asset-manager \
  --service-name asset-manager-web \
  --task-definition "$WEB_TASK_ARN" \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$PRIVATE_SUBNET_IDS],securityGroups=[$ECS_SG_ID],assignPublicIp=DISABLED}" \
  --load-balancers "targetGroupArn=$TG_ARN,containerName=asset-manager-web,containerPort=3000"

# Create daemon service (no ALB)
aws ecs create-service \
  --cluster asset-manager \
  --service-name asset-manager-daemon \
  --task-definition "$DAEMON_TASK_ARN" \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$PRIVATE_SUBNET_IDS],securityGroups=[$ECS_SG_ID],assignPublicIp=DISABLED}"
```

### 7. Configure Route53 DNS

```bash
# Get ALB DNS name
ALB_DNS_NAME=$(aws elbv2 describe-load-balancers \
  --load-balancer-arns $ALB_ARN \
  --query 'LoadBalancers[0].DNSName' --output text)

ALB_HOSTED_ZONE_ID=$(aws elbv2 describe-load-balancers \
  --load-balancer-arns $ALB_ARN \
  --query 'LoadBalancers[0].CanonicalHostedZoneId' --output text)

# Get Route53 hosted zone ID
HOSTED_ZONE_ID=$(aws route53 list-hosted-zones \
  --query 'HostedZones[?Name==`leverslabs.com.`].Id' --output text | cut -d'/' -f3)

# Create DNS record
aws route53 change-resource-record-sets \
  --hosted-zone-id $HOSTED_ZONE_ID \
  --change-batch '{
    "Changes": [{
      "Action": "CREATE",
      "ResourceRecordSet": {
        "Name": "dg.leverslabs.com",
        "Type": "A",
        "AliasTarget": {
          "DNSName": "'$ALB_DNS_NAME'",
          "HostedZoneId": "'$ALB_HOSTED_ZONE_ID'",
          "EvaluateTargetHealth": false
        }
      }
    }]
  }'

echo "DNS configured: https://dg.leverslabs.com"
```

## Validation

### 1. Health Checks
```bash
# Check service status
aws ecs describe-services --cluster asset-manager --services asset-manager-web asset-manager-daemon

# Check task health
aws ecs list-tasks --cluster asset-manager --service-name asset-manager-web
aws ecs list-tasks --cluster asset-manager --service-name asset-manager-daemon

# Check ALB target health
aws elbv2 describe-target-health --target-group-arn $TG_ARN

# Check security group rules
aws ec2 describe-security-groups --group-ids $ALB_SG_ID $ECS_SG_ID
```

### 2. Application Validation
1. **Access UI**: Navigate to `https://dg.leverslabs.com`
2. **Health endpoint**: Check `https://dg.leverslabs.com/server_info` returns 200
3. **Dagster UI**: Verify assets, schedules, and sensors are visible
4. **Schedules**: Confirm `daily_snowflake_cache_schedule` is RUNNING
5. **Sensors**: Confirm `partition_sync_sensor` is RUNNING and syncing partitions

### 3. Test Run Execution
1. In Dagster UI, manually trigger a partition materialization
2. Verify:
   - ECS run task is launched in the cluster
   - Logs appear in CloudWatch (`dagster/asset_manager/runs`)
   - Compute logs are stored in S3 bucket
   - Snowflake cache load completes successfully

## Operations

### Rolling Updates
```bash
# Build and push new image
docker build -t $account.dkr.ecr.us-west-1.amazonaws.com/asset-manager:$NEW_TAG -f asset_manager/Dockerfile .
docker push $account.dkr.ecr.us-west-1.amazonaws.com/asset-manager:$NEW_TAG

# Update task definitions with new image
# ... (repeat steps 2-3 with new tag)

# Update services
aws ecs update-service --cluster asset-manager --service asset-manager-web --task-definition $NEW_WEB_TASK_ARN --force-new-deployment
aws ecs update-service --cluster asset-manager --service asset-manager-daemon --task-definition $NEW_DAEMON_TASK_ARN --force-new-deployment
```

### Scaling
```bash
# Scale web service for HA (keep daemon at 1)
aws ecs update-service --cluster asset-manager --service asset-manager-web --desired-count 2
```

### Troubleshooting
- **Connection issues**: Check security groups allow traffic between ALB → ECS, ECS → NAT → Internet
- **Database connection**: Verify Supabase allows connections from NAT gateway IPs, SSL is enabled
- **Run tasks failing**: Check ECS task role has permissions for S3, SSM, and task definition is correct
- **Logs**: Check CloudWatch log groups for container logs and S3 bucket for compute logs

## Security Notes

- All secrets stored in SSM Parameter Store with SecureString encryption
- ECS tasks use IAM roles for AWS service access (no hardcoded credentials)
- Supabase connection requires SSL (`PGSSLMODE=require`)
- ALB terminates SSL; internal communication over HTTP in private subnets
- S3 bucket has server-side encryption enabled
