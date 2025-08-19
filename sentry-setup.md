# **Sentry Setup Guide**

This document outlines the steps required to set up and configure **Sentry** error & performance monitoring for all Fulcrum Intelligence services.

---

# **Table of Contents**

1. **Create Sentry Organization & Projects**
2. **Install Dependencies**
3. **Add DSN to Environment**
4. **Initialize Sentry in Code**
   1. *FastAPI Services*
   2. *Prefect / Background Workers*
5. **Verify Integration**
6. **Advanced Configuration**
7. **Troubleshooting**

---

# **1. Create Sentry Organization & Projects**

### **Step 1: Create/Select Organization**

1. Sign in to [Sentry](https://sentry.io/) and create an **Organization** (e.g. `Levers Labs`).

### **Step 2: Create Projects**

Create one **Python** project for each service so events are isolated and dashboards remain clear:

| Service | Project Name | Platform |
|---------|--------------|----------------------|
| Analysis Manager | `analysis-manager` | **Python + Fastapi** |
| Query Manager | `query-manager` | **Python + Fastapi** |
| Story Manager | `story-manager` | **Python + Fastapi** |
| Insights Backend | `insights-backend` | **Python + Fastapi** |
| Tasks Manager | `tasks-manager` | **Python** |

> After each project is created, note the **DSN** shown on the “Install SDK” page – we will add it as an environment variable.

---

# **2. Install Dependencies**

In commons add [sentry-sdk[fastapi]] which will work for Analysis Manager, Query Manager, Story Manager and Insights Backend:

```bash
poetry add sentry-sdk[fastapi]
```

For Tasks Manager add `sentry-sdk`,

```bash
poetry add sentry-sdk
```
---

# **3. Add DSN to Environment**

Store the DSN as an **environment variable** for each service:

```bash
# .env (local development)
SENTRY_DSN="https://<key>@o<org-id>.ingest.sentry.io/<project-id>"
ENVIRONMENT="local"  # dev | staging | prod
```

For ECS deployments update each **deployment/ecs/env.json** file, e.g. for `analysis_manager`:

```json
{
  "name": "SENTRY_DSN",
  "valueFrom": "arn:aws:ssm:$region:$account:parameter/$app/SENTRY_DSN"
}
```
---

# **4. Initialize Sentry in Code**

## **4.1 FastAPI Services (Analysis, Query, Story, Insights)**

Add the following code in `main.py` just before initializing the app:

```python
import sentry_sdk

from insights_backend.config import get_settings  # adjust to service

settings = get_settings()

def get_application() -> FastAPI:
    settings = get_settings()
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,   # 💡 adjust sample rate as needed
        environment=settings.ENV,
    )

    _app = FastAPI(
        ...
    )
```

## **4.2 Tasks Manager (Prefect Worker / Scripts)**

For Tasks Manager, init Sentry at process startup – in `tasks_manager/__init__.py`:

```python
import os
import logging
import sentry_sdk

# Simple Sentry initialization
try:
    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            traces_sample_rate=1.0,
            environment=os.getenv("ENV", "dev"),
        )
        logging.getLogger(__name__).info("Sentry initialized for Tasks Manager")
except ImportError:
    logging.getLogger(__name__).warning("Sentry SDK not available")
except Exception as e:
    logging.getLogger(__name__).error(f"Failed to initialize Sentry: {e}")
```

---

# **5. Verify Integration**

Run any service locally and intentionally raise an exception:

```python
from fastapi import APIRouter, HTTPException
router = APIRouter()

@router.get("/sentry-test")
async def sentry_test():
    1 / 0  # deliberate ZeroDivisionError
```

Open the endpoint → confirm the event appears in Sentry under the correct project.

---

# **6. Advanced Configuration**

| Feature | Why Use It | How |
|---------|-----------|------|
| **Tagging** | Add tenant / request ID context | `scope.set_tag("tenant_id", tenant_id)` |
| **Breadcrumbs** | Custom breadcrumbs for domain events | `sentry_sdk.add_breadcrumb(...)` |
| **Tracing** | End-to-end performance | Wrap async tasks with `sentry_sdk.start_transaction()` |
| **Health Checks** | Filter noisy 200s | Ignore `/health` route in `before_send` |
| **Alert Rules** | Set up basic email alerts for new issues | Go to Sentry → Issues → Alerts → Create Alert Rule → Choose "Issues" → Set conditions (e.g., "When an event is first seen") → Add email action
| **Slack Integration** | Real-time error notifications | Settings → Integrations → Slack → Install & configure channels

---

# **7. Troubleshooting**

| Issue | Possible Cause | Fix |
|-------|----------------|-----|
| **No events in Sentry** | DSN not loaded / init missing | Verify `SENTRY_DSN` is set & printed on startup |
| **Events show wrong project** | Copied DSN from another service | Double-check each DSN per project |
| **High ingest cost** | `traces_sample_rate` = 1.0 in prod | Lower to `0.1` or disable performance for non-critical services |
| **Duplicate events in workers** | Multiple `sentry_sdk.init()` calls | Ensure init runs only once per process |
| **Missing stack traces** | Raised exceptions handled & swallowed | Re-raise or use `sentry_sdk.capture_exception(e)` manually |

> 📚 Reference: [Sentry Python SDK Docs](https://docs.sentry.io/platforms/python/).

---

🎉 **Sentry setup complete!**
