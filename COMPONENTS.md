# AtlasFlow — Components List

A full inventory of everything in the project, organized by category.

---

## 1. Infrastructure services (`docker-compose.yml`)

| Service | What it is | Databricks equivalent | Port(s) |
|---|---|---|---|
| `postgres` | Metadata database — workflows, tasks, runs, users, audit log, usage records, table catalog | Control-plane metastore | 5432 |
| `minio` | S3-compatible object storage | Cloud object storage (S3/ADLS/GCS) | 9000 (API), 9001 (console) |
| `spark-master` | Apache Spark cluster master | Job cluster driver | 7077 (cluster), 8080 (web UI) |
| `spark-worker-1` / `spark-worker-2` | Spark worker nodes (distributed compute) | Job cluster executors | — |
| `spark-thrift` | Spark Thrift Server — standard JDBC/ODBC endpoint | SQL warehouse JDBC/ODBC | 10000 |
| `notebook` | Jupyter Lab, pre-wired to the Spark cluster | Databricks Notebooks | 8888 |
| `mlflow` | Real MLflow tracking server + model registry, artifacts in MinIO | MLflow (Databricks-managed) | 5000 |
| `backend` | The custom orchestrator (FastAPI) — everything AtlasFlow-specific | Jobs/Workflows control plane | 8000 |
| `frontend` | React web UI, served via nginx | Databricks Workspace UI | 3000 |

**Docker volumes**: `pgdata`, `minio-data`, `lake-data`, `mlflow-data` (persist data across restarts; `docker compose down -v` wipes them).

---

## 2. Backend modules (`backend/app/`)

| File | What it does |
|---|---|
| `main.py` | Every REST API endpoint — workflows, runs, tables, secrets, users, SQL, Python, dashboards, models, cluster, billing, SSO |
| `models.py` | Full database schema: `Workflow`, `Task`, `WorkflowRun`, `TaskRun`, `User`, `Secret`, `Table`, `TableGrant`, `Dashboard`, `DashboardTile`, `ModelDeployment`, `AuditLog`, `UsageRecord` |
| `schemas.py` | Pydantic request/response validation for every endpoint |
| `database.py` | SQLAlchemy engine/session setup |
| `executor.py` | The DAG engine — topological task ordering, concurrent execution, retries/backoff, failure propagation, `run_if` conditional branching, task-value passing, cost recording, failure webhooks |
| `scheduler.py` | Cron-based scheduling (APScheduler), independent of manual triggers |
| `task_runners.py` | Executors for each task type: `python`, `shell`, `spark_submit`, `sql`, `notebook` — plus Spark-task load tracking for the autoscaler |
| `catalog.py` | Resolves catalog table names (`sales_data`) to storage paths in SQL text; finds referenced tables for access-control checks |
| `secrets_resolver.py` | Resolves `{{secret:name}}` and `{{task.key.field}}` placeholders just before a task runs; redacts secrets from logs |
| `security.py` | Password hashing, JWT issuance/verification, secret encryption (Fernet), role-based access dependencies |
| `oidc.py` | OIDC SSO (Okta, Azure AD, Google Workspace, Auth0, etc.) |
| `saml.py` | SAML 2.0 SSO (via `python3-saml`) |
| `scim.py` | SCIM 2.0 user provisioning (list/create/replace/patch/delete) |
| `billing.py` | Real Stripe integration — Checkout sessions, webhooks, metered usage reporting |
| `autoscaler.py` | Docker-Engine-API-based controller that creates/removes Spark worker containers by load |

---

## 3. Frontend (`frontend/index.html`) — single-file React app

| Tab / screen | What it covers |
|---|---|
| Login screen | Local username/password, OIDC button, SAML button |
| **Workflows** | Create/trigger/pause/clone/delete workflows; DAG graph with live status; run history; per-task logs and outputs |
| **Data** | Upload CSV/JSON/Parquet → onboard as a Delta table; visibility toggle; access grants; lineage view; feature-table tagging |
| **SQL** | Ad-hoc queries against catalog tables; auto-chart; query history |
| **Python** | Ad-hoc in-process analysis (pandas/deltalake) against onboarded tables |
| **Dashboards** | Multi-tile saved SQL dashboards, live-refreshed, auto-charted |
| **Feature Store** | Browse tables tagged as feature tables (entity key / timestamp key) |
| **Models** | Deploy an MLflow-registered model as a live endpoint; test predictions inline |
| **Cluster** | Live Spark cluster status (workers/cores/memory); auto-scale controller status |
| **Billing** | Usage reports (by user/workflow, CSV export); Stripe Checkout button if configured |
| **Secrets** | Create/list/delete encrypted secrets |
| **Users** | Create users, assign roles (admin-only) |
| **Audit** | Full action log — who did what, when (admin-only) |

---

## 4. Example Spark jobs (`spark_jobs/`)

| Script | Demonstrates |
|---|---|
| `transform_sales.py` | Basic distributed Spark transform |
| `delta_example.py` | Real Delta Lake: ACID writes, `MERGE` upserts, time travel, transaction history |
| `run_sql.py` | Backs the `sql` task type and the SQL tab — runs Spark SQL, returns structured results |
| `onboard_table.py` | Converts an uploaded file into a Delta table (the Data tab's backend) |
| `train_model_example.py` | Logs to real MLflow — params, metrics, model registry |
| `point_in_time_join.py` | Correct point-in-time join for feature-store training sets |

---

## 5. Example workflow definition

`example_workflows/daily_sales_etl.json` — an end-to-end pipeline demonstrating task values, `run_if`, and the `sql` task type together. Paste it into the UI's "+ new" button to try the whole platform in one go.

---

## 6. Documentation

| File | Covers |
|---|---|
| `README.md` | Architecture, quickstart, what maps to what |
| `GETTING_STARTED.md` | Prerequisites, installation, first-run checklist, troubleshooting |
| `HOW_TO_RUN_ON_WINDOWS.md` | No-coding-experience walkthrough |
| `SECURITY.md` | Auth, RBAC, table ACLs, SSO, autoscaler/billing tradeoffs, pre-production checklist |
| `INTEGRATION.md` | REST API reference, task-type integration, external system wiring |
| `GAP_ANALYSIS.md` | Honest, row-by-row comparison against Databricks |
| `.env.example` | Every configurable credential/key, documented |

---

## 7. Key third-party technologies actually integrated (not reimplemented)

Apache Spark · Delta Lake (`delta-spark`) · MLflow · MinIO · PostgreSQL · Spark Thrift Server · Stripe · python3-saml · Docker Engine API · FastAPI · React
