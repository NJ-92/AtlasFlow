# AtlasFlow — a Databricks-style platform

A self-hosted analytics platform built the same way Databricks-alternatives (EMR, Dataproc,
open-source lakehouse stacks) actually are: proven distributed-systems components underneath,
plus a purpose-built workflow orchestrator on top that ties them together with a web UI.

**New here?** See [`GETTING_STARTED.md`](./GETTING_STARTED.md) for prerequisites,
installation, and a first-run checklist. For wiring external systems into AtlasFlow, see
[`INTEGRATION.md`](./INTEGRATION.md). For a full feature comparison against Databricks,
see [`GAP_ANALYSIS.md`](./GAP_ANALYSIS.md). For everything below, see
[`SECURITY.md`](./SECURITY.md).

## Security

AtlasFlow now ships with:
- **Authentication** — JWT-based login, plus optional **OIDC and SAML SSO**, plus **SCIM** user provisioning
- **Role-based access control** — `viewer` (read-only) / `editor` (create/trigger/pause) / `admin` (delete, manage users, manage secrets, view audit log)
- **Table-level governance** — public/restricted visibility + per-user grants on catalog tables, enforced across SQL, Python, and dashboards
- **Encrypted secrets store** — credentials are encrypted at rest (Fernet) and referenced in tasks as `{{secret:name}}`; values are redacted from persisted logs and never returned by the API once written
- **Audit logging** — every login and mutating action is recorded with user, timestamp, and IP
- **Rate-limited login** — brute-force protection on `/api/auth/login`
- **Restricted CORS** — explicit origin allowlist instead of `*`
- **Bootstrap admin account** — auto-created on first boot; GETTING_STARTED.md walks through securing it

See [`SECURITY.md`](./SECURITY.md) for the full model, and `.env.example` for every
credential/key you should override before deploying anywhere beyond your own machine.

## Using it without writing any code

Tabs in the web UI cover the everyday "data storage, query, analyze, track, serve" workflow
without touching JSON or the API directly:

- **Data tab** — upload a CSV/JSON/Parquet file, give it a name, and it's converted into
  a real Delta table and registered in a lightweight catalog. Reference it by name
  (`SELECT * FROM sales_data`) instead of a raw storage path. Mark it **restricted** to
  control who can read it (grant access per-user), tag it as a **feature table**, or view
  its **lineage** (what it was derived from / what's derived from it).
- **SQL tab** — write SQL against any onboarded table, see results as a table, get an
  automatic bar chart when the result shape supports it, and revisit past queries via
  **history**.
- **Python tab** — quick, in-process Python (pandas + deltalake preinstalled) for
  exploring onboarded tables without waiting on the Spark cluster; for large-scale
  distributed work, use a `spark_submit` task in a workflow instead.
- **Dashboards tab** — save several SQL tiles together under one dashboard; refreshing it
  re-runs every tile live and auto-charts each result.
- **Feature Store tab** — browse tables tagged as feature tables (entity key + timestamp
  key); `spark_jobs/point_in_time_join.py` does correct point-in-time joins against them.
- **Models tab** — deploy an MLflow-registered model as a live prediction endpoint and test
  it right from the UI.
- **Cluster tab** — live, read-only visibility into the real Spark cluster (workers, cores,
  memory, running apps), plus an optional auto-scale controller that creates/removes real
  worker containers by load (bounded by your host's physical resources — see SECURITY.md
  §2c for the docker.sock tradeoff it requires).
- **Billing tab** — usage reports (by user or workflow, CSV export) for everyone; if Stripe
  is configured, also real metered billing via Stripe Checkout — actual charges, not a
  simulation.

Any standard BI tool (Tableau, PowerBI, DBeaver, etc.) can also connect directly via the
**Spark Thrift Server** at `jdbc:hive2://localhost:10000` — a real JDBC/ODBC endpoint, not
a custom integration.

Workflow-list operational niceties (mirroring Databricks' Jobs list):
- The status dot next to each workflow reflects its **actual last run status** (success/failed/running/skipped), and the sidebar shows the **next scheduled run time** for cron-scheduled workflows
- **Clone** duplicates a workflow (all tasks included) as a starting point for a new one — the clone starts paused so it never silently double-runs a schedule
- Set `on_failure_webhook` on a workflow to get a POSTed JSON summary the moment a run fails — no need to wire a "notify" task into every DAG just to catch failures
- Every run shows an **estimated compute cost** (configurable $/worker-hour) — a self-hosted usage estimate, not real vendor billing

## Architecture — what maps to what

| Databricks concept | Here |
|---|---|
| Job clusters (distributed Spark) | Real **Apache Spark** cluster (1 master + N workers), via Docker |
| Delta Lake / data lakehouse | **MinIO** + real **Delta Lake** (`delta-spark`, ACID transactions, time travel, MERGE) |
| Workflows / Jobs (DAGs, schedules, retries, task values, conditional tasks) | **Custom orchestrator** — `backend/`, the code below |
| Notebooks | **Notebook server**, pre-wired to the Spark cluster |
| SQL warehouses / ad-hoc SQL / dashboards | **Spark SQL** against Delta tables, via the `sql` task type, the SQL tab, and the Dashboards tab |
| BI tool connectivity | Real **Spark Thrift Server** (JDBC/ODBC, port 10000) |
| MLflow experiment tracking + model registry + serving | Real **MLflow** server, wired to MinIO for artifacts; the Models tab deploys registered models as live endpoints |
| Feature Store | Table tagging + `point_in_time_join.py`, browsable in the Feature Store tab |
| Unity Catalog (partial) | Lightweight table catalog with **visibility/grants** and basic **lineage** |
| SSO | Real **OIDC** (Okta, Azure AD, Google Workspace, etc.) |
| Job/run history, metastore | **Postgres** |
| Workspace UI | **React web app** — `frontend/index.html` |

Nothing here is a toy stub: the Spark cluster is real Spark and will distribute work across
workers, MinIO is real S3-compatible storage, and the orchestrator actually executes DAGs
with retries/backoff, parallel task execution, and failure propagation — not a mockup.

## Quickstart

```bash
docker compose up --build
```

Then open:
- **Web UI**: http://localhost:3000
- **Orchestrator API docs**: http://localhost:8000/docs
- **Spark master UI**: http://localhost:8080 (watch jobs get distributed across workers)
- **Notebooks**: http://localhost:8888
- **MLflow**: http://localhost:5000 (experiment tracking + model registry)
- **MinIO console**: http://localhost:9001 (user `admin` / pass `admin12345`)

Click **"+ new"** in the sidebar, paste (or edit) the example in
`example_workflows/daily_sales_etl.json`, and hit **Run now**. Watch the DAG animate as tasks
execute, and click any run to see per-task logs.

## How the orchestrator works (`backend/`)

- `app/catalog.py` — resolves friendly table names (`sales_data`) to their underlying Delta
  path in SQL text, so you never have to type/remember raw storage paths day-to-day
- `app/models.py` — schema: `Workflow` → `Task`s (with `depends_on` edges, optional `run_if`) → `WorkflowRun` → `TaskRun` (with `output` for task values); also `Table` (the catalog)
- `app/executor.py` — the actual DAG engine: topologically sorts tasks into layers, evaluates
  each task's `run_if` expression against upstream task values/status before running it
  (Databricks-style conditional tasks), runs each layer's independent tasks concurrently
  (`ThreadPoolExecutor`), retries failed tasks with exponential backoff, propagates failure
  and skips downstream (`upstream_failed` / `skipped`), and captures each task's output for
  the next task to consume
- `app/secrets_resolver.py` — resolves `{{secret:name}}` and `{{task.key.field}}`
  placeholders just before a task runs, and redacts secret values from persisted logs
- `app/task_runners.py` — pluggable executors per task type:
  - `python` — runs in-process; assign to `task_output` to pass values downstream
  - `shell` — any CLI tool; print a line starting `ATLASFLOW_OUTPUT_JSON:` to pass values downstream
  - `spark_submit` — submits to the real Spark cluster, with Delta Lake auto-attached, for distributed, large-scale processing
  - `sql` — runs Spark SQL against Delta tables in the lakehouse; results become the task's output automatically
  - `notebook` — executes a `.ipynb` end-to-end via `papermill`
- `app/scheduler.py` — cron-based scheduling (`APScheduler`), independent of manual triggers
- `app/main.py` — REST API the UI talks to (`/api/workflows`, `/api/workflows/{id}/trigger`, `/api/sql/query`, etc.)

## Data engineering, SQL, and ML — worked examples

- `spark_jobs/delta_example.py` — real Delta Lake: ACID writes, `MERGE` upserts, time travel
  (`versionAsOf`), and full transaction history — run it as a `spark_submit` task
- `spark_jobs/run_sql.py` — backs both the `sql` task type and the UI's SQL tab; runs
  Spark SQL against Delta tables and returns structured results
- `spark_jobs/train_model_example.py` — logs params/metrics to real MLflow and registers a
  model version in MLflow's model registry
- `example_workflows/daily_sales_etl.json` — an end-to-end pipeline demonstrating task
  values (`{{task.extract.row_count}}`), a `run_if` condition, and the `sql` task type
  together

## Scaling to "large, cluster-scale" data

This is the honest part: scaling out is a matter of adding Spark workers, not rewriting code.

```bash
docker compose up --scale spark-worker-1=6   # add more Spark workers
```

For real production scale beyond a single host:
- Run the Spark workers on separate machines (or swap in **Kubernetes** as the Spark cluster
  manager — Spark supports this natively) instead of Docker Compose
- Point Spark/notebooks at real **S3** (or keep MinIO, self-hosted) and write data as
  **Delta Lake** or **Apache Iceberg** tables for ACID transactions, time travel, schema evolution
- Move Postgres to a managed instance (RDS/Cloud SQL) once run-history volume grows
- Run multiple orchestrator backend replicas behind a load balancer for HA (the executor is
  stateless per-run; runs are tracked in Postgres so this is safe)

## What this is / isn't

**Is:** a real, working orchestration engine + real distributed Spark compute + real object
storage + a UI to drive it — genuinely deployable and extensible.

**Isn't:** feature-parity with Databricks. Authentication, role-based access control, secrets
management, and audit logging are now in place (see Security above). Still missing, and
non-trivial to add: SSO/SAML, autoscaling clusters, a Delta Lake transaction log
implementation, MLflow-style experiment tracking, a drag-and-drop notebook-cell-in-DAG
builder, and fine-grained data governance (Unity Catalog equivalent). See
`GAP_ANALYSIS.md` for the full breakdown. Treat this as the skeleton you'd extend, not a
drop-in replacement.

## Project layout

```
backend/            FastAPI orchestrator (the workflow engine)
frontend/            React UI, single file, no build step
spark_jobs/           Example PySpark job, mounted into Spark + backend containers
example_workflows/    Sample DAG JSON you can paste into the UI
docker-compose.yml    Wires: postgres, minio, spark-master, spark-worker x2, notebook, backend, frontend
```
