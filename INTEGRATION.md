# AtlasFlow — Integration Documentation

This document describes how to integrate external systems (CI/CD, applications, cron,
other services) with the AtlasFlow workflow orchestrator, and how the platform's
internal components integrate with each other end-to-end.

---

## 1. Integration overview

There are two integration surfaces:

1. **External → Orchestrator** — how your systems trigger, monitor, and consume the
   results of workflows via the REST API.
2. **Orchestrator → Compute/Storage** — how the orchestrator itself integrates with the
   Spark cluster, the data lake, and notebooks when it executes a task. You'll touch this
   layer when writing new task types or connecting a new data source.

```
Your system  →  Orchestrator API  →  DAG executor  →  Spark / notebook / shell  →  Data lake
     ↑                                                                                 │
     └───────────────────────  poll GET /api/runs/{id}  ─────────────────────────────┘
```

---

## 2. End-to-end flow (step by step)

This is what happens, in order, from the moment an external system triggers a run to the
moment it collects results.

**Step 1 — Define the workflow (one-time setup)**
Send a `POST /api/workflows` request describing the DAG: tasks, their `type`
(`python` / `shell` / `spark_submit` / `notebook`), and their `depends_on` edges. The
orchestrator validates the graph is acyclic and stores it in Postgres.

**Step 2 — Trigger a run**
Your system calls `POST /api/workflows/{workflow_id}/trigger`. The orchestrator:
- Creates a `WorkflowRun` row with status `pending`
- Immediately returns `{ "run_id": "..." }` — this call does not block on execution
- Kicks off execution on a background thread

*(Alternative: instead of an external call, a `schedule_cron` on the workflow causes the
same trigger to fire automatically — see §5.)*

**Step 3 — DAG execution**
The executor (`app/executor.py`):
1. Loads all tasks for the workflow and topologically sorts them into dependency layers
2. Runs all tasks in a layer concurrently (they have no dependency on each other)
3. For each task, dispatches to the runner matching its `type` (see §4)
4. On task failure, retries with exponential backoff up to `retries` times
5. If a task ultimately fails, every downstream task is marked `upstream_failed` and
   skipped — it does not attempt to run
6. Updates the `TaskRun` row's status/logs after every attempt, and the parent
   `WorkflowRun`'s status once all layers complete

**Step 4 — Task execution against compute/storage**
Depending on task type, execution reaches out to the Spark cluster (`spark_submit`),
runs a notebook against the same cluster (`notebook`), or executes directly on the
orchestrator host (`python`/`shell`). Spark jobs typically read/write the data lake
(MinIO/S3) — see §6.

**Step 5 — Poll for status**
Your system polls `GET /api/runs/{run_id}` until `status` is `success` or `failed`.
Each poll also returns per-task status and logs, so you can surface progress
incrementally rather than waiting for the whole run.

**Step 6 — Consume results**
Once `status: success`, downstream consumers read the output your tasks wrote — typically
files in the data lake (e.g. the Parquet output of `transform_sales.py`), or a message
posted to your own system as the final task in the DAG (see §7 for that pattern).

---

## 3. API integration reference

Base URL: `http://<orchestrator-host>:8000` (interactive docs at `/docs`).

### Create a workflow
```
POST /api/workflows
Content-Type: application/json

{
  "name": "daily_sales_etl",
  "description": "Extract, transform, load sales data",
  "schedule_cron": "0 2 * * *",
  "tasks": [
    { "key": "extract", "type": "shell", "command": "...", "depends_on": [], "params": {}, "retries": 0, "timeout_seconds": 600 },
    { "key": "transform", "type": "spark_submit", "command": "/jobs/transform_sales.py",
      "depends_on": ["extract"], "params": { "args": ["--input", "/data/raw", "--output", "/data/curated"] },
      "retries": 1, "timeout_seconds": 3600 }
  ]
}
```
Returns the created workflow, including its generated `id`.

### Trigger a run
```
POST /api/workflows/{workflow_id}/trigger
```
Returns immediately: `{ "run_id": "..." }`.

### Poll run status
```
GET /api/runs/{run_id}
```
Returns `status` (`pending` / `running` / `success` / `failed`), and a `task_runs` array
with each task's `status`, `attempt`, `started_at`, `finished_at`, and `logs`.

### Run an ad-hoc SQL query
```
POST /api/sql/query
{ "sql": "SELECT region, SUM(total_sales) FROM delta.`/data/curated` GROUP BY region", "limit": 200 }
```
Returns `{ columns, rows, row_count, logs }`. Runs on the real Spark cluster with Delta
Lake enabled; requires `editor` role or higher. Every query is audit-logged.

### List recent runs for a workflow
```
GET /api/workflows/{workflow_id}/runs
```

### Pause / resume scheduled runs
```
POST /api/workflows/{workflow_id}/pause
```
Toggles `is_paused`; a paused workflow's cron schedule is skipped (manual triggers still work).

### Delete a workflow
```
DELETE /api/workflows/{workflow_id}
```

**Example: trigger-and-wait from a shell script (e.g. a CI pipeline step)**
```bash
RUN_ID=$(curl -s -X POST http://orchestrator:8000/api/workflows/$WF_ID/trigger | jq -r .run_id)

while true; do
  STATUS=$(curl -s http://orchestrator:8000/api/runs/$RUN_ID | jq -r .status)
  case "$STATUS" in
    success) echo "Pipeline succeeded"; exit 0 ;;
    failed)  echo "Pipeline failed"; exit 1 ;;
    *)       sleep 5 ;;
  esac
done
```

---

## 4. Integrating a task with a compute target

Each task's `type` determines which runner in `app/task_runners.py` executes it. To
integrate a new piece of work, pick the type that matches where it needs to run:

| Task type | Where it runs | When to use |
|---|---|---|
| `python` | In-process, on the orchestrator | Lightweight glue logic, validation, notifications |
| `shell` | Subprocess, on the orchestrator | Any CLI tool — `curl`, `dbt`, `aws s3 cp`, custom scripts |
| `spark_submit` | Distributed, on the Spark cluster (Delta Lake auto-attached) | Large-scale data transforms — anything that needs to scan/join large datasets, or read/write Delta tables |
| `sql` | Distributed, on the Spark cluster | Ad-hoc or scheduled SQL against Delta tables — results become the task's output automatically |
| `notebook` | On the orchestrator, connected to Spark | Exploratory or parameterized analysis defined as a `.ipynb` |

**To add a `spark_submit` task:**
1. Write your PySpark job as a standalone script (see `spark_jobs/transform_sales.py`
   for the pattern — `argparse` for inputs, `SparkSession.builder` to connect)
2. Mount it into the `backend` and `spark-master`/`spark-worker` containers via the
   `./spark_jobs:/jobs` volume in `docker-compose.yml` (already wired — drop new scripts
   in that folder)
3. Reference it as the task's `command` (e.g. `/jobs/your_job.py`), and pass CLI args via
   `params.args`

**To add a `notebook` task:**
1. Place the `.ipynb` in the `./notebooks` folder (mounted into the `notebook` service)
2. Set the task's `command` to its path, and `params` to the values you want injected as
   notebook parameters (requires a `parameters` cell tagged in the notebook, per papermill
   convention)

**To integrate a brand-new task type** (e.g. calling a third-party API, or a dbt run):
Add a function to `RUNNERS` in `app/task_runners.py` with signature
`(command: str, params: dict) -> tuple[bool, str]`, and add the type name to the
`TaskType` enum in `app/models.py`.

---

## 4b. Passing data between tasks & conditional execution

**Task values** — have a task hand a small piece of data to the tasks that depend on it:
- `python`/`spark_submit` (script) tasks: assign a dict to a variable named `task_output`
- `shell`/`spark_submit` (script) tasks: print a line `ATLASFLOW_OUTPUT_JSON:{"key": "value"}` to stdout
- `sql` tasks: automatic — the query's result rows/columns become the task's output

Downstream tasks reference it two ways:
- Inline in `command` or any string `params` value: `{{task.extract.row_count}}`
- Programmatically: every python/shell/spark task receives the full upstream context as
  `params['upstream']` (python) or the `ATLASFLOW_UPSTREAM_JSON` env var (shell/spark)

**Conditional execution** — set a task's `run_if` to a Python expression; it's evaluated
against `upstream` (task values) and `upstream_status` (dict of dependency → status) just
before the task would run. A falsy result skips the task (and propagates the skip
downstream), e.g.:
```json
{ "key": "notify", "run_if": "upstream['extract']['row_count'] > 0", "depends_on": ["extract"], ... }
```

## 4c. ML tracking integration (MLflow)

Every task's environment already has `MLFLOW_TRACKING_URI` set, so any `python` or
`spark_submit` task can log directly to the real MLflow server:
```python
import mlflow
mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_experiment("my-experiment")
with mlflow.start_run():
    mlflow.log_params({...})
    mlflow.log_metric("accuracy", 0.94)
    mlflow.sklearn.log_model(model, "model", registered_model_name="my-model")
```
See `spark_jobs/train_model_example.py` for a complete example. View runs and registered
models at `http://localhost:5000`.

## 5. Scheduling integration (cron-triggered workflows)

Set `schedule_cron` on a workflow (standard 5-field cron syntax) to have it run
automatically — no external trigger call needed. On every backend startup, and whenever a
workflow is created/updated, `app/scheduler.py` re-reads all workflows and registers a
cron job per active schedule via APScheduler. Pausing a workflow removes its schedule
until resumed.

---

## 6. Data lake integration (MinIO / S3)

Spark jobs and notebooks are pre-wired to reach the Spark cluster; to read/write the
lakehouse, point them at MinIO using the S3A connector:

```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin12345") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.parquet("s3a://your-bucket/path/")
```
Create buckets via the MinIO console (`localhost:9001`) or the `mc` CLI / `boto3` before
first use. To integrate with a real cloud provider instead of self-hosted MinIO, swap the
endpoint/credentials for your S3/GCS/ADLS equivalent — the Spark-side code is unchanged.

---

## 7. Notifying external systems on completion

The orchestrator doesn't push webhooks natively; the standard integration pattern is to
make the **last task in your DAG** the notification step, using `shell` or `python`:

```json
{ "key": "notify", "type": "shell",
  "command": "curl -X POST https://your-system/webhooks/pipeline-done -d '{\"workflow\":\"daily_sales_etl\"}'",
  "depends_on": ["quality_check"] }
```
Because this task only runs after its dependencies succeed (and is skipped as
`upstream_failed` if they don't), it doubles as a success signal — if your webhook fires,
the pipeline succeeded.

---

## 8. Failure handling contract

When integrating, rely on these semantics rather than re-implementing them:

- A task's `status` is `failed` only after all of its `retries` are exhausted
- Any task with a failed dependency gets `status: upstream_failed` and never executes
- A `WorkflowRun`'s overall `status` is `failed` if **any** task in it failed or was
  `upstream_failed`, otherwise `success`
- Logs from every attempt are concatenated in `TaskRun.logs`, separated by
  `--- attempt N ---` markers, so retries are fully auditable

---

## 9. Reference: sequence diagram

```
External System        Orchestrator API        DAG Executor          Spark Cluster
       │                       │                      │                     │
       │  POST /trigger        │                      │                     │
       ├──────────────────────>│                      │                     │
       │  { run_id }            │  spawn background   │                     │
       │<──────────────────────┤─────────────────────>│                     │
       │                       │                      │  spark-submit       │
       │                       │                      ├────────────────────>│
       │  GET /runs/{id}        │                      │   job result        │
       ├──────────────────────>│                      │<────────────────────┤
       │  status: running       │                      │                     │
       │<──────────────────────┤                      │                     │
       │        ...poll loop...                        │                     │
       │  GET /runs/{id}        │                      │                     │
       ├──────────────────────>│                      │                     │
       │  status: success       │                      │                     │
       │<──────────────────────┤                      │                     │
```
