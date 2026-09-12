# AtlasFlow — Getting Started

Everything needed to go from a fresh machine to a running AtlasFlow platform.

---

## 1. System requirements

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16 GB+ (Spark workers are memory-hungry) |
| Disk | 10 GB free | 30 GB+ (notebook outputs, Spark shuffle files, MinIO data all accumulate) |
| OS | Linux, macOS, or Windows with WSL2 | Linux |

AtlasFlow runs entirely in Docker containers, so the host itself only needs Docker — no
Python, Java, or Spark installed locally.

---

## 2. Prerequisite software

| Tool | Why it's needed | Check you have it |
|---|---|---|
| **Docker Engine** 24+ | Runs every service (Postgres, MinIO, Spark, backend, frontend) | `docker --version` |
| **Docker Compose** v2 (bundled with Docker Desktop) | Wires all services together | `docker compose version` |
| **A modern browser** | Web UI, Spark UI, Jupyter, MinIO console | — |
| `curl` and `jq` *(optional)* | Useful for scripting API calls (see INTEGRATION.md) | `curl --version`, `jq --version` |

Install Docker:
- **macOS/Windows**: Docker Desktop — https://www.docker.com/products/docker-desktop
- **Linux**: `curl -fsSL https://get.docker.com | sh`, then add your user to the `docker`
  group (`sudo usermod -aG docker $USER`, then log out/in)

No API keys, licenses, or external accounts are required — everything self-hosts.

---

## 3. Ports used

AtlasFlow binds these ports on the host. Make sure nothing else is using them, or edit
`docker-compose.yml` to remap (`"HOST:CONTAINER"`) if there's a conflict.

| Port | Service | Purpose |
|---|---|---|
| 3000 | frontend | Web UI |
| 8000 | backend | Orchestrator REST API + docs (`/docs`) |
| 8080 | spark-master | Spark cluster web UI |
| 7077 | spark-master | Spark cluster port (internal, used by `spark-submit`) |
| 8888 | notebook | Jupyter notebook server |
| 5000 | mlflow | MLflow experiment tracking + model registry UI |
| 9000 | minio | S3-compatible API |
| 9001 | minio | MinIO web console |
| 5432 | postgres | Metadata database (only needed if you want to connect a DB client directly) |
| 10000 | spark-thrift | JDBC/ODBC endpoint for BI tools (Tableau, PowerBI, DBeaver) |

```bash
# quick check for conflicts before starting (macOS/Linux)
lsof -i :3000 -i :8000 -i :8080 -i :8888 -i :5000 -i :9000 -i :9001 -i :5432 -i :10000
```

---

## 4. First-time setup

```bash
unzip atlasflow.zip
cd atlasflow

cp .env.example .env    # optional for local testing, required before any shared use — see SECURITY.md
docker compose up --build
```

First run will take a few minutes — it's pulling/building the Postgres, MinIO, Spark
(x3), Jupyter, backend, and frontend images. Subsequent starts are fast (`docker compose up`,
no `--build` needed unless you change backend/frontend code).

### Startup checklist — verify each service came up clean

| # | Check | Expected |
|---|---|---|
| 1 | `docker compose ps` | All services show `running`/`healthy` |
| 2 | Open `http://localhost:8000/api/health` | `{"status": "ok"}` |
| 3 | Open `http://localhost:3000` | AtlasFlow UI loads, sidebar says "No workflows yet" |
| 4 | Open `http://localhost:8080` | Spark master UI shows 2 workers registered |
| 5 | Open `http://localhost:9001` | MinIO console login page (user `admin` / pass `admin12345`) |
| 6 | Open `http://localhost:8888` | Jupyter Lab loads (no password/token needed — dev config) |

### Run your first workflow

1. Open `localhost:3000` and sign in as `admin` with the password from
   `ATLASFLOW_ADMIN_BOOTSTRAP_PASSWORD` (default `atlasflow-admin`) — then change it
   immediately: create a personal admin user via the **Users** tab and stop using the
   bootstrap account day-to-day (see `SECURITY.md`)
2. Click **"+ new"**
3. Paste the contents of `example_workflows/daily_sales_etl.json` (already prefilled by
   default) and click **Create workflow**
4. Select it in the sidebar, click **Run now**
5. Watch the DAG animate — edges pulse amber while tasks run, then turn green/red
6. Click the run in **Run history** to see per-task logs
7. Check the Spark master UI (`localhost:8080`) — you'll see the `transform_spark` task's
   job appear and complete there, distributed across the 2 workers
8. Check the **Audit** tab — you should see `auth.login` and `workflow.create`/`trigger`
   entries recorded
9. Try the **Data** tab: upload any small CSV, give it a name, and once it finishes
   onboarding, switch to the **SQL** tab and run `SELECT * FROM your_table_name LIMIT 10`

If all nine steps above work, the platform is fully operational.

---

## 5. Stopping / resetting

```bash
docker compose down           # stop everything, keep data (Postgres, MinIO, Spark data)
docker compose down -v        # stop everything AND wipe all data volumes (clean slate)
docker compose up --scale spark-worker-1=5   # scale Spark workers up/down on the fly
```

---

## 6. Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `port is already allocated` on startup | Another process is using one of the ports in §3 | Stop the conflicting process, or remap the port in `docker-compose.yml` |
| Backend container restarts in a loop | Postgres wasn't ready yet | It should self-heal (backend `depends_on` a Postgres healthcheck) — if not, run `docker compose logs backend` |
| `spark_submit` task fails with connection refused | Spark master isn't up yet, or workflow was triggered before `docker compose up` finished | Confirm `localhost:8080` shows 2 workers before triggering runs |
| Workflow UI shows blank DAG | Task `depends_on` references a `key` that doesn't exist in the same workflow | Check task `key`s match exactly (case-sensitive) in your workflow JSON |
| Notebook task can't reach Spark | Notebook wasn't pointed at `spark://spark-master:7077` | Set that as the master URL when creating a `SparkSession` inside the notebook |
| Out-of-memory Spark job | Default worker memory (`2G` each) too low for your data | Raise `SPARK_WORKER_MEMORY` in `docker-compose.yml` per worker, or add more workers |

For anything else: `docker compose logs -f <service>` (e.g. `backend`, `spark-master`) is
the first place to look — every service logs to stdout, captured by Docker.

---

## 7. Before using this beyond local/dev

AtlasFlow now includes authentication, RBAC, encrypted secrets, and audit logging out of
the box (see [`SECURITY.md`](./SECURITY.md) for the full model). Before exposing it beyond
your machine, work through the checklist at the bottom of `SECURITY.md` — at minimum:
generate a real `ATLASFLOW_JWT_SECRET` and `ATLASFLOW_ENCRYPTION_KEY`, change every default
credential in `.env`, change the default admin password, restrict `ATLASFLOW_CORS_ORIGINS`,
and put TLS in front of the frontend and backend.
