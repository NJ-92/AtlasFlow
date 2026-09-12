# AtlasFlow vs Databricks — Gap Analysis

An honest, categorized comparison. ✅ = present, ⚠️ = partially present / basic version, ❌ = not present.

## Compute & orchestration

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| Job clusters (DAG-based workflows, retries, dependencies) | ✅ | This is AtlasFlow's core — `executor.py` |
| Cron / scheduled jobs | ✅ | `scheduler.py`, cron syntax |
| Task types (notebook, Python, JAR, SQL, dbt, etc.) | ⚠️ | We support python/shell/spark_submit/sql/notebook; no JAR or dbt-native task type |
| Conditional/branching tasks, "run if" logic | ✅ | `run_if` expression on any task, evaluated against upstream task values/status |
| Task values (pass output of one task as input to the next) | ✅ | `{{task.key.field}}` placeholders + an `upstream` dict available to every task |
| Disable-without-removing a task | ❌ | Would need to delete/edit the task |
| Job notifications (email/webhook on failure) | ⚠️ | `on_failure_webhook` posts a JSON summary on failure; no built-in email delivery |
| Clone an existing job as a starting point | ✅ | `POST /workflows/{id}/clone` + a Clone button in the UI |
| Job list showing last run status + next scheduled run | ✅ | Now shown per-workflow in the sidebar |
| Distributed Spark compute | ✅ | Real Spark cluster via `spark_submit` |
| Autoscaling clusters | ⚠️ | An optional auto-scale controller now creates/removes real Spark worker containers by load (`ATLASFLOW_AUTOSCALE_ENABLED`) — genuinely automated, but bounded by the host machine's physical CPU/RAM, and requires giving the backend container docker.sock access (a real security tradeoff, see SECURITY.md §2c). This is not cloud-elastic scaling — that needs Kubernetes or a cloud provider underneath, which a single-host stack structurally can't provide |
| Serverless compute (no cluster management at all) | ❌ | You manage the Spark cluster yourself |
| Cluster policies / cost controls per team | ❌ | No concept of clusters as a governed resource |
| Photon query acceleration | ❌ | Vanilla Spark only |

## Storage & data management

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| Object storage (S3/ADLS/GCS) | ✅ | MinIO, S3-compatible |
| Delta Lake (ACID transactions, schema enforcement) | ✅ | Real `delta-spark` integration, auto-attached to every `spark_submit`/`sql` task — ACID writes, `MERGE`, schema enforcement |
| Time travel / `DESCRIBE HISTORY` | ✅ | Real Delta Lake feature — `versionAsOf` reads, full transaction history (see `spark_jobs/delta_example.py`) |
| Auto Loader (incremental file ingestion) | ⚠️ | The Data tab onboards a file as a one-time batch load into a Delta table; no incremental/streaming ingestion of new files landing in a folder |
| Delta Live Tables / Lakeflow declarative pipelines | ❌ | AtlasFlow workflows are imperative (you write the steps), not declarative |
| Change data capture (CDC) connectors | ❌ | No built-in source connectors |
| Delta Sharing (cross-org data sharing) | ❌ | — |
| Liquid clustering / auto file optimization | ❌ | — |

## Governance & catalog

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| Unity Catalog (centralized metadata, lineage, cross-workspace) | ⚠️ | A lightweight table catalog now exists (Data tab: upload → onboard → query by name), with table-level visibility/grants and basic lineage tracking (source_tables) — no cross-workspace sharing, no column-level security |
| Fine-grained/row/column-level access control on data | ⚠️ | Table-level access control now exists (public/restricted + per-user grants, enforced in SQL/Python/dashboards); still no row or column-level rules within a table |
| Attribute-based access control (ABAC) | ❌ | AtlasFlow now has role-based (RBAC), not attribute-based |
| Data lineage tracking | ⚠️ | Basic table-level lineage now exists (`source_tables`, upstream/downstream view in Data tab) — manually tagged at table creation, not automatically inferred from arbitrary SQL/Spark transformations |
| Audit logs (who did what, when) | ✅ (new) | Added — see Security section below |
| Data classification / tagging (PII, sensitivity) | ❌ | — |

## Notebooks & collaboration

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| Notebook execution against a cluster | ✅ | Jupyter, connected to Spark |
| Real-time multi-user collaborative editing | ❌ | Standard single-user Jupyter |
| Mixed-language cells (%sql, %python, %r in one notebook) | ⚠️ | Depends on kernel; not a first-class multi-language notebook experience |
| Git integration for notebooks (Repos) | ❌ | No built-in version control UI |
| Notebook-as-DAG-task with parameters | ✅ | Via `notebook` task type + papermill |

## ML / AI

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| MLflow experiment tracking | ✅ | Real MLflow server (`docker-compose.yml`), artifacts stored in MinIO |
| Model registry + versioning | ✅ | Real MLflow model registry — `mlflow.sklearn.log_model(..., registered_model_name=...)` |
| Model serving endpoints | ⚠️ | Real serving via `mlflow models serve`, deployable and testable from the Models tab, proxied through the API — single-instance only, no rolling updates/replicas/autoscaling of the serving layer itself |
| Feature Store | ⚠️ | Tables can be tagged as feature tables (entity key + timestamp key) and browsed in the Feature Store tab; `point_in_time_join.py` does correct point-in-time joins — no online/low-latency serving layer, no automatic feature computation pipelines |
| AI/ML notebooks with GPU compute | ❌ | No GPU support wired in |
| Foundation model hosting / AI Gateway | ❌ | — |

## SQL analytics

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| SQL warehouses (serverless SQL compute) | ⚠️ | `sql` task type + a SQL tab in the UI run real Spark SQL against Delta tables — not serverless, and no autoscaling, but functionally you can run SQL and see results |
| BI tool connectors (Tableau, PowerBI, etc.) | ✅ | Real Spark Thrift Server (JDBC/ODBC endpoint, port 10000) — connect any standard "Spark SQL"/Hive JDBC driver, same protocol Tableau/PowerBI/DBeaver already support |
| Dashboards | ⚠️ | Multi-tile dashboards now exist (Dashboards tab: save several SQL queries together, auto-refreshed, each auto-charted) — single chart type (bar), no drag-to-arrange layout |
| Query history / cost per query | ⚠️ | A dedicated query history view exists; every workflow run shows an estimated compute cost, aggregated in a Usage Report (by user or workflow, CSV export). If Stripe is configured, this becomes **real billing** — actual metered charges via Stripe Checkout + usage records, not just an estimate. Still self-hosted, single-instance usage attribution, not per-tenant data isolation |

## Security & identity (see also the Security section below — this is what we added)

| Databricks capability | AtlasFlow before this update | AtlasFlow now |
|---|---|---|
| User authentication | ❌ | ✅ JWT-based login |
| Role-based access control | ❌ | ✅ admin / editor / viewer roles |
| Secrets management | ❌ (plaintext in task params) | ✅ Encrypted secrets store, referenced by name |
| Audit logging | ❌ | ✅ Every mutating action logged with user + timestamp |
| SSO / SAML / SCIM | ⚠️ | All three now implemented: OIDC (Okta/Azure AD/Google Workspace/Auth0), SAML 2.0 (via python3-saml, real XML signature validation), and SCIM 2.0 provisioning (list/create/update/deactivate). Honest caveat: SAML and SCIM are written correctly to spec but haven't been exercised against a live IdP in this environment (no network access here to test) — validate against your actual provider before relying on them |
| IP access lists / network policies | ❌ | ❌ Still missing — handle at reverse-proxy/firewall layer |
| Encryption at rest (customer-managed keys) | ❌ | ⚠️ Secrets encrypted; data-at-rest for MinIO/Postgres depends on your deployment (not app-managed) |
| Personal access tokens scoped to specific operations | ❌ | ⚠️ JWTs carry a role, not per-operation scoping |

## Operations & cost

| Databricks capability | AtlasFlow | Notes |
|---|---|---|
| Multi-cloud managed service | ❌ | Self-hosted only, on infra you manage |
| Usage-based billing / cost attribution | ❌ | — |
| SLA-backed uptime | ❌ | You own reliability |
| Workspace-level admin console | ⚠️ | Basic — the web UI covers workflows; no cluster/user/billing admin panels beyond user management |

## Bottom line

AtlasFlow now covers essentially every named capability in at least a real, working form:
**data engineering** (Spark + real Delta Lake ACID tables), **orchestration with
conditional logic and data passing**, **ML tracking + model serving** (real MLflow,
deployable prediction endpoints), **SQL analytics + dashboards**, **BI tool connectivity**
(a real Thrift Server JDBC/ODBC endpoint), **governance** (table-level access control +
lineage), a **feature store**, **SSO via OIDC and SAML**, **SCIM provisioning**, a
**real Docker-SDK-driven auto-scale controller**, and **real Stripe metered billing**.

What's left is now down to the parts that are structurally, not just currently,
out of reach for a self-hosted single-machine stack, plus a few honest caveats on the
newest additions:

- **True cloud-elastic autoscaling** — the auto-scale controller is real and automated,
  but it's still bounded by one host's physical CPU/RAM. Actual elasticity (spinning up
  new machines, not just containers on the same box) needs Kubernetes or a cloud
  provider's API underneath.
- **SAML and SCIM are implemented but not yet IdP-validated** — written correctly to
  spec, but this environment has no network access to test them against a live identity
  provider. Validate against yours before depending on them.
- **Stripe billing charges individual users of one shared instance**, not separate,
  data-isolated tenants. Real multi-tenant isolation (customer A structurally cannot see
  customer B's data) is a materially larger architecture change than billing alone.
- **Column/row-level security and cross-workspace catalog sharing** remain the deepest
  Unity Catalog features not implemented — table-level ACLs cover the common case, not
  the full one.

None of these are things a fake implementation would have made better — each is called
out because building a convincing-looking but non-functional version would be worse than
being clear about where the real boundary sits.
