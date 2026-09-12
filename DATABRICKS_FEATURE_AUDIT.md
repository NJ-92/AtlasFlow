# Databricks Feature Audit (systematic, by product pillar)

Organized by Databricks' actual current product structure (verified against their
release notes as of mid-2026), not by what AtlasFlow happens to already cover. Use this
to work through systematically and flag what matters to you — `GAP_ANALYSIS.md` has the
narrative version of what's already built; this is the full-breadth checklist.

Legend: ✅ built and real · ⚠️ partial/scoped-down version exists · ❌ not present · 🚫 structurally out of reach for self-hosted software (explained inline)

---

## 1. Compute

| Feature | Status | Note |
|---|---|---|
| Distributed Spark clusters | ✅ | Real Apache Spark, 1 master + N workers |
| Autoscaling clusters | ⚠️ | Real Docker-based controller, bounded by host hardware — see SECURITY.md §2c |
| Serverless compute (zero cluster management) | 🚫 | Needs a cloud provider's control plane; a self-hosted stack has no elastic infrastructure to provision from |
| Photon (vectorized query engine) | ❌ | Vanilla Spark only — Photon is closed-source, Databricks-proprietary |
| Cluster policies (cost/config governance) | ❌ | No concept of governed cluster templates |
| Container Services (custom Docker images per cluster) | ❌ | — |
| GPU / AI Runtime compute | ❌ | No GPU wiring |

## 2. Data Engineering — "Lakeflow"

| Feature | Status | Note |
|---|---|---|
| Jobs/Workflows (DAGs, schedules, retries) | ✅ | Core of AtlasFlow |
| Task values, conditional tasks, disabled tasks | ✅ | `run_if`, task values; disabling a task without deleting it is the one gap here |
| Lakeflow Pipelines (declarative, DLT-style) | ❌ | AtlasFlow workflows are imperative (you write the steps), not declarative-with-auto-orchestration |
| Lakeflow Designer (no-code pipeline builder) | ❌ | No visual pipeline builder |
| Lakeflow Connect (managed source connectors: Salesforce, Zendesk, Google Sheets, etc.) | ⚠️ | Generic JDBC (Postgres/MySQL) + REST API connectors exist; no managed SaaS-specific connectors |
| Auto Loader (incremental file ingestion) | ❌ | Ingestion wizard does one-time batch loads, not streaming/incremental |
| Unit testing for pipelines | ❌ | — |
| Zerobus (low-latency streaming ingest) | ❌ | — |

## 3. Governance — Unity Catalog

| Feature | Status | Note |
|---|---|---|
| Table catalog (browsable, named) | ✅ | Data tab |
| Volumes (arbitrary file storage) | ✅ | Volumes tab |
| Table-level access control | ✅ | Visibility + grants |
| Row/column-level security, ABAC | ❌ | Table-level only |
| Lineage | ⚠️ | Manually tagged, not auto-inferred from SQL/Spark |
| Data quality monitoring (anomaly detection) | ❌ | — |
| Clean rooms (multi-party data collaboration without sharing raw data) | ❌ | — |
| Delta Sharing (cross-org table sharing) | ❌ | — |
| Catalog commits / multi-table transactions | ❌ | — |
| Governed tags system | ❌ | — |
| Cross-workspace metastore | 🚫 | AtlasFlow is single-instance by design |

## 4. SQL Analytics — Databricks SQL

| Feature | Status | Note |
|---|---|---|
| SQL editor, run queries | ✅ | SQL tab |
| Named query parameters | ✅ | `:paramName` auto-detected inputs |
| Query history | ✅ | — |
| Dashboards | ⚠️ | Multi-tile, auto-refresh; single chart type, no drag layout |
| Alerts | ✅ | Threshold alerting, scheduled or manual |
| SQL warehouses (serverless SQL compute) | ⚠️ | Runs on the same Spark cluster, not a separate elastic SQL-only compute tier |
| BI tool connectivity (JDBC/ODBC) | ✅ | Real Spark Thrift Server |
| Cost attribution tags on queries | ⚠️ | Cost estimation exists per-run, not per-query |
| Genie spaces (per-table NL→SQL with example questions) | ✅ | Genie tab |

## 5. AI/ML — Mosaic AI

| Feature | Status | Note |
|---|---|---|
| MLflow experiment tracking | ✅ | Real MLflow server |
| Model registry | ✅ | — |
| Model serving | ⚠️ | Real `mlflow models serve`, single-instance |
| Feature Store | ⚠️ | Tagging + point-in-time joins; no online/low-latency serving |
| AI Playground (interactive LLM testing UI) | ❌ | — |
| Agent Framework / Agents (build custom AI agents) | ❌ | Genie is a fixed NL→SQL agent, not a general agent builder |
| Supervisor Agent (multi-agent orchestration) | ❌ | — |
| Knowledge Assistant (RAG over documents) | ❌ | — |
| AI Gateway (unified LLM proxy: rate limits, logging, multi-provider routing) | ❌ | Genie calls Anthropic directly, no gateway abstraction |
| Foundation Model APIs (pay-per-token hosted models) | 🚫 | Requires hosting/licensing foundation models — not something self-hosted software can add without a model-hosting business behind it |
| MLflow 3 GenAI evaluation, tracing (OTel format) | ❌ | — |

## 6. Lakebase (Postgres-compatible OLTP database)

| Feature | Status | Note |
|---|---|---|
| Managed transactional Postgres tied into the lakehouse | ❌ | Entirely new product pillar Databricks added — AtlasFlow uses Postgres only for its own metadata, not as an offered OLTP service |

## 7. Development tools

| Feature | Status | Note |
|---|---|---|
| Notebooks | ✅ | Jupyter, wired to Spark |
| Notebook parameters (set before run) | ✅ | Quick-schedule form |
| Live in-notebook widgets | ❌ | Scoped-down version only (form, not reactive live widgets) |
| Multi-language cells in one notebook | ⚠️ | Depends on kernel/extensions, not configured out of the box |
| Git folders / Repos (version control UI) | ❌ | No in-app Git integration |
| Web terminal | ❌ | — |
| Databricks Apps (host custom web apps in-workspace) | ❌ | — |
| Inline notebook-cell comments | ⚠️ | Scoped to run-level comments instead (see SECURITY-adjacent note in README) |

## 8. Platform / Admin

| Feature | Status | Note |
|---|---|---|
| Authentication (local) | ✅ | — |
| OIDC SSO | ✅ | — |
| SAML SSO | ⚠️ | Implemented, not IdP-validated in this environment |
| SCIM provisioning | ⚠️ | Implemented, not IdP-validated |
| RBAC | ✅ | viewer/editor/admin |
| Audit logging | ✅ | — |
| Personal access token scoping (per-API-operation) | ❌ | Tokens carry a role, not fine-grained scopes |
| Compliance certifications (HITRUST, ISMAP, FedRAMP, etc.) | 🚫 | Requires third-party audits of an actual hosted service — not applicable to self-hosted software you run yourself |
| Multi-workspace account console | 🚫 | Single-instance by design |
| Budget policies / serverless cost controls | ⚠️ | Cost estimation + Stripe billing exist; no policy-based spend caps |
| IP access lists / network policies | ❌ | Handle at your firewall/reverse-proxy layer instead |

## 9. Marketplace & Sharing

| Feature | Status | Note |
|---|---|---|
| Databricks Marketplace (buy/sell datasets, models) | 🚫 | Requires a multi-tenant commercial marketplace operator — structurally not something one self-hosted instance can be |
| Delta Sharing (open cross-org sharing protocol) | ❌ | Could theoretically be added (it's an open protocol) — not built |

---

## How to use this

For each ❌ or ⚠️ row that matters to you, tell me the row (or a short list) and I'll scope
and build it the same way as everything so far — real integrations where possible, clearly
labeled honest limits where not. Rows marked 🚫 are ones I won't build convincing-looking
fake versions of, for the reasons stated inline — if one of those is actually essential to
your use case, it's worth a conversation about whether AtlasFlow (self-hosted, single
instance) is the right foundation for that specific need, rather than something to patch
around.
