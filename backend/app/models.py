import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Text, DateTime, ForeignKey, Enum, Integer, JSON, Boolean
)
from sqlalchemy.orm import relationship

from .database import Base


def gen_id():
    return str(uuid.uuid4())


class RunStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"              # run_if evaluated falsy
    UPSTREAM_FAILED = "upstream_failed"


class TaskType(str, enum.Enum):
    PYTHON = "python"          # arbitrary python code, executed in-process
    SHELL = "shell"            # shell command
    SPARK_SUBMIT = "spark_submit"  # spark-submit a job against the cluster
    SQL = "sql"                 # ad-hoc Spark SQL against Delta tables in the lakehouse
    NOTEBOOK = "notebook"      # run a .ipynb via papermill


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=gen_id)
    username = Column(String, nullable=False, unique=True)
    hashed_password = Column(String, nullable=True)  # nullable: SSO-only users have no local password
    role = Column(String, nullable=False, default="viewer")  # viewer | editor | admin
    auth_provider = Column(String, default="local")  # "local" | "oidc" | "saml" | "scim"
    external_id = Column(String, nullable=True, unique=True)  # SCIM externalId, for idempotent IdP-driven updates
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # --- billing: Stripe metered billing (see app/billing.py) ---
    stripe_customer_id = Column(String, nullable=True)
    stripe_subscription_item_id = Column(String, nullable=True)  # the metered item usage is reported against
    billing_status = Column(String, default="none")  # "none" | "pending" | "active" | "canceled"


class Secret(Base):
    """Encrypted credential store. Values are never returned by the API once written;
    tasks reference a secret by name (e.g. `{{secret:snowflake_password}}` in a shell
    command or python param) and the executor resolves + decrypts it just before running,
    then redacts it from persisted logs."""
    __tablename__ = "secrets"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    encrypted_value = Column(Text, nullable=False)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, default=gen_id)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)
    username = Column(String, nullable=True)  # denormalized so logs survive user deletion
    action = Column(String, nullable=False)   # e.g. "workflow.create", "workflow.trigger"
    resource = Column(String, nullable=True)  # e.g. workflow id or name
    details = Column(JSON, default=dict)
    ip_address = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)


class Table(Base):
    """A lightweight catalog entry: maps a friendly name to a Delta table's storage path,
    so SQL/Python can reference `sales_data` instead of a raw delta path. This is
    AtlasFlow's stand-in for a Unity Catalog-style metastore - much simpler (no
    cross-workspace sharing), but it covers table-level access control, row/column-level
    security, basic lineage, and feature-table tagging."""
    __tablename__ = "tables"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    path = Column(String, nullable=False)  # storage path to the Delta table
    description = Column(Text, default="")
    row_count = Column(Integer, nullable=True)
    columns = Column(JSON, default=list)  # [{"name": ..., "type": ...}, ...]
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # --- governance: table-level access control ---
    visibility = Column(String, default="public")  # "public" (any viewer+) or "restricted" (owner/admin/grants only)

    # --- governance: row/column-level security, applied via query rewriting (see app/catalog.py) ---
    row_filters = Column(JSON, default=dict)      # {"viewer": "region = 'public'", ...} - a SQL boolean expr per role
    masked_columns = Column(JSON, default=list)   # [{"column": "ssn", "unmask_role": "admin"}, ...]

    # --- lineage: what this table was derived from, and how it came to exist ---
    source_tables = Column(JSON, default=list)  # names of tables this one was derived from
    origin = Column(String, default="upload")   # "upload" | "registered" | "workflow"

    # --- feature store: tag a table as a feature table for point-in-time joins ---
    is_feature_table = Column(Boolean, default=False)
    entity_key = Column(String, nullable=True)      # join key column, e.g. "customer_id"
    timestamp_key = Column(String, nullable=True)   # event-time column, for point-in-time correctness


class TableGrant(Base):
    """Explicit read access to a 'restricted' table for a specific user."""
    __tablename__ = "table_grants"

    id = Column(String, primary_key=True, default=gen_id)
    table_id = Column(String, ForeignKey("tables.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    granted_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Volume(Base):
    """Browsable file storage for arbitrary files (not just tabular data) - AtlasFlow's
    equivalent of a Unity Catalog Volume. Backed by MinIO; each volume is a prefix within
    a shared 'atlasflow-volumes' bucket, browsed like a folder tree."""
    __tablename__ = "volumes"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    visibility = Column(String, default="public")  # "public" or "restricted", same model as Table
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class VolumeGrant(Base):
    """Explicit access to a 'restricted' volume for a specific user - mirrors TableGrant."""
    __tablename__ = "volume_grants"

    id = Column(String, primary_key=True, default=gen_id)
    volume_id = Column(String, ForeignKey("volumes.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    granted_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Dashboard(Base):
    __tablename__ = "dashboards"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class DashboardTile(Base):
    __tablename__ = "dashboard_tiles"

    id = Column(String, primary_key=True, default=gen_id)
    dashboard_id = Column(String, ForeignKey("dashboards.id"), nullable=False)
    title = Column(String, nullable=False)
    sql = Column(Text, nullable=False)
    chart_type = Column(String, default="bar")  # "bar" | "table"
    position = Column(Integer, default=0)


class ModelDeployment(Base):
    """A locally-served MLflow registered model - real serving via `mlflow models serve`,
    proxied through the API. Single-instance (no replicas/rolling updates), which is the
    honest limit of doing this without a container orchestrator."""
    __tablename__ = "model_deployments"

    id = Column(String, primary_key=True, default=gen_id)
    model_name = Column(String, nullable=False, unique=True)
    model_version = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    pid = Column(Integer, nullable=True)
    status = Column(String, default="starting")  # starting | running | stopped | failed
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Alert(Base):
    """A saved SQL query, evaluated on a schedule, that notifies when its result crosses
    a threshold - mirrors Databricks SQL Alerts. Evaluation reuses the same Spark SQL
    engine as the SQL tab and the `sql` task type."""
    __tablename__ = "alerts"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    sql = Column(Text, nullable=False)
    # the alert watches this column's value in the query's first result row
    value_column = Column(String, nullable=False)
    operator = Column(String, nullable=False)  # ">" | "<" | ">=" | "<=" | "==" | "!="
    threshold = Column(String, nullable=False)  # stored as string, compared numerically at eval time
    schedule_cron = Column(String, nullable=True)  # None = manual/on-demand only
    notify_webhook = Column(String, nullable=True)
    is_paused = Column(Boolean, default=False)
    last_status = Column(String, default="unknown")  # "ok" | "triggered" | "error" | "unknown"
    last_value = Column(String, nullable=True)
    last_checked_at = Column(DateTime, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class AlertHistory(Base):
    __tablename__ = "alert_history"

    id = Column(String, primary_key=True, default=gen_id)
    alert_id = Column(String, ForeignKey("alerts.id"), nullable=False)
    status = Column(String, nullable=False)  # "ok" | "triggered" | "error"
    value = Column(String, nullable=True)
    message = Column(Text, default="")
    checked_at = Column(DateTime, default=datetime.utcnow)


class GenieQuery(Base):
    """Log of natural-language questions asked to Genie, and the SQL it generated -
    lets users see (and reuse) past questions, and gives a paper trail for auditing what
    an LLM was allowed to generate against your data."""
    __tablename__ = "genie_queries"

    id = Column(String, primary_key=True, default=gen_id)
    question = Column(Text, nullable=False)
    generated_sql = Column(Text, nullable=True)
    success = Column(Boolean, default=False)
    error = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ClusterPolicy(Base):
    """A governed template constraining what a workflow's tasks are allowed to do -
    AtlasFlow's equivalent of Databricks Cluster Policies. Since AtlasFlow runs tasks
    against one shared Spark cluster rather than provisioning per-job VM clusters, this
    governs task-level limits (timeout, retries, allowed task types) instead of instance
    types/node counts - the same cost/security-governance intent, adapted to the
    architecture."""
    __tablename__ = "cluster_policies"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    max_timeout_seconds = Column(Integer, nullable=True)   # None = no limit
    max_retries = Column(Integer, nullable=True)
    allowed_task_types = Column(JSON, nullable=True)  # None = all types allowed; else a list like ["sql", "python"]
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Workflow(Base):
    __tablename__ = "workflows"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    schedule_cron = Column(String, nullable=True)  # None = manual trigger only
    is_paused = Column(Boolean, default=False)
    on_failure_webhook = Column(String, nullable=True)  # POSTed a JSON summary if a run fails
    cluster_policy_id = Column(String, ForeignKey("cluster_policies.id"), nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)  # billing attribution fallback for scheduled runs
    created_at = Column(DateTime, default=datetime.utcnow)

    tasks = relationship("Task", back_populates="workflow", cascade="all, delete-orphan")
    runs = relationship("WorkflowRun", back_populates="workflow", cascade="all, delete-orphan")


class UsageRecord(Base):
    """One row per workflow run's billing attribution - lets us report usage to Stripe
    exactly once per run (idempotency) and lets the usage-report endpoint aggregate
    without recomputing from scratch each time."""
    __tablename__ = "usage_records"

    id = Column(String, primary_key=True, default=gen_id)
    workflow_run_id = Column(String, ForeignKey("workflow_runs.id"), nullable=False, unique=True)
    workflow_id = Column(String, ForeignKey("workflows.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)  # who the cost is attributed to
    cost_usd = Column(String, nullable=False)  # stored as string to avoid float rounding drift
    reported_to_stripe = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, default=gen_id)
    workflow_id = Column(String, ForeignKey("workflows.id"), nullable=False)
    key = Column(String, nullable=False)  # unique short name within the workflow, used in depends_on
    type = Column(Enum(TaskType), nullable=False)
    command = Column(Text, nullable=False)  # python code / shell cmd / script path / notebook path
    params = Column(JSON, default=dict)      # extra args, e.g. spark-submit flags, notebook params
    depends_on = Column(JSON, default=list)  # list of task `key`s this task waits on
    run_if = Column(Text, nullable=True)  # optional python expression; task is skipped if it evaluates falsy
    retries = Column(Integer, default=0)
    timeout_seconds = Column(Integer, default=3600)

    workflow = relationship("Workflow", back_populates="tasks")


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id = Column(String, primary_key=True, default=gen_id)
    workflow_id = Column(String, ForeignKey("workflows.id"), nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.PENDING)
    triggered_by = Column(String, default="manual")  # "manual" or "schedule"
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    workflow = relationship("Workflow", back_populates="runs")
    task_runs = relationship("TaskRun", back_populates="workflow_run", cascade="all, delete-orphan")


class TaskRun(Base):
    __tablename__ = "task_runs"

    id = Column(String, primary_key=True, default=gen_id)
    workflow_run_id = Column(String, ForeignKey("workflow_runs.id"), nullable=False)
    task_key = Column(String, nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.PENDING)
    attempt = Column(Integer, default=0)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    logs = Column(Text, default="")
    output = Column(JSON, default=dict)  # "task values" this task passed downstream

    workflow_run = relationship("WorkflowRun", back_populates="task_runs")


class RunComment(Base):
    """A discussion thread attached to a workflow run - AtlasFlow's scoped equivalent of
    Databricks' inline notebook-cell comments. Not the same feature (this is per-run, not
    per-cell, since notebooks here are edited in plain Jupyter, outside AtlasFlow's own
    UI), but it gives the same collaborative "discuss this specific execution" value for
    incident review and handoffs."""
    __tablename__ = "run_comments"

    id = Column(String, primary_key=True, default=gen_id)
    workflow_run_id = Column(String, ForeignKey("workflow_runs.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)
    username = Column(String, nullable=True)  # denormalized so comments survive user deletion
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class IngestionSource(Base):
    """A saved external data source connection for the Data Ingestion wizard - a JDBC
    database (Postgres/MySQL) or a REST API. Connection secrets (passwords, tokens) are
    encrypted at rest with the same Fernet key used for the Secrets store."""
    __tablename__ = "ingestion_sources"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    source_type = Column(String, nullable=False)  # "postgres" | "mysql" | "rest_api"
    config_encrypted = Column(Text, nullable=False)  # JSON blob: host/port/db/user/password, or url/headers/token
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class LakebaseTable(Base):
    """Catalog entry for a real Postgres-backed transactional table - AtlasFlow's
    equivalent of Lakebase. The actual table lives in a dedicated `lakebase` schema on
    the same Postgres instance (see app/lakebase.py), giving genuine row-level
    insert/update/delete OLTP semantics, unlike Delta tables which are batch-oriented.
    `last_synced_at` tracks the last full-refresh sync into a Delta table for analytics -
    this is periodic full-refresh, not true log-based CDC streaming."""
    __tablename__ = "lakebase_tables"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    columns = Column(JSON, default=list)  # [{"name": ..., "type": ...}, ...]
    description = Column(Text, default="")
    synced_table_name = Column(String, nullable=True)  # name of the Delta table this syncs to, once synced
    last_synced_at = Column(DateTime, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Share(Base):
    """A Delta Share: a named collection of catalog tables exposed to external
    recipients via the real, open Delta Sharing REST protocol
    (github.com/delta-io/delta-sharing) - any standard Delta Sharing client (the
    delta-sharing Python library, Spark connector, Power BI connector) can read from
    this, not just AtlasFlow itself."""
    __tablename__ = "shares"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ShareTable(Base):
    """A catalog Table included in a Share, exposed under a share-local name (defaults
    to the catalog table's own name)."""
    __tablename__ = "share_tables"

    id = Column(String, primary_key=True, default=gen_id)
    share_id = Column(String, ForeignKey("shares.id"), nullable=False)
    table_id = Column(String, ForeignKey("tables.id"), nullable=False)
    shared_name = Column(String, nullable=False)


class ShareRecipient(Base):
    """An external recipient of a Share, authenticated via bearer token per the Delta
    Sharing protocol - `bearer_token` is handed out once as part of a downloadable
    'profile file' the recipient uses with their own Delta Sharing client."""
    __tablename__ = "share_recipients"

    id = Column(String, primary_key=True, default=gen_id)
    share_id = Column(String, ForeignKey("shares.id"), nullable=False)
    name = Column(String, nullable=False)
    bearer_token = Column(String, nullable=False, unique=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Pipeline(Base):
    """A declarative pipeline - AtlasFlow's equivalent of Lakeflow Declarative Pipelines
    (formerly Delta Live Tables). Unlike a Workflow (where you manually wire up
    `depends_on` between imperative tasks), a Pipeline is a set of declared tables, each
    defined by a single SQL statement (`CREATE OR REFRESH ... AS SELECT ...` style); the
    engine parses each statement to find which other declared tables it reads from and
    infers execution order automatically - you declare *what* each table is, not the
    order to build them in."""
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    schedule_cron = Column(String, nullable=True)
    is_paused = Column(Boolean, default=False)
    last_run_status = Column(String, default="unknown")
    last_run_at = Column(DateTime, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class PipelineTable(Base):
    """One declared table within a Pipeline. `sql` is a SELECT statement (no explicit
    dependency wiring needed - referenced table names are auto-detected)."""
    __tablename__ = "pipeline_tables"

    id = Column(String, primary_key=True, default=gen_id)
    pipeline_id = Column(String, ForeignKey("pipelines.id"), nullable=False)
    table_name = Column(String, nullable=False)  # local name within the pipeline; also the resulting catalog table name
    sql = Column(Text, nullable=False)
    last_status = Column(String, default="unknown")  # "ok" | "error" | "unknown"
    last_error = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)


class AIGatewayRoute(Base):
    """A configured LLM endpoint - AtlasFlow's equivalent of a Databricks AI Gateway
    route. Provides a single place to configure provider/model/rate-limit for LLM calls,
    used by the Playground and available for any future integration (Genie currently
    calls Anthropic directly rather than through a route, to keep it a standalone,
    dependency-free feature). API keys are referenced by name from the existing Secrets
    store, not duplicated here."""
    __tablename__ = "ai_gateway_routes"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)
    provider = Column(String, nullable=False)  # "anthropic" | "openai"
    model = Column(String, nullable=False)
    api_key_secret_name = Column(String, nullable=False)  # references a Secret by name
    requests_per_minute = Column(Integer, default=30)
    is_enabled = Column(Boolean, default=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class AIGatewayLog(Base):
    """One call through the gateway - real usage logging, per Databricks AI Gateway's
    governance intent (see who's calling what, how often)."""
    __tablename__ = "ai_gateway_logs"

    id = Column(String, primary_key=True, default=gen_id)
    route_id = Column(String, ForeignKey("ai_gateway_routes.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)
    username = Column(String, nullable=True)
    success = Column(Boolean, default=False)
    latency_ms = Column(Integer, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class GitRepo(Base):
    """A Git repository cloned into the shared notebooks folder - AtlasFlow's equivalent
    of Databricks Repos/Git folders. Real `git` operations (clone/pull/commit/push) via
    subprocess, not a reimplementation. For private repos, `pat_secret_name` references a
    Secret holding a personal access token, embedded into the HTTPS remote URL for auth."""
    __tablename__ = "git_repos"

    id = Column(String, primary_key=True, default=gen_id)
    name = Column(String, nullable=False, unique=True)  # also the local directory name under /notebooks
    remote_url = Column(String, nullable=False)
    branch = Column(String, default="main")
    pat_secret_name = Column(String, nullable=True)  # optional - references a Secret for private repos
    last_synced_at = Column(DateTime, nullable=True)
    last_sync_status = Column(String, default="unknown")  # "ok" | "error" | "unknown"
    last_sync_error = Column(Text, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class PersonalAccessToken(Base):
    """A scoped API token - AtlasFlow's equivalent of Databricks fine-grained PATs. Unlike
    a login session (JWT), a PAT is restricted to specific operations (see app/scopes.py)
    on top of - never beyond - the owning user's role; it narrows access, it never grants
    more than the user already has. Only the hash is stored; the plaintext token is shown
    once, at creation."""
    __tablename__ = "personal_access_tokens"

    id = Column(String, primary_key=True, default=gen_id)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=False)
    token_hash = Column(String, nullable=False, unique=True)
    scopes = Column(JSON, default=list)  # list of scope names from app/scopes.SCOPE_CATALOG
    expires_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
