from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"  # viewer | editor | admin


class UserOut(BaseModel):
    id: str
    username: str
    role: str
    is_active: bool

    class Config:
        from_attributes = True


class LoginRequest(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class SecretCreate(BaseModel):
    name: str
    value: str


class SecretOut(BaseModel):
    id: str
    name: str
    created_at: str

    class Config:
        from_attributes = True


class AuditLogOut(BaseModel):
    id: str
    username: Optional[str]
    action: str
    resource: Optional[str]
    details: Dict[str, Any]
    timestamp: str

    class Config:
        from_attributes = True


class TableOut(BaseModel):
    id: str
    name: str
    path: str
    description: str
    row_count: Optional[int]
    columns: List[Dict[str, Any]] = Field(default_factory=list)
    visibility: str = "public"
    source_tables: List[str] = Field(default_factory=list)
    origin: str = "upload"
    is_feature_table: bool = False
    entity_key: Optional[str] = None
    timestamp_key: Optional[str] = None
    created_at: str

    class Config:
        from_attributes = True


class TableRegister(BaseModel):
    name: str
    path: str
    description: str = ""
    visibility: str = "public"
    source_tables: List[str] = Field(default_factory=list)
    is_feature_table: bool = False
    entity_key: Optional[str] = None
    timestamp_key: Optional[str] = None


class VolumeCreate(BaseModel):
    name: str
    description: str = ""
    visibility: str = "public"


class VolumeOut(BaseModel):
    id: str
    name: str
    description: str
    visibility: str
    created_at: str

    class Config:
        from_attributes = True


class VolumeEntry(BaseModel):
    name: str
    is_dir: bool
    size: Optional[int] = None
    last_modified: Optional[str] = None


class AlertCreate(BaseModel):
    name: str
    sql: str
    value_column: str
    operator: str  # >, <, >=, <=, ==, !=
    threshold: str
    schedule_cron: Optional[str] = None
    notify_webhook: Optional[str] = None


class AlertOut(BaseModel):
    id: str
    name: str
    sql: str
    value_column: str
    operator: str
    threshold: str
    schedule_cron: Optional[str]
    notify_webhook: Optional[str]
    is_paused: bool
    last_status: str
    last_value: Optional[str]
    last_checked_at: Optional[str]

    class Config:
        from_attributes = True


class AlertHistoryOut(BaseModel):
    id: str
    status: str
    value: Optional[str]
    message: str
    checked_at: str

    class Config:
        from_attributes = True


class GenieAskRequest(BaseModel):
    question: str


class GenieAskResponse(BaseModel):
    question: str
    sql: Optional[str] = None
    success: bool
    error: Optional[str] = None
    columns: List[str] = Field(default_factory=list)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0


class GenieHistoryOut(BaseModel):
    id: str
    question: str
    generated_sql: Optional[str]
    success: bool
    row_count: Optional[int]
    created_at: str


class NotebookFile(BaseModel):
    name: str
    path: str


class NotebookQuickSchedule(BaseModel):
    notebook_path: str
    params: Dict[str, Any] = Field(default_factory=dict)
    schedule_cron: Optional[str] = None
    workflow_name: Optional[str] = None


class CommentCreate(BaseModel):
    text: str


class CommentOut(BaseModel):
    id: str
    username: Optional[str]
    text: str
    created_at: str

    class Config:
        from_attributes = True


class IngestionSourceCreate(BaseModel):
    name: str
    source_type: str  # "postgres" | "mysql" | "rest_api"
    # JDBC fields
    host: Optional[str] = None
    port: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    # REST API fields
    url: Optional[str] = None
    method: str = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    auth_token: Optional[str] = None
    response_path: Optional[str] = None  # dotted path to the array of records in the JSON response


class IngestionSourceOut(BaseModel):
    id: str
    name: str
    source_type: str
    created_at: str


class IngestionPreviewRequest(BaseModel):
    query: Optional[str] = None  # JDBC: table name or (SELECT ...) subquery


class IngestionRunRequest(BaseModel):
    query: Optional[str] = None  # JDBC: table name or subquery
    table_name: str
    visibility: str = "public"


class LakebaseColumn(BaseModel):
    name: str
    type: str  # text | integer | bigint | boolean | "double precision" | timestamp | date | numeric


class LakebaseTableCreate(BaseModel):
    name: str
    columns: List[LakebaseColumn]
    description: str = ""


class LakebaseTableOut(BaseModel):
    id: str
    name: str
    columns: List[Dict[str, Any]]
    description: str
    row_count: int = 0
    synced_table_name: Optional[str] = None
    last_synced_at: Optional[str] = None
    created_at: str


class LakebaseRowInsert(BaseModel):
    values: Dict[str, Any]


class ShareCreate(BaseModel):
    name: str
    description: str = ""


class ShareOut(BaseModel):
    id: str
    name: str
    description: str
    table_count: int = 0
    recipient_count: int = 0
    created_at: str


class ShareAddTable(BaseModel):
    table_name: str
    shared_name: Optional[str] = None


class RecipientCreate(BaseModel):
    name: str


class RecipientOut(BaseModel):
    id: str
    name: str
    bearer_token: Optional[str] = None  # only populated once, at creation
    created_at: str


class AIGatewayRouteCreate(BaseModel):
    name: str
    provider: str  # anthropic | openai
    model: str
    api_key_secret_name: str
    requests_per_minute: int = 30


class AIGatewayRouteOut(BaseModel):
    id: str
    name: str
    provider: str
    model: str
    api_key_secret_name: str
    requests_per_minute: int
    is_enabled: bool
    created_at: str


class AIGatewayChatRequest(BaseModel):
    route_name: str
    messages: List[Dict[str, str]]
    system: Optional[str] = None


class AIGatewayChatResponse(BaseModel):
    content: str


class AIGatewayLogOut(BaseModel):
    id: str
    route_id: str
    username: Optional[str]
    success: bool
    latency_ms: Optional[int]
    error: Optional[str]
    created_at: str


class RowFilterSet(BaseModel):
    role: str  # viewer | editor | admin
    filter_expr: Optional[str] = None  # None/empty removes the filter for that role


class ColumnMaskSet(BaseModel):
    column: str
    unmask_role: str  # viewer | editor | admin


class ColumnMaskRemove(BaseModel):
    column: str


class GitRepoCreate(BaseModel):
    name: str
    remote_url: str
    branch: str = "main"
    pat_secret_name: Optional[str] = None


class GitRepoOut(BaseModel):
    id: str
    name: str
    remote_url: str
    branch: str
    last_synced_at: Optional[str]
    last_sync_status: str
    last_sync_error: Optional[str]
    created_at: str


class GitCommitRequest(BaseModel):
    message: str


class PATCreate(BaseModel):
    name: str
    scopes: List[str]
    expires_in_days: Optional[int] = None


class PATOut(BaseModel):
    id: str
    name: str
    scopes: List[str]
    token: Optional[str] = None  # only populated once, at creation
    expires_at: Optional[str]
    last_used_at: Optional[str]
    created_at: str


class GrantCreate(BaseModel):
    username: str


class DashboardTileIn(BaseModel):
    title: str
    sql: str
    chart_type: str = "bar"


class DashboardCreate(BaseModel):
    name: str
    tiles: List[DashboardTileIn] = Field(default_factory=list)


class DashboardOut(BaseModel):
    id: str
    name: str
    tiles: List[Dict[str, Any]]
    created_at: str


class ModelDeployRequest(BaseModel):
    model_name: str
    model_version: str = "latest"


class ModelDeploymentOut(BaseModel):
    id: str
    model_name: str
    model_version: str
    port: int
    status: str
    created_at: str

    class Config:
        from_attributes = True


class BillingConfigOut(BaseModel):
    stripe_enabled: bool
    scim_enabled: bool


class CheckoutOut(BaseModel):
    url: str


class UsageReportRow(BaseModel):
    group: str
    total_cost_usd: float
    run_count: int


class PythonRunRequest(BaseModel):
    code: str


class PythonRunResult(BaseModel):
    success: bool
    logs: str
    output: Dict[str, Any] = Field(default_factory=dict)


class SqlQueryRequest(BaseModel):
    sql: str
    limit: int = 200
    params: Dict[str, str] = Field(default_factory=dict)


class SqlQueryResult(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    logs: str


class TaskIn(BaseModel):
    key: str
    type: str  # python | shell | spark_submit | sql | notebook
    command: str
    params: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)
    run_if: Optional[str] = None  # python expression; task is skipped if it evaluates falsy
    retries: int = 0
    timeout_seconds: int = 3600


class ClusterPolicyCreate(BaseModel):
    name: str
    description: str = ""
    max_timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    allowed_task_types: Optional[List[str]] = None


class ClusterPolicyOut(BaseModel):
    id: str
    name: str
    description: str
    max_timeout_seconds: Optional[int]
    max_retries: Optional[int]
    allowed_task_types: Optional[List[str]]
    created_at: str


class PipelineTableIn(BaseModel):
    table_name: str
    sql: str


class PipelineCreate(BaseModel):
    name: str
    description: str = ""
    schedule_cron: Optional[str] = None
    tables: List[PipelineTableIn]


class PipelineTableOut(BaseModel):
    id: str
    table_name: str
    sql: str
    last_status: str
    last_error: Optional[str]
    row_count: Optional[int]


class PipelineOut(BaseModel):
    id: str
    name: str
    description: str
    schedule_cron: Optional[str]
    is_paused: bool
    last_run_status: str
    last_run_at: Optional[str]
    tables: List[PipelineTableOut] = Field(default_factory=list)


class WorkflowCreate(BaseModel):
    name: str
    description: str = ""
    schedule_cron: Optional[str] = None
    on_failure_webhook: Optional[str] = None
    cluster_policy_name: Optional[str] = None
    tasks: List[TaskIn]


class WorkflowOut(BaseModel):
    id: str
    name: str
    description: str
    schedule_cron: Optional[str]
    is_paused: bool
    tasks: List[TaskIn]

    class Config:
        from_attributes = True


class TaskRunOut(BaseModel):
    id: str
    task_key: str
    status: str
    attempt: int
    started_at: Optional[str]
    finished_at: Optional[str]
    logs: str
    output: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        from_attributes = True


class WorkflowRunOut(BaseModel):
    id: str
    workflow_id: str
    status: str
    triggered_by: str
    started_at: Optional[str]
    finished_at: Optional[str]
    task_runs: List[TaskRunOut] = []

    class Config:
        from_attributes = True
