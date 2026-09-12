import os
import re
import json
import threading
import uuid
import shutil
import urllib.parse

from fastapi import FastAPI, HTTPException, Depends, Request, UploadFile, File, Form
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from croniter import croniter
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from . import models, schemas, scheduler, security, scim
from .database import engine, get_db, Base, SessionLocal
from .executor import execute_workflow_run

Base.metadata.create_all(bind=engine)

app = FastAPI(title="AtlasFlow Orchestrator API")

# --- rate limiting (protects auth endpoints from brute-force credential guessing) ---
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- CORS: restricted to an explicit allowlist, not "*" ---
# Set ATLASFLOW_CORS_ORIGINS to a comma-separated list in production, e.g.
#   ATLASFLOW_CORS_ORIGINS=https://atlasflow.yourcompany.com
_cors_origins = os.getenv("ATLASFLOW_CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(scim.router)


@app.middleware("http")
async def enforce_pat_scopes(request: Request, call_next):
    """A PAT can only ever narrow what its owning user could already do - this checks the
    raw method+path against the token's allowed scopes *in addition to* the normal
    require_role() check the route itself performs. JWT session tokens are unaffected
    (full role-based access, as before) - scoping is opt-in via issuing a PAT."""
    from fastapi.responses import JSONResponse
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer ") and auth[len("Bearer "):].startswith(security.PAT_PREFIX):
        token = auth[len("Bearer "):]
        from . import scopes
        db = SessionLocal()
        try:
            pat = db.query(models.PersonalAccessToken).filter_by(token_hash=security.hash_pat(token)).first()
            if not pat or (pat.expires_at and pat.expires_at < datetime.utcnow()):
                return JSONResponse({"detail": "Invalid or expired token"}, status_code=401)
            if not scopes.scope_allows(pat.scopes, request.method, request.url.path):
                return JSONResponse({"detail": f"Token scope does not permit {request.method} {request.url.path}"}, status_code=403)
        finally:
            db.close()
    return await call_next(request)


@app.on_event("startup")
def on_startup():
    scheduler.start()
    _seed_default_admin()
    from . import autoscaler
    autoscaler.start_if_enabled()
    from . import storage
    storage.ensure_bucket()


def _seed_default_admin():
    """Creates a default admin user on first boot so there's a way in. GETTING_STARTED.md
    instructs changing this password immediately - see also the warning logged below."""
    db = SessionLocal()
    try:
        if db.query(models.User).count() == 0:
            default_password = os.getenv("ATLASFLOW_ADMIN_BOOTSTRAP_PASSWORD", "atlasflow-admin")
            admin = models.User(
                username="admin",
                hashed_password=security.hash_password(default_password),
                role="admin",
            )
            db.add(admin)
            db.commit()
            print(
                "\n[AtlasFlow] Created default admin user 'admin'. "
                "Log in and change the password immediately, or set "
                "ATLASFLOW_ADMIN_BOOTSTRAP_PASSWORD before first boot.\n"
            )
    finally:
        db.close()


def _audit(db: Session, user, action: str, resource: str = None, details: dict = None, request: Request = None):
    db.add(models.AuditLog(
        user_id=user.id if user else None,
        username=user.username if user else None,
        action=action,
        resource=resource,
        details=details or {},
        ip_address=request.client.host if request and request.client else None,
    ))
    db.commit()


# --- serializers -------------------------------------------------------------

def _serialize_task(t: models.Task) -> dict:
    return {
        "key": t.key, "type": t.type.value, "command": t.command, "params": t.params or {},
        "depends_on": t.depends_on or [], "run_if": t.run_if,
        "retries": t.retries, "timeout_seconds": t.timeout_seconds,
    }


def _serialize_workflow(w: models.Workflow, db: Session = None) -> dict:
    last_run = None
    policy_name = None
    if db is not None:
        last_run = (
            db.query(models.WorkflowRun).filter_by(workflow_id=w.id)
            .order_by(models.WorkflowRun.started_at.desc()).first()
        )
        if w.cluster_policy_id:
            policy = db.query(models.ClusterPolicy).get(w.cluster_policy_id)
            policy_name = policy.name if policy else None
    next_run_at = None
    if w.schedule_cron and not w.is_paused:
        try:
            next_run_at = croniter(w.schedule_cron, datetime.utcnow()).get_next(datetime).isoformat()
        except Exception:
            next_run_at = None
    return {
        "id": w.id, "name": w.name, "description": w.description, "schedule_cron": w.schedule_cron,
        "is_paused": w.is_paused, "on_failure_webhook": w.on_failure_webhook, "cluster_policy_name": policy_name,
        "tasks": [_serialize_task(t) for t in w.tasks],
        "last_run_status": (last_run.status.value if last_run and hasattr(last_run.status, "value") else (last_run.status if last_run else None)),
        "last_run_finished_at": last_run.finished_at.isoformat() if last_run and last_run.finished_at else None,
        "next_run_at": next_run_at,
    }


def _serialize_task_run(tr: models.TaskRun) -> dict:
    return {
        "id": tr.id, "task_key": tr.task_key,
        "status": tr.status.value if hasattr(tr.status, "value") else tr.status,
        "attempt": tr.attempt,
        "started_at": tr.started_at.isoformat() if tr.started_at else None,
        "finished_at": tr.finished_at.isoformat() if tr.finished_at else None,
        "logs": tr.logs or "",
        "output": tr.output or {},
    }


COST_PER_WORKER_HOUR = float(os.getenv("ATLASFLOW_COST_PER_WORKER_HOUR", "0.40"))


def _estimate_run_cost(task_runs) -> float:
    """Rough compute-cost estimate: sum of task wall-clock time x an hourly rate you
    configure. This is a self-hosted stack, so there's no real vendor billing to report -
    this is a usage estimate, not an invoice, and is labeled as such in the UI."""
    total_seconds = 0
    for tr in task_runs:
        if tr.started_at and tr.finished_at:
            total_seconds += (tr.finished_at - tr.started_at).total_seconds()
    return round((total_seconds / 3600) * COST_PER_WORKER_HOUR, 4)


def _serialize_run(r: models.WorkflowRun) -> dict:
    return {
        "id": r.id, "workflow_id": r.workflow_id,
        "status": r.status.value if hasattr(r.status, "value") else r.status,
        "triggered_by": r.triggered_by,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "task_runs": [_serialize_task_run(tr) for tr in r.task_runs],
        "estimated_cost_usd": _estimate_run_cost(r.task_runs),
    }


# --- auth ---------------------------------------------------------------------

@app.post("/api/auth/login", response_model=schemas.Token)
@limiter.limit("5/minute")
def login(request: Request, payload: schemas.LoginRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=payload.username).first()
    if not user or not user.is_active or not user.hashed_password or not security.verify_password(payload.password, user.hashed_password):
        _audit(db, None, "auth.login_failed", resource=payload.username, request=request)
        raise HTTPException(401, "Invalid username or password")
    _audit(db, user, "auth.login", request=request)
    token = security.create_access_token(user)
    return {"access_token": token, "role": user.role, "username": user.username}


@app.get("/api/auth/sso/config")
def sso_config():
    from . import saml
    return {"oidc_enabled": security_oidc_enabled(), "saml_enabled": saml.is_enabled()}


@app.get("/api/auth/sso/login")
def sso_login():
    from . import oidc
    if not oidc.is_enabled():
        raise HTTPException(404, "SSO is not configured")
    return RedirectResponse(oidc.build_authorize_url())


@app.get("/api/auth/sso/callback")
def sso_callback(code: str, state: str, db: Session = Depends(get_db)):
    from . import oidc
    frontend_url = os.getenv("ATLASFLOW_FRONTEND_URL", "http://localhost:3000")
    try:
        claims = oidc.exchange_code(code, state)
        username = oidc.username_from_claims(claims)
        user = db.query(models.User).filter_by(username=username).first()
        if not user:
            # first SSO login for this identity - provision as viewer; an admin promotes as needed
            user = models.User(username=username, hashed_password=None, role="viewer", auth_provider="oidc")
            db.add(user)
            db.commit()
        if not user.is_active:
            raise ValueError("This account is disabled")
        _audit(db, user, "auth.sso_login")
        token = security.create_access_token(user)
        redirect_url = f"{frontend_url}/?sso_token={token}&username={user.username}&role={user.role}"
        return RedirectResponse(redirect_url)
    except Exception as e:
        return RedirectResponse(f"{frontend_url}/?sso_error={urllib.parse.quote(str(e))}")


def security_oidc_enabled() -> bool:
    from . import oidc
    return oidc.is_enabled()


# --- SAML SSO (alongside OIDC above) ---

@app.get("/api/auth/saml/metadata")
def saml_metadata():
    from . import saml
    from fastapi.responses import Response
    if not saml.is_enabled():
        raise HTTPException(404, "SAML is not configured")
    xml, errors = saml.metadata_xml()
    if errors:
        raise HTTPException(500, f"Invalid SAML SP settings: {errors}")
    return Response(content=xml, media_type="application/xml")


@app.get("/api/auth/saml/login")
async def saml_login(request: Request):
    from . import saml
    if not saml.is_enabled():
        raise HTTPException(404, "SAML is not configured")
    auth = await saml.build_auth(request)
    return RedirectResponse(auth.login())


@app.post("/api/auth/saml/acs")
async def saml_acs(request: Request, db: Session = Depends(get_db)):
    from . import saml
    frontend_url = os.getenv("ATLASFLOW_FRONTEND_URL", "http://localhost:3000")
    try:
        auth = await saml.build_auth(request)
        auth.process_response()
        errors = auth.get_errors()
        if errors:
            raise ValueError(f"SAML validation failed: {errors} - {auth.get_last_error_reason()}")
        if not auth.is_authenticated():
            raise ValueError("SAML authentication was not successful")

        attributes = auth.get_attributes()
        name_id = auth.get_nameid()
        username = (attributes.get("email") or attributes.get("emailaddress") or [None])[0] if attributes else None
        username = username or name_id

        user = db.query(models.User).filter_by(username=username).first()
        if not user:
            user = models.User(username=username, hashed_password=None, role="viewer", auth_provider="saml")
            db.add(user)
            db.commit()
        if not user.is_active:
            raise ValueError("This account is disabled")
        _audit(db, user, "auth.saml_login")
        token = security.create_access_token(user)
        return RedirectResponse(f"{frontend_url}/?sso_token={token}&username={user.username}&role={user.role}", status_code=303)
    except Exception as e:
        return RedirectResponse(f"{frontend_url}/?sso_error={urllib.parse.quote(str(e))}", status_code=303)


@app.post("/api/auth/users", response_model=schemas.UserOut)
def create_user(payload: schemas.UserCreate, db: Session = Depends(get_db),
                 current: models.User = Depends(security.require_role("admin"))):
    if payload.role not in security.ROLE_RANK:
        raise HTTPException(400, "role must be one of viewer, editor, admin")
    if db.query(models.User).filter_by(username=payload.username).first():
        raise HTTPException(400, "Username already exists")
    user = models.User(username=payload.username, hashed_password=security.hash_password(payload.password), role=payload.role)
    db.add(user)
    _audit(db, current, "user.create", resource=payload.username, details={"role": payload.role})
    return user


@app.get("/api/auth/users", response_model=list[schemas.UserOut])
def list_users(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    return db.query(models.User).all()


@app.get("/api/auth/me", response_model=schemas.UserOut)
def whoami(current: models.User = Depends(security.get_current_user)):
    return current


# --- workflows: viewers can read, editors+ can mutate, admins can delete ---

@app.get("/api/workflows")
def list_workflows(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    return [_serialize_workflow(w, db) for w in db.query(models.Workflow).all()]


@app.post("/api/workflows")
def create_workflow(payload: schemas.WorkflowCreate, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    if db.query(models.Workflow).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Workflow '{payload.name}' already exists")

    policy = None
    if payload.cluster_policy_name:
        policy = db.query(models.ClusterPolicy).filter_by(name=payload.cluster_policy_name).first()
        if not policy:
            raise HTTPException(400, f"Cluster policy '{payload.cluster_policy_name}' not found")
        for t in payload.tasks:
            if policy.allowed_task_types and t.type not in policy.allowed_task_types:
                raise HTTPException(400, f"Task type '{t.type}' not allowed by policy '{policy.name}' (allowed: {policy.allowed_task_types})")
            if policy.max_timeout_seconds and t.timeout_seconds > policy.max_timeout_seconds:
                raise HTTPException(400, f"Task '{t.key}' timeout ({t.timeout_seconds}s) exceeds policy '{policy.name}' max ({policy.max_timeout_seconds}s)")
            if policy.max_retries is not None and t.retries > policy.max_retries:
                raise HTTPException(400, f"Task '{t.key}' retries ({t.retries}) exceeds policy '{policy.name}' max ({policy.max_retries})")

    wf = models.Workflow(
        name=payload.name, description=payload.description,
        schedule_cron=payload.schedule_cron, on_failure_webhook=payload.on_failure_webhook,
        cluster_policy_id=policy.id if policy else None,
        created_by=current.id,
    )
    db.add(wf)
    db.flush()

    keys = [t.key for t in payload.tasks]
    if len(keys) != len(set(keys)):
        raise HTTPException(400, "Task keys must be unique within a workflow")

    for t in payload.tasks:
        db.add(models.Task(
            workflow_id=wf.id, key=t.key, type=t.type, command=t.command, params=t.params,
            depends_on=t.depends_on, run_if=t.run_if, retries=t.retries, timeout_seconds=t.timeout_seconds,
        ))
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "workflow.create", resource=wf.name, details={"cluster_policy": payload.cluster_policy_name}, request=request)
    return _serialize_workflow(wf, db)


@app.delete("/api/workflows/{workflow_id}")
def delete_workflow(workflow_id: str, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("admin"))):
    wf = db.query(models.Workflow).get(workflow_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")
    name = wf.name
    db.delete(wf)
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "workflow.delete", resource=name, request=request)
    return {"ok": True}


@app.post("/api/workflows/{workflow_id}/clone")
def clone_workflow(workflow_id: str, request: Request, db: Session = Depends(get_db),
                    current: models.User = Depends(security.require_role("editor"))):
    """Duplicates a workflow (all tasks included) under a new auto-generated name.
    The clone starts paused, so cloning a scheduled workflow never silently doubles up runs."""
    src = db.query(models.Workflow).get(workflow_id)
    if not src:
        raise HTTPException(404, "Workflow not found")

    base_name = f"{src.name}_copy"
    new_name = base_name
    n = 2
    while db.query(models.Workflow).filter_by(name=new_name).first():
        new_name = f"{base_name}_{n}"
        n += 1

    clone = models.Workflow(
        name=new_name, description=src.description, schedule_cron=src.schedule_cron,
        on_failure_webhook=src.on_failure_webhook, is_paused=True,
    )
    db.add(clone)
    db.flush()
    for t in src.tasks:
        db.add(models.Task(
            workflow_id=clone.id, key=t.key, type=t.type, command=t.command, params=t.params,
            depends_on=t.depends_on, run_if=t.run_if, retries=t.retries, timeout_seconds=t.timeout_seconds,
        ))
    db.commit()
    _audit(db, current, "workflow.clone", resource=new_name, details={"source": src.name}, request=request)
    return _serialize_workflow(clone, db)


@app.post("/api/workflows/{workflow_id}/pause")
def pause_workflow(workflow_id: str, request: Request, db: Session = Depends(get_db),
                    current: models.User = Depends(security.require_role("editor"))):
    wf = db.query(models.Workflow).get(workflow_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")
    wf.is_paused = not wf.is_paused
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "workflow.pause_toggle", resource=wf.name, details={"is_paused": wf.is_paused}, request=request)
    return _serialize_workflow(wf, db)


@app.post("/api/workflows/{workflow_id}/trigger")
def trigger_workflow(workflow_id: str, request: Request, db: Session = Depends(get_db),
                      current: models.User = Depends(security.require_role("editor"))):
    wf = db.query(models.Workflow).get(workflow_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")

    run = models.WorkflowRun(workflow_id=wf.id, triggered_by=f"manual:{current.username}")
    db.add(run)
    db.commit()
    run_id = run.id
    _audit(db, current, "workflow.trigger", resource=wf.name, details={"run_id": run_id}, request=request)

    def _run():
        bg_db = SessionLocal()
        try:
            execute_workflow_run(bg_db, run_id)
        finally:
            bg_db.close()

    threading.Thread(target=_run, daemon=True).start()
    return {"run_id": run_id}


@app.get("/api/workflows/{workflow_id}/runs")
def list_runs(workflow_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    runs = (
        db.query(models.WorkflowRun).filter_by(workflow_id=workflow_id)
        .order_by(models.WorkflowRun.started_at.desc()).limit(50).all()
    )
    return [_serialize_run(r) for r in runs]


@app.get("/api/runs/{run_id}")
def get_run(run_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    r = db.query(models.WorkflowRun).get(run_id)
    if not r:
        raise HTTPException(404, "Run not found")
    return _serialize_run(r)


# --- secrets: editors can write, values never leave the server once stored ---

@app.get("/api/secrets", response_model=list[schemas.SecretOut])
def list_secrets(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    secrets = db.query(models.Secret).all()
    return [{"id": s.id, "name": s.name, "created_at": s.created_at.isoformat()} for s in secrets]


@app.post("/api/secrets", response_model=schemas.SecretOut)
def create_secret(payload: schemas.SecretCreate, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("editor"))):
    existing = db.query(models.Secret).filter_by(name=payload.name).first()
    encrypted = security.encrypt_secret(payload.value)
    if existing:
        existing.encrypted_value = encrypted
        db.commit()
        _audit(db, current, "secret.update", resource=payload.name, request=request)
        return {"id": existing.id, "name": existing.name, "created_at": existing.created_at.isoformat()}
    s = models.Secret(name=payload.name, encrypted_value=encrypted, created_by=current.id)
    db.add(s)
    db.commit()
    _audit(db, current, "secret.create", resource=payload.name, request=request)
    return {"id": s.id, "name": s.name, "created_at": s.created_at.isoformat()}


@app.delete("/api/secrets/{secret_id}")
def delete_secret(secret_id: str, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("admin"))):
    s = db.query(models.Secret).get(secret_id)
    if not s:
        raise HTTPException(404, "Secret not found")
    db.delete(s)
    db.commit()
    _audit(db, current, "secret.delete", resource=s.name, request=request)
    return {"ok": True}


# --- table catalog: onboard raw files as named Delta tables, queryable by name ---
# Also covers table-level governance (visibility + grants - "who can read this table"),
# lineage (source_tables - "what was this derived from"), and feature-store tagging.

UPLOAD_DIR = "/data/uploads"
TABLES_DIR = "/data/tables"


def _can_read_table(table: models.Table, user: models.User, db: Session) -> bool:
    if table.visibility != "restricted" or user.role == "admin" or table.created_by == user.id:
        return True
    return db.query(models.TableGrant).filter_by(table_id=table.id, user_id=user.id).first() is not None


def _require_table_access(table_name: str, user: models.User, db: Session):
    table = db.query(models.Table).filter_by(name=table_name).first()
    if table and not _can_read_table(table, user, db):
        raise HTTPException(403, f"You don't have access to table '{table_name}' (restricted)")


@app.get("/api/tables", response_model=list[schemas.TableOut])
def list_tables(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    all_tables = db.query(models.Table).order_by(models.Table.created_at.desc()).all()
    return [t for t in all_tables if _can_read_table(t, current, db)]


@app.post("/api/tables/upload", response_model=schemas.TableOut)
async def upload_table(
    request: Request,
    file: UploadFile = File(...),
    name: str = Form(...),
    description: str = Form(""),
    file_format: str = Form("csv"),
    visibility: str = Form("public"),
    is_feature_table: bool = Form(False),
    entity_key: str = Form(""),
    timestamp_key: str = Form(""),
    db: Session = Depends(get_db),
    current: models.User = Depends(security.require_role("editor")),
):
    """Turns an uploaded CSV/JSON/Parquet file into a real Delta table and registers it
    in the catalog under `name` - after this, SQL/Python can reference it by that name
    instead of a raw storage path. This is the 'table onboarding' step."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise HTTPException(400, "Table name must start with a letter/underscore and contain only letters, numbers, underscores")
    if db.query(models.Table).filter_by(name=name).first():
        raise HTTPException(400, f"Table '{name}' already exists")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    upload_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{file.filename}")
    with open(upload_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    output_path = os.path.join(TABLES_DIR, name)
    from .task_runners import run_spark_submit
    success, logs, output = run_spark_submit("/jobs/onboard_table.py", {
        "args": ["--input", upload_path, "--output", output_path, "--format", file_format],
        "_timeout": 1800,
    })
    try:
        os.remove(upload_path)
    except OSError:
        pass
    if not success:
        raise HTTPException(400, f"Failed to onboard table - see logs: {logs[-2000:]}")

    table = models.Table(
        name=name, path=output_path, description=description,
        row_count=output.get("row_count"), columns=output.get("columns", []),
        created_by=current.id, visibility=visibility, origin="upload",
        is_feature_table=is_feature_table, entity_key=entity_key or None, timestamp_key=timestamp_key or None,
    )
    db.add(table)
    db.commit()
    _audit(db, current, "table.onboard", resource=name, details={"row_count": output.get("row_count")}, request=request)
    return table


@app.post("/api/tables/register", response_model=schemas.TableOut)
def register_table(payload: schemas.TableRegister, request: Request, db: Session = Depends(get_db),
                    current: models.User = Depends(security.require_role("editor"))):
    """Registers a Delta table that already exists at `path` under a friendly name,
    without going through the upload/onboard flow (e.g. a table a Spark job already wrote).
    Use `source_tables` to record lineage if this table was derived from others."""
    if db.query(models.Table).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Table '{payload.name}' already exists")
    table = models.Table(
        name=payload.name, path=payload.path, description=payload.description, created_by=current.id,
        visibility=payload.visibility, source_tables=payload.source_tables, origin="registered",
        is_feature_table=payload.is_feature_table, entity_key=payload.entity_key, timestamp_key=payload.timestamp_key,
    )
    db.add(table)
    db.commit()
    _audit(db, current, "table.register", resource=payload.name, details={"source_tables": payload.source_tables}, request=request)
    return table


@app.delete("/api/tables/{table_id}")
def delete_table(table_id: str, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("admin"))):
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    name = t.name
    db.delete(t)
    db.commit()
    _audit(db, current, "table.delete", resource=name, request=request)
    return {"ok": True}


@app.get("/api/tables/{table_id}/lineage")
def table_lineage(table_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    if not _can_read_table(t, current, db):
        raise HTTPException(403, "You don't have access to this table")
    upstream = db.query(models.Table).filter(models.Table.name.in_(t.source_tables or [])).all()
    downstream = db.query(models.Table).filter(models.Table.source_tables.isnot(None)).all()
    downstream = [d for d in downstream if t.name in (d.source_tables or [])]
    return {
        "table": t.name, "origin": t.origin,
        "upstream": [{"id": u.id, "name": u.name} for u in upstream],
        "downstream": [{"id": d.id, "name": d.name} for d in downstream],
    }


@app.post("/api/tables/{table_id}/visibility")
def set_table_visibility(table_id: str, visibility: str, request: Request, db: Session = Depends(get_db),
                          current: models.User = Depends(security.require_role("editor"))):
    if visibility not in ("public", "restricted"):
        raise HTTPException(400, "visibility must be 'public' or 'restricted'")
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    t.visibility = visibility
    db.commit()
    _audit(db, current, "table.visibility_change", resource=t.name, details={"visibility": visibility}, request=request)
    return {"ok": True}


@app.get("/api/tables/{table_id}/security")
def get_table_security(table_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    return {"row_filters": t.row_filters or {}, "masked_columns": t.masked_columns or []}


@app.post("/api/tables/{table_id}/row-filter")
def set_row_filter(table_id: str, payload: schemas.RowFilterSet, request: Request, db: Session = Depends(get_db),
                    current: models.User = Depends(security.require_role("admin"))):
    """Sets (or, with an empty filter_expr, removes) a row-level security filter for a
    role - real query rewriting applied server-side in app/catalog.py, not a client-side
    filter a user could bypass."""
    if payload.role not in security.ROLE_RANK:
        raise HTTPException(400, "role must be viewer, editor, or admin")
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    filters = dict(t.row_filters or {})
    if payload.filter_expr:
        filters[payload.role] = payload.filter_expr
    else:
        filters.pop(payload.role, None)
    t.row_filters = filters
    db.commit()
    _audit(db, current, "table.row_filter_set", resource=t.name, details={"role": payload.role, "filter": payload.filter_expr}, request=request)
    return {"row_filters": t.row_filters}


@app.post("/api/tables/{table_id}/column-mask")
def set_column_mask(table_id: str, payload: schemas.ColumnMaskSet, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("admin"))):
    """Masks a column with NULL for anyone below `unmask_role` - applied via Spark SQL's
    `SELECT * REPLACE(...)` when the table is queried, not a display-layer redaction."""
    if payload.unmask_role not in security.ROLE_RANK:
        raise HTTPException(400, "unmask_role must be viewer, editor, or admin")
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    masks = [m for m in (t.masked_columns or []) if m["column"] != payload.column]
    masks.append({"column": payload.column, "unmask_role": payload.unmask_role})
    t.masked_columns = masks
    db.commit()
    _audit(db, current, "table.column_mask_set", resource=t.name, details={"column": payload.column, "unmask_role": payload.unmask_role}, request=request)
    return {"masked_columns": t.masked_columns}


@app.post("/api/tables/{table_id}/column-mask/remove")
def remove_column_mask(table_id: str, payload: schemas.ColumnMaskRemove, db: Session = Depends(get_db),
                        current: models.User = Depends(security.require_role("admin"))):
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    t.masked_columns = [m for m in (t.masked_columns or []) if m["column"] != payload.column]
    db.commit()
    return {"masked_columns": t.masked_columns}


@app.post("/api/tables/{table_id}/grants")
def grant_table_access(table_id: str, payload: schemas.GrantCreate, request: Request, db: Session = Depends(get_db),
                        current: models.User = Depends(security.require_role("editor"))):
    t = db.query(models.Table).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    grantee = db.query(models.User).filter_by(username=payload.username).first()
    if not grantee:
        raise HTTPException(404, f"User '{payload.username}' not found")
    if not db.query(models.TableGrant).filter_by(table_id=t.id, user_id=grantee.id).first():
        db.add(models.TableGrant(table_id=t.id, user_id=grantee.id, granted_by=current.id))
        db.commit()
    _audit(db, current, "table.grant", resource=t.name, details={"granted_to": payload.username}, request=request)
    return {"ok": True}


@app.get("/api/tables/{table_id}/grants")
def list_table_grants(table_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    grants = db.query(models.TableGrant).filter_by(table_id=table_id).all()
    out = []
    for g in grants:
        u = db.query(models.User).get(g.user_id)
        out.append({"id": g.id, "username": u.username if u else "?"})
    return out


@app.delete("/api/tables/grants/{grant_id}")
def revoke_table_grant(grant_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    g = db.query(models.TableGrant).get(grant_id)
    if g:
        db.delete(g)
        db.commit()
    return {"ok": True}


# --- volumes: browsable storage for arbitrary files (not just tabular data) ---
# AtlasFlow's equivalent of Unity Catalog Volumes. Backed by MinIO (app/storage.py).

def _can_read_volume(volume: models.Volume, user: models.User, db: Session) -> bool:
    if volume.visibility != "restricted" or user.role == "admin" or volume.created_by == user.id:
        return True
    return db.query(models.VolumeGrant).filter_by(volume_id=volume.id, user_id=user.id).first() is not None


def _get_accessible_volume(volume_id: str, user: models.User, db: Session) -> models.Volume:
    v = db.query(models.Volume).get(volume_id)
    if not v:
        raise HTTPException(404, "Volume not found")
    if not _can_read_volume(v, user, db):
        raise HTTPException(403, "You don't have access to this volume (restricted)")
    return v


@app.get("/api/volumes", response_model=list[schemas.VolumeOut])
def list_volumes(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    all_volumes = db.query(models.Volume).order_by(models.Volume.created_at.desc()).all()
    return [v for v in all_volumes if _can_read_volume(v, current, db)]


@app.post("/api/volumes", response_model=schemas.VolumeOut)
def create_volume(payload: schemas.VolumeCreate, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("editor"))):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$", payload.name):
        raise HTTPException(400, "Volume name must start with a letter/underscore and contain only letters, numbers, underscores, hyphens")
    if db.query(models.Volume).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Volume '{payload.name}' already exists")
    v = models.Volume(name=payload.name, description=payload.description, visibility=payload.visibility, created_by=current.id)
    db.add(v)
    db.commit()
    _audit(db, current, "volume.create", resource=payload.name, request=request)
    return v


@app.delete("/api/volumes/{volume_id}")
def delete_volume(volume_id: str, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("admin"))):
    from . import storage
    v = db.query(models.Volume).get(volume_id)
    if not v:
        raise HTTPException(404, "Volume not found")
    name = v.name
    storage.delete_volume_prefix(name)
    db.query(models.VolumeGrant).filter_by(volume_id=v.id).delete()
    db.delete(v)
    db.commit()
    _audit(db, current, "volume.delete", resource=name, request=request)
    return {"ok": True}


@app.post("/api/volumes/{volume_id}/visibility")
def set_volume_visibility(volume_id: str, visibility: str, request: Request, db: Session = Depends(get_db),
                           current: models.User = Depends(security.require_role("editor"))):
    if visibility not in ("public", "restricted"):
        raise HTTPException(400, "visibility must be 'public' or 'restricted'")
    v = db.query(models.Volume).get(volume_id)
    if not v:
        raise HTTPException(404, "Volume not found")
    v.visibility = visibility
    db.commit()
    _audit(db, current, "volume.visibility_change", resource=v.name, details={"visibility": visibility}, request=request)
    return {"ok": True}


@app.post("/api/volumes/{volume_id}/grants")
def grant_volume_access(volume_id: str, payload: schemas.GrantCreate, request: Request, db: Session = Depends(get_db),
                         current: models.User = Depends(security.require_role("editor"))):
    v = db.query(models.Volume).get(volume_id)
    if not v:
        raise HTTPException(404, "Volume not found")
    grantee = db.query(models.User).filter_by(username=payload.username).first()
    if not grantee:
        raise HTTPException(404, f"User '{payload.username}' not found")
    if not db.query(models.VolumeGrant).filter_by(volume_id=v.id, user_id=grantee.id).first():
        db.add(models.VolumeGrant(volume_id=v.id, user_id=grantee.id, granted_by=current.id))
        db.commit()
    _audit(db, current, "volume.grant", resource=v.name, details={"granted_to": payload.username}, request=request)
    return {"ok": True}


@app.get("/api/volumes/{volume_id}/grants")
def list_volume_grants(volume_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    grants = db.query(models.VolumeGrant).filter_by(volume_id=volume_id).all()
    out = []
    for g in grants:
        u = db.query(models.User).get(g.user_id)
        out.append({"id": g.id, "username": u.username if u else "?"})
    return out


@app.delete("/api/volumes/grants/{grant_id}")
def revoke_volume_grant(grant_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    g = db.query(models.VolumeGrant).get(grant_id)
    if g:
        db.delete(g)
        db.commit()
    return {"ok": True}


@app.get("/api/volumes/{volume_id}/browse", response_model=list[schemas.VolumeEntry])
def browse_volume(volume_id: str, path: str = "", db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("viewer"))):
    from . import storage
    v = _get_accessible_volume(volume_id, current, db)
    return storage.list_dir(v.name, path)


@app.post("/api/volumes/{volume_id}/mkdir")
def volume_mkdir(volume_id: str, path: str, name: str, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("editor"))):
    from . import storage
    v = _get_accessible_volume(volume_id, current, db)
    new_path = f"{path.strip('/')}/{name}" if path.strip("/") else name
    storage.mkdir(v.name, new_path)
    _audit(db, current, "volume.mkdir", resource=v.name, details={"path": new_path}, request=request)
    return {"ok": True}


@app.post("/api/volumes/{volume_id}/upload")
async def volume_upload(volume_id: str, request: Request, path: str = Form(""), file: UploadFile = File(...),
                         db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    from . import storage
    v = _get_accessible_volume(volume_id, current, db)
    storage.upload(v.name, path, file.filename, file.file)
    _audit(db, current, "volume.upload", resource=v.name, details={"path": path, "filename": file.filename}, request=request)
    return {"ok": True}


@app.get("/api/volumes/{volume_id}/download")
def volume_download(volume_id: str, path: str, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("viewer"))):
    from . import storage
    v = _get_accessible_volume(volume_id, current, db)
    return {"url": storage.download_url(v.name, path)}


@app.delete("/api/volumes/{volume_id}/object")
def volume_delete_object(volume_id: str, path: str, is_dir: bool, request: Request, db: Session = Depends(get_db),
                          current: models.User = Depends(security.require_role("editor"))):
    from . import storage
    v = _get_accessible_volume(volume_id, current, db)
    storage.delete(v.name, path, is_dir)
    _audit(db, current, "volume.delete_object", resource=v.name, details={"path": path}, request=request)
    return {"ok": True}


# --- dashboards: save multiple SQL tiles together, mirroring Databricks SQL dashboards ---

@app.get("/api/dashboards")
def list_dashboards(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    dashboards = db.query(models.Dashboard).all()
    out = []
    for d in dashboards:
        tiles = db.query(models.DashboardTile).filter_by(dashboard_id=d.id).order_by(models.DashboardTile.position).all()
        out.append({
            "id": d.id, "name": d.name, "created_at": d.created_at.isoformat(),
            "tiles": [{"id": t.id, "title": t.title, "sql": t.sql, "chart_type": t.chart_type} for t in tiles],
        })
    return out


@app.post("/api/dashboards")
def create_dashboard(payload: schemas.DashboardCreate, request: Request, db: Session = Depends(get_db),
                      current: models.User = Depends(security.require_role("editor"))):
    if db.query(models.Dashboard).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Dashboard '{payload.name}' already exists")
    d = models.Dashboard(name=payload.name, created_by=current.id)
    db.add(d)
    db.flush()
    for i, tile in enumerate(payload.tiles):
        db.add(models.DashboardTile(dashboard_id=d.id, title=tile.title, sql=tile.sql, chart_type=tile.chart_type, position=i))
    db.commit()
    _audit(db, current, "dashboard.create", resource=payload.name, request=request)
    return {"id": d.id, "name": d.name}


@app.delete("/api/dashboards/{dashboard_id}")
def delete_dashboard(dashboard_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    d = db.query(models.Dashboard).get(dashboard_id)
    if d:
        db.query(models.DashboardTile).filter_by(dashboard_id=d.id).delete()
        db.delete(d)
        db.commit()
    return {"ok": True}


@app.post("/api/dashboards/{dashboard_id}/refresh")
def refresh_dashboard(dashboard_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    """Re-runs every tile's query and returns fresh results - dashboards store the SQL,
    not stale results, so this is always live data."""
    from .task_runners import run_sql
    from .catalog import resolve_table_names, referenced_table_names
    tiles = db.query(models.DashboardTile).filter_by(dashboard_id=dashboard_id).order_by(models.DashboardTile.position).all()
    results = []
    for tile in tiles:
        try:
            for name in referenced_table_names(tile.sql, db):
                _require_table_access(name, current, db)
            resolved = resolve_table_names(tile.sql, db, current.role)
            success, logs, output = run_sql(resolved, {"limit": 200, "_timeout": 300})
            results.append({
                "tile_id": tile.id, "title": tile.title, "chart_type": tile.chart_type,
                "success": success,
                "columns": output.get("columns", []), "rows": output.get("rows", []),
                "error": None if success else logs[-500:],
            })
        except HTTPException as e:
            results.append({"tile_id": tile.id, "title": tile.title, "chart_type": tile.chart_type, "success": False, "columns": [], "rows": [], "error": e.detail})
    return {"tiles": results}


# --- alerts: SQL threshold alerting, mirroring Databricks SQL Alerts ---

def _serialize_alert(a: models.Alert) -> dict:
    return {
        "id": a.id, "name": a.name, "sql": a.sql, "value_column": a.value_column,
        "operator": a.operator, "threshold": a.threshold, "schedule_cron": a.schedule_cron,
        "notify_webhook": a.notify_webhook, "is_paused": a.is_paused,
        "last_status": a.last_status, "last_value": a.last_value,
        "last_checked_at": a.last_checked_at.isoformat() if a.last_checked_at else None,
    }


@app.get("/api/alerts", response_model=list[schemas.AlertOut])
def list_alerts(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    return [_serialize_alert(a) for a in db.query(models.Alert).order_by(models.Alert.created_at.desc()).all()]


@app.post("/api/alerts", response_model=schemas.AlertOut)
def create_alert(payload: schemas.AlertCreate, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("editor"))):
    if payload.operator not in (">", "<", ">=", "<=", "==", "!="):
        raise HTTPException(400, "operator must be one of >, <, >=, <=, ==, !=")
    if db.query(models.Alert).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Alert '{payload.name}' already exists")
    a = models.Alert(
        name=payload.name, sql=payload.sql, value_column=payload.value_column,
        operator=payload.operator, threshold=payload.threshold,
        schedule_cron=payload.schedule_cron, notify_webhook=payload.notify_webhook,
        created_by=current.id,
    )
    db.add(a)
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "alert.create", resource=payload.name, request=request)
    return _serialize_alert(a)


@app.delete("/api/alerts/{alert_id}")
def delete_alert(alert_id: str, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("admin"))):
    a = db.query(models.Alert).get(alert_id)
    if not a:
        raise HTTPException(404, "Alert not found")
    name = a.name
    db.query(models.AlertHistory).filter_by(alert_id=a.id).delete()
    db.delete(a)
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "alert.delete", resource=name, request=request)
    return {"ok": True}


@app.post("/api/alerts/{alert_id}/pause")
def pause_alert(alert_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    a = db.query(models.Alert).get(alert_id)
    if not a:
        raise HTTPException(404, "Alert not found")
    a.is_paused = not a.is_paused
    db.commit()
    scheduler.sync_jobs()
    return _serialize_alert(a)


@app.post("/api/alerts/{alert_id}/check", response_model=schemas.AlertOut)
def check_alert_now(alert_id: str, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    from .alerts import evaluate_alert
    a = db.query(models.Alert).get(alert_id)
    if not a:
        raise HTTPException(404, "Alert not found")
    evaluate_alert(db, a)
    _audit(db, current, "alert.check", resource=a.name, details={"result": a.last_status}, request=request)
    return _serialize_alert(a)


@app.get("/api/alerts/{alert_id}/history", response_model=list[schemas.AlertHistoryOut])
def alert_history(alert_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = (
        db.query(models.AlertHistory).filter_by(alert_id=alert_id)
        .order_by(models.AlertHistory.checked_at.desc()).limit(50).all()
    )
    return [{
        "id": h.id, "status": h.status, "value": h.value, "message": h.message,
        "checked_at": h.checked_at.isoformat(),
    } for h in rows]


# --- genie: natural language -> SQL, via the real Anthropic API ---

@app.get("/api/genie/config")
def genie_config():
    from . import genie
    return {"enabled": genie.is_enabled()}


@app.post("/api/genie/ask", response_model=schemas.GenieAskResponse)
def genie_ask(payload: schemas.GenieAskRequest, request: Request, db: Session = Depends(get_db),
              current: models.User = Depends(security.require_role("editor"))):
    from . import genie
    from .task_runners import run_sql
    from .catalog import resolve_table_names, referenced_table_names

    if not genie.is_enabled():
        raise HTTPException(404, "Genie is not configured - set ATLASFLOW_ANTHROPIC_API_KEY")

    accessible = [t.name for t in db.query(models.Table).all() if _can_read_table(t, current, db)]
    schema_context = genie.build_schema_context(db, accessible)

    record = models.GenieQuery(question=payload.question, created_by=current.id)
    try:
        result = genie.ask(payload.question, schema_context)
        sql = result["sql"]
        record.generated_sql = sql

        for name in referenced_table_names(sql, db):
            _require_table_access(name, current, db)
        resolved_sql = resolve_table_names(sql, db, current.role)
        success, logs, output = run_sql(resolved_sql, {"limit": 200, "_timeout": 300})

        if not success:
            record.success = False
            record.error = logs[-1000:]
            db.add(record); db.commit()
            _audit(db, current, "genie.ask", details={"question": payload.question, "success": False}, request=request)
            return {"question": payload.question, "sql": sql, "success": False, "error": logs[-1000:]}

        record.success = True
        record.row_count = output.get("row_count", 0)
        db.add(record); db.commit()
        _audit(db, current, "genie.ask", details={"question": payload.question, "success": True}, request=request)
        return {
            "question": payload.question, "sql": sql, "success": True,
            "columns": output.get("columns", []), "rows": output.get("rows", []),
            "row_count": output.get("row_count", 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        record.success = False
        record.error = str(e)
        db.add(record); db.commit()
        return {"question": payload.question, "sql": record.generated_sql, "success": False, "error": str(e)}


@app.get("/api/genie/history", response_model=list[schemas.GenieHistoryOut])
def genie_history(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = db.query(models.GenieQuery).order_by(models.GenieQuery.created_at.desc()).limit(50).all()
    return [{
        "id": r.id, "question": r.question, "generated_sql": r.generated_sql,
        "success": r.success, "row_count": r.row_count, "created_at": r.created_at.isoformat(),
    } for r in rows]


# --- notebooks: list files, and a "quick schedule" shortcut that wraps one notebook
# in a single-task workflow, without having to hand-build a full DAG for it ---

NOTEBOOKS_DIR = "/notebooks"


@app.get("/api/notebooks", response_model=list[schemas.NotebookFile])
def list_notebooks(current: models.User = Depends(security.require_role("viewer"))):
    if not os.path.isdir(NOTEBOOKS_DIR):
        return []
    files = []
    for root, _, filenames in os.walk(NOTEBOOKS_DIR):
        for fn in filenames:
            if fn.endswith(".ipynb") and ".ipynb_checkpoints" not in root:
                full = os.path.join(root, fn)
                files.append({"name": fn, "path": full})
    return sorted(files, key=lambda f: f["name"])


@app.post("/api/notebooks/quick-schedule")
def quick_schedule_notebook(payload: schemas.NotebookQuickSchedule, request: Request, db: Session = Depends(get_db),
                             current: models.User = Depends(security.require_role("editor"))):
    """Wraps a single notebook in a new one-task workflow - the honest equivalent of
    Databricks' 'Schedule' button directly on a notebook, since AtlasFlow schedules
    workflows rather than individual notebook files."""
    base_name = payload.workflow_name or f"notebook_{os.path.basename(payload.notebook_path).replace('.ipynb', '')}"
    name = base_name
    n = 2
    while db.query(models.Workflow).filter_by(name=name).first():
        name = f"{base_name}_{n}"
        n += 1

    wf = models.Workflow(name=name, description=f"Quick-scheduled from {payload.notebook_path}",
                          schedule_cron=payload.schedule_cron, created_by=current.id)
    db.add(wf)
    db.flush()
    db.add(models.Task(
        workflow_id=wf.id, key="notebook", type=models.TaskType.NOTEBOOK,
        command=payload.notebook_path, params=payload.params, depends_on=[],
    ))
    db.commit()
    scheduler.sync_jobs()
    _audit(db, current, "notebook.quick_schedule", resource=name, details={"notebook": payload.notebook_path}, request=request)
    return _serialize_workflow(wf, db)


# --- run comments: a discussion thread per workflow run ---

@app.get("/api/runs/{run_id}/comments", response_model=list[schemas.CommentOut])
def list_run_comments(run_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = db.query(models.RunComment).filter_by(workflow_run_id=run_id).order_by(models.RunComment.created_at).all()
    return [{"id": c.id, "username": c.username, "text": c.text, "created_at": c.created_at.isoformat()} for c in rows]


@app.post("/api/runs/{run_id}/comments", response_model=schemas.CommentOut)
def add_run_comment(run_id: str, payload: schemas.CommentCreate, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("viewer"))):
    if not db.query(models.WorkflowRun).get(run_id):
        raise HTTPException(404, "Run not found")
    c = models.RunComment(workflow_run_id=run_id, user_id=current.id, username=current.username, text=payload.text)
    db.add(c)
    db.commit()
    return {"id": c.id, "username": c.username, "text": c.text, "created_at": c.created_at.isoformat()}


# --- data ingestion wizard: real JDBC (Postgres/MySQL) and REST API connectors ---

JDBC_MAVEN_PACKAGES = {
    "postgres": "org.postgresql:postgresql:42.7.3",
    "mysql": "com.mysql:mysql-connector-j:8.3.0",
}


def _decrypt_source_config(source: models.IngestionSource) -> dict:
    return json.loads(security.decrypt_secret(source.config_encrypted))


@app.get("/api/ingestion/sources", response_model=list[schemas.IngestionSourceOut])
def list_ingestion_sources(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    sources = db.query(models.IngestionSource).order_by(models.IngestionSource.created_at.desc()).all()
    return [{"id": s.id, "name": s.name, "source_type": s.source_type, "created_at": s.created_at.isoformat()} for s in sources]


@app.post("/api/ingestion/sources", response_model=schemas.IngestionSourceOut)
def create_ingestion_source(payload: schemas.IngestionSourceCreate, request: Request, db: Session = Depends(get_db),
                             current: models.User = Depends(security.require_role("editor"))):
    if payload.source_type not in ("postgres", "mysql", "rest_api"):
        raise HTTPException(400, "source_type must be postgres, mysql, or rest_api")
    if db.query(models.IngestionSource).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Source '{payload.name}' already exists")

    config = payload.dict(exclude={"name", "source_type"})
    encrypted = security.encrypt_secret(json.dumps(config))
    s = models.IngestionSource(name=payload.name, source_type=payload.source_type, config_encrypted=encrypted, created_by=current.id)
    db.add(s)
    db.commit()
    _audit(db, current, "ingestion_source.create", resource=payload.name, details={"type": payload.source_type}, request=request)
    return {"id": s.id, "name": s.name, "source_type": s.source_type, "created_at": s.created_at.isoformat()}


@app.delete("/api/ingestion/sources/{source_id}")
def delete_ingestion_source(source_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    s = db.query(models.IngestionSource).get(source_id)
    if s:
        db.delete(s)
        db.commit()
    return {"ok": True}


def _fetch_rest_sample(config: dict, limit: int = 20) -> dict:
    import urllib.request as urlreq
    headers = dict(config.get("headers") or {})
    if config.get("auth_token"):
        headers["Authorization"] = f"Bearer {config['auth_token']}"
    req = urlreq.Request(config["url"], headers=headers, method=config.get("method", "GET"))
    with urlreq.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    records = data
    for part in (config.get("response_path") or "").split("."):
        if part:
            records = records[part]
    records = records[:limit] if isinstance(records, list) else [records]
    columns = sorted({k for r in records for k in (r.keys() if isinstance(r, dict) else [])})
    return {"columns": columns, "rows": records, "row_count": len(records)}


@app.post("/api/ingestion/sources/{source_id}/preview")
def preview_ingestion_source(source_id: str, payload: schemas.IngestionPreviewRequest,
                              db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    from .task_runners import run_spark_submit
    s = db.query(models.IngestionSource).get(source_id)
    if not s:
        raise HTTPException(404, "Source not found")
    config = _decrypt_source_config(s)

    if s.source_type == "rest_api":
        try:
            return _fetch_rest_sample(config)
        except Exception as e:
            raise HTTPException(400, f"Preview failed: {e}")

    if not payload.query:
        raise HTTPException(400, "query (table name or subquery) is required for database sources")
    success, logs, output = run_spark_submit("/jobs/ingest_jdbc.py", {
        "args": ["--driver", s.source_type, "--host", config["host"], "--port", config["port"],
                 "--database", config["database"], "--user", config["username"], "--password", config["password"],
                 "--query", payload.query, "--preview"],
        "extra_packages": [JDBC_MAVEN_PACKAGES[s.source_type]],
        "_timeout": 120,
    })
    if not success:
        raise HTTPException(400, f"Preview failed - see logs: {logs[-2000:]}")
    return output


@app.post("/api/ingestion/sources/{source_id}/ingest", response_model=schemas.TableOut)
def run_ingestion(source_id: str, payload: schemas.IngestionRunRequest, request: Request,
                   db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    from .task_runners import run_spark_submit
    s = db.query(models.IngestionSource).get(source_id)
    if not s:
        raise HTTPException(404, "Source not found")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", payload.table_name):
        raise HTTPException(400, "Table name must start with a letter/underscore and contain only letters, numbers, underscores")
    if db.query(models.Table).filter_by(name=payload.table_name).first():
        raise HTTPException(400, f"Table '{payload.table_name}' already exists")
    config = _decrypt_source_config(s)
    output_path = f"/data/tables/{payload.table_name}"

    if s.source_type == "rest_api":
        try:
            headers = dict(config.get("headers") or {})
            if config.get("auth_token"):
                headers["Authorization"] = f"Bearer {config['auth_token']}"
            import urllib.request as urlreq
            req = urlreq.Request(config["url"], headers=headers, method=config.get("method", "GET"))
            with urlreq.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
            records = data
            for part in (config.get("response_path") or "").split("."):
                if part:
                    records = records[part]
        except Exception as e:
            raise HTTPException(400, f"Fetch failed: {e}")

        os.makedirs("/data/uploads", exist_ok=True)
        jsonl_path = f"/data/uploads/{uuid.uuid4()}.jsonl"
        with open(jsonl_path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        success, logs, output = run_spark_submit("/jobs/onboard_table.py", {
            "args": ["--input", jsonl_path, "--output", output_path, "--format", "json"], "_timeout": 1800,
        })
        try:
            os.remove(jsonl_path)
        except OSError:
            pass
    else:
        if not payload.query:
            raise HTTPException(400, "query (table name or subquery) is required for database sources")
        success, logs, output = run_spark_submit("/jobs/ingest_jdbc.py", {
            "args": ["--driver", s.source_type, "--host", config["host"], "--port", config["port"],
                     "--database", config["database"], "--user", config["username"], "--password", config["password"],
                     "--query", payload.query, "--output", output_path],
            "extra_packages": [JDBC_MAVEN_PACKAGES[s.source_type]],
            "_timeout": 1800,
        })

    if not success:
        raise HTTPException(400, f"Ingestion failed - see logs: {logs[-2000:]}")

    table = models.Table(
        name=payload.table_name, path=output_path, description=f"Ingested from source '{s.name}'",
        row_count=output.get("row_count"), columns=output.get("columns", []),
        created_by=current.id, visibility=payload.visibility, origin="ingestion",
    )
    db.add(table)
    db.commit()
    _audit(db, current, "ingestion.run", resource=payload.table_name, details={"source": s.name}, request=request)
    return table


# --- lakebase: real Postgres-backed transactional (OLTP) tables ---

@app.get("/api/lakebase/tables", response_model=list[schemas.LakebaseTableOut])
def list_lakebase_tables(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    from . import lakebase
    out = []
    for t in db.query(models.LakebaseTable).order_by(models.LakebaseTable.created_at.desc()).all():
        try:
            count = lakebase.row_count(db, t.name)
        except Exception:
            count = 0
        out.append({
            "id": t.id, "name": t.name, "columns": t.columns, "description": t.description,
            "row_count": count, "synced_table_name": t.synced_table_name,
            "last_synced_at": t.last_synced_at.isoformat() if t.last_synced_at else None,
            "created_at": t.created_at.isoformat(),
        })
    return out


@app.post("/api/lakebase/tables", response_model=schemas.LakebaseTableOut)
def create_lakebase_table(payload: schemas.LakebaseTableCreate, request: Request, db: Session = Depends(get_db),
                           current: models.User = Depends(security.require_role("editor"))):
    from . import lakebase
    if db.query(models.LakebaseTable).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Lakebase table '{payload.name}' already exists")
    lakebase.ensure_schema(db)
    try:
        lakebase.create_table(db, payload.name, [c.dict() for c in payload.columns])
    except ValueError as e:
        raise HTTPException(400, str(e))
    t = models.LakebaseTable(name=payload.name, columns=[c.dict() for c in payload.columns],
                              description=payload.description, created_by=current.id)
    db.add(t)
    db.commit()
    _audit(db, current, "lakebase.create_table", resource=payload.name, request=request)
    return {"id": t.id, "name": t.name, "columns": t.columns, "description": t.description,
            "row_count": 0, "synced_table_name": None, "last_synced_at": None, "created_at": t.created_at.isoformat()}


@app.delete("/api/lakebase/tables/{table_id}")
def delete_lakebase_table(table_id: str, request: Request, db: Session = Depends(get_db),
                           current: models.User = Depends(security.require_role("admin"))):
    from . import lakebase
    t = db.query(models.LakebaseTable).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    lakebase.drop_table(db, t.name)
    name = t.name
    db.delete(t)
    db.commit()
    _audit(db, current, "lakebase.delete_table", resource=name, request=request)
    return {"ok": True}


@app.get("/api/lakebase/tables/{table_id}/rows")
def list_lakebase_rows(table_id: str, limit: int = 200, db: Session = Depends(get_db),
                        current: models.User = Depends(security.require_role("viewer"))):
    from . import lakebase
    t = db.query(models.LakebaseTable).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    return lakebase.list_rows(db, t.name, limit)


@app.post("/api/lakebase/tables/{table_id}/rows")
def insert_lakebase_row(table_id: str, payload: schemas.LakebaseRowInsert, db: Session = Depends(get_db),
                         current: models.User = Depends(security.require_role("editor"))):
    from . import lakebase
    t = db.query(models.LakebaseTable).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    try:
        return lakebase.insert_row(db, t.name, payload.values)
    except Exception as e:
        raise HTTPException(400, f"Insert failed: {e}")


@app.delete("/api/lakebase/tables/{table_id}/rows/{row_id}")
def delete_lakebase_row(table_id: str, row_id: int, db: Session = Depends(get_db),
                         current: models.User = Depends(security.require_role("editor"))):
    from . import lakebase
    t = db.query(models.LakebaseTable).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")
    lakebase.delete_row(db, t.name, row_id)
    return {"ok": True}


@app.post("/api/lakebase/tables/{table_id}/sync", response_model=schemas.TableOut)
def sync_lakebase_table(table_id: str, request: Request, db: Session = Depends(get_db),
                         current: models.User = Depends(security.require_role("editor"))):
    """Periodic full-refresh sync into a Delta table for analytics - not true CDC
    streaming (see app/lakebase.py's module docstring for why)."""
    from .task_runners import run_spark_submit
    t = db.query(models.LakebaseTable).get(table_id)
    if not t:
        raise HTTPException(404, "Table not found")

    delta_table_name = t.synced_table_name or f"lakebase_{t.name}"
    output_path = f"/data/tables/{delta_table_name}"
    pg_host = "postgres"  # the Postgres service name in docker-compose
    success, logs, output = run_spark_submit("/jobs/ingest_jdbc.py", {
        "args": ["--driver", "postgres", "--host", pg_host, "--port", "5432",
                 "--database", "orchestrator", "--user", "orchestrator",
                 "--password", os.getenv("POSTGRES_PASSWORD", "orchestrator"),
                 "--query", f'lakebase."{t.name}"', "--output", output_path],
        "extra_packages": [JDBC_MAVEN_PACKAGES["postgres"]],
        "_timeout": 600,
    })
    if not success:
        raise HTTPException(400, f"Sync failed - see logs: {logs[-2000:]}")

    existing_table = db.query(models.Table).filter_by(name=delta_table_name).first()
    if existing_table:
        existing_table.row_count = output.get("row_count")
        existing_table.columns = output.get("columns", [])
    else:
        existing_table = models.Table(
            name=delta_table_name, path=output_path, description=f"Synced from Lakebase table '{t.name}'",
            row_count=output.get("row_count"), columns=output.get("columns", []),
            created_by=current.id, origin="lakebase_sync",
        )
        db.add(existing_table)

    t.synced_table_name = delta_table_name
    t.last_synced_at = datetime.utcnow()
    db.commit()
    _audit(db, current, "lakebase.sync", resource=t.name, details={"synced_to": delta_table_name}, request=request)
    return existing_table


# --- delta sharing: manage shares (normal AtlasFlow auth) ---

@app.get("/api/sharing/shares", response_model=list[schemas.ShareOut])
def list_shares(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    out = []
    for s in db.query(models.Share).order_by(models.Share.created_at.desc()).all():
        out.append({
            "id": s.id, "name": s.name, "description": s.description,
            "table_count": db.query(models.ShareTable).filter_by(share_id=s.id).count(),
            "recipient_count": db.query(models.ShareRecipient).filter_by(share_id=s.id).count(),
            "created_at": s.created_at.isoformat(),
        })
    return out


@app.post("/api/sharing/shares", response_model=schemas.ShareOut)
def create_share(payload: schemas.ShareCreate, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("editor"))):
    if db.query(models.Share).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Share '{payload.name}' already exists")
    s = models.Share(name=payload.name, description=payload.description, created_by=current.id)
    db.add(s)
    db.commit()
    _audit(db, current, "share.create", resource=payload.name, request=request)
    return {"id": s.id, "name": s.name, "description": s.description, "table_count": 0, "recipient_count": 0, "created_at": s.created_at.isoformat()}


@app.delete("/api/sharing/shares/{share_id}")
def delete_share(share_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    db.query(models.ShareTable).filter_by(share_id=share_id).delete()
    db.query(models.ShareRecipient).filter_by(share_id=share_id).delete()
    s = db.query(models.Share).get(share_id)
    if s:
        db.delete(s)
        db.commit()
    return {"ok": True}


@app.get("/api/sharing/shares/{share_id}/tables")
def list_share_tables(share_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = db.query(models.ShareTable).filter_by(share_id=share_id).all()
    out = []
    for r in rows:
        t = db.query(models.Table).get(r.table_id)
        out.append({"id": r.id, "table_name": t.name if t else "?", "shared_name": r.shared_name})
    return out


@app.post("/api/sharing/shares/{share_id}/tables")
def add_share_table(share_id: str, payload: schemas.ShareAddTable, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    table = db.query(models.Table).filter_by(name=payload.table_name).first()
    if not table:
        raise HTTPException(404, f"Table '{payload.table_name}' not found")
    st = models.ShareTable(share_id=share_id, table_id=table.id, shared_name=payload.shared_name or table.name)
    db.add(st)
    db.commit()
    _audit(db, current, "share.add_table", resource=payload.table_name, request=request)
    return {"ok": True}


@app.delete("/api/sharing/shares/tables/{share_table_id}")
def remove_share_table(share_table_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    row = db.query(models.ShareTable).get(share_table_id)
    if row:
        db.delete(row)
        db.commit()
    return {"ok": True}


@app.get("/api/sharing/shares/{share_id}/recipients", response_model=list[schemas.RecipientOut])
def list_recipients(share_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    rows = db.query(models.ShareRecipient).filter_by(share_id=share_id).all()
    return [{"id": r.id, "name": r.name, "bearer_token": None, "created_at": r.created_at.isoformat()} for r in rows]


@app.post("/api/sharing/shares/{share_id}/recipients", response_model=schemas.RecipientOut)
def create_recipient(share_id: str, payload: schemas.RecipientCreate, request: Request, db: Session = Depends(get_db),
                      current: models.User = Depends(security.require_role("editor"))):
    import secrets as secrets_mod
    token = secrets_mod.token_urlsafe(32)
    r = models.ShareRecipient(share_id=share_id, name=payload.name, bearer_token=token, created_by=current.id)
    db.add(r)
    db.commit()
    _audit(db, current, "share.add_recipient", resource=payload.name, request=request)
    return {"id": r.id, "name": r.name, "bearer_token": token, "created_at": r.created_at.isoformat()}


@app.get("/api/sharing/shares/{share_id}/recipients/{recipient_id}/profile")
def get_recipient_profile(share_id: str, recipient_id: str, db: Session = Depends(get_db),
                           current: models.User = Depends(security.require_role("editor"))):
    """Regenerates a fresh bearer token and returns a Delta Sharing 'profile file' - the
    exact JSON format the real `delta-sharing` client library expects
    (delta_sharing.SharingClient('/path/to/this.json'))."""
    import secrets as secrets_mod
    r = db.query(models.ShareRecipient).get(recipient_id)
    if not r:
        raise HTTPException(404, "Recipient not found")
    r.bearer_token = secrets_mod.token_urlsafe(32)
    db.commit()
    base_url = os.getenv("ATLASFLOW_SHARING_BASE_URL", "http://localhost:8000")
    return {
        "shareCredentialsVersion": 1,
        "endpoint": f"{base_url}/delta-sharing",
        "bearerToken": r.bearer_token,
    }


@app.delete("/api/sharing/shares/recipients/{recipient_id}")
def delete_recipient(recipient_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    r = db.query(models.ShareRecipient).get(recipient_id)
    if r:
        db.delete(r)
        db.commit()
    return {"ok": True}


# --- delta sharing: the real, open protocol server - authenticated via recipient bearer tokens ---

def _require_share_recipient(authorization: str = None, db: Session = None) -> models.ShareRecipient:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing bearer token")
    token = authorization[len("Bearer "):]
    r = db.query(models.ShareRecipient).filter_by(bearer_token=token).first()
    if not r:
        raise HTTPException(401, "Invalid bearer token")
    return r


@app.get("/delta-sharing/shares")
def ds_list_shares(request: Request, db: Session = Depends(get_db)):
    recipient = _require_share_recipient(request.headers.get("authorization"), db)
    share = db.query(models.Share).get(recipient.share_id)
    return {"items": [{"name": share.name}] if share else [], "nextPageToken": None}


@app.get("/delta-sharing/shares/{share}/schemas")
def ds_list_schemas(share: str, request: Request, db: Session = Depends(get_db)):
    recipient = _require_share_recipient(request.headers.get("authorization"), db)
    s = db.query(models.Share).get(recipient.share_id)
    if not s or s.name != share:
        raise HTTPException(404, "Share not found")
    return {"items": [{"name": "default", "share": share}], "nextPageToken": None}


@app.get("/delta-sharing/shares/{share}/schemas/{schema}/tables")
def ds_list_tables(share: str, schema: str, request: Request, db: Session = Depends(get_db)):
    recipient = _require_share_recipient(request.headers.get("authorization"), db)
    s = db.query(models.Share).get(recipient.share_id)
    if not s or s.name != share:
        raise HTTPException(404, "Share not found")
    rows = db.query(models.ShareTable).filter_by(share_id=s.id).all()
    return {"items": [{"name": r.shared_name, "schema": "default", "share": share} for r in rows], "nextPageToken": None}


def _get_shared_table_or_404(db: Session, recipient: models.ShareRecipient, share: str, table: str):
    s = db.query(models.Share).get(recipient.share_id)
    if not s or s.name != share:
        raise HTTPException(404, "Share not found")
    st = db.query(models.ShareTable).filter_by(share_id=s.id, shared_name=table).first()
    if not st:
        raise HTTPException(404, "Table not found in this share")
    return db.query(models.Table).get(st.table_id)


@app.get("/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/metadata")
def ds_table_metadata(share: str, schema: str, table: str, request: Request, db: Session = Depends(get_db)):
    from . import sharing
    from fastapi.responses import Response
    recipient = _require_share_recipient(request.headers.get("authorization"), db)
    catalog_table = _get_shared_table_or_404(db, recipient, share, table)
    snap = sharing.table_snapshot(catalog_table.path)
    lines = [
        json.dumps({"protocol": {"minReaderVersion": 1}}),
        json.dumps({"metaData": {"id": catalog_table.id, "format": {"provider": "parquet"},
                                  "schemaString": snap["schema_string"], "partitionColumns": []}}),
    ]
    return Response(content="\n".join(lines) + "\n", media_type="application/x-ndjson")


@app.post("/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/query")
def ds_query_table(share: str, schema: str, table: str, request: Request, db: Session = Depends(get_db)):
    from . import sharing
    from fastapi.responses import Response
    recipient = _require_share_recipient(request.headers.get("authorization"), db)
    catalog_table = _get_shared_table_or_404(db, recipient, share, table)
    snap = sharing.table_snapshot(catalog_table.path)
    base_url = os.getenv("ATLASFLOW_SHARING_BASE_URL", "http://localhost:8000")

    lines = [
        json.dumps({"protocol": {"minReaderVersion": 1}}),
        json.dumps({"metaData": {"id": catalog_table.id, "format": {"provider": "parquet"},
                                  "schemaString": snap["schema_string"], "partitionColumns": [], "size": sum(f["size"] for f in snap["files"])}}),
    ]
    for f in snap["files"]:
        url = sharing.build_file_url(base_url, catalog_table.path, f["path"])
        lines.append(json.dumps({"file": {"url": url, "id": f["path"], "partitionValues": {}, "size": f["size"]}}))
    return Response(content="\n".join(lines) + "\n", media_type="application/x-ndjson")


@app.get("/delta-sharing/files/{token}")
def ds_serve_file(token: str):
    from . import sharing
    from fastapi.responses import FileResponse
    try:
        payload = sharing.verify_file_token(token)
    except Exception:
        raise HTTPException(401, "Invalid or expired file token")
    full_path = os.path.join(payload["table_path"], payload["relative_path"])
    if not os.path.isfile(full_path):
        raise HTTPException(404, "File not found")
    return FileResponse(full_path)


# --- cluster policies: governed limits a workflow's tasks must stay within ---

@app.get("/api/cluster-policies", response_model=list[schemas.ClusterPolicyOut])
def list_cluster_policies(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = db.query(models.ClusterPolicy).order_by(models.ClusterPolicy.created_at.desc()).all()
    return [{
        "id": p.id, "name": p.name, "description": p.description,
        "max_timeout_seconds": p.max_timeout_seconds, "max_retries": p.max_retries,
        "allowed_task_types": p.allowed_task_types, "created_at": p.created_at.isoformat(),
    } for p in rows]


@app.post("/api/cluster-policies", response_model=schemas.ClusterPolicyOut)
def create_cluster_policy(payload: schemas.ClusterPolicyCreate, request: Request, db: Session = Depends(get_db),
                           current: models.User = Depends(security.require_role("admin"))):
    if db.query(models.ClusterPolicy).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Policy '{payload.name}' already exists")
    p = models.ClusterPolicy(
        name=payload.name, description=payload.description, max_timeout_seconds=payload.max_timeout_seconds,
        max_retries=payload.max_retries, allowed_task_types=payload.allowed_task_types, created_by=current.id,
    )
    db.add(p)
    db.commit()
    _audit(db, current, "cluster_policy.create", resource=payload.name, request=request)
    return {"id": p.id, "name": p.name, "description": p.description, "max_timeout_seconds": p.max_timeout_seconds,
            "max_retries": p.max_retries, "allowed_task_types": p.allowed_task_types, "created_at": p.created_at.isoformat()}


@app.delete("/api/cluster-policies/{policy_id}")
def delete_cluster_policy(policy_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    p = db.query(models.ClusterPolicy).get(policy_id)
    if p:
        db.query(models.Workflow).filter_by(cluster_policy_id=p.id).update({"cluster_policy_id": None})
        db.delete(p)
        db.commit()
    return {"ok": True}


# --- pipelines: declarative tables with auto-inferred dependency order ---

def _serialize_pipeline(p: models.Pipeline, db: Session) -> dict:
    tables = db.query(models.PipelineTable).filter_by(pipeline_id=p.id).all()
    return {
        "id": p.id, "name": p.name, "description": p.description, "schedule_cron": p.schedule_cron,
        "is_paused": p.is_paused, "last_run_status": p.last_run_status,
        "last_run_at": p.last_run_at.isoformat() if p.last_run_at else None,
        "tables": [{"id": t.id, "table_name": t.table_name, "sql": t.sql, "last_status": t.last_status,
                    "last_error": t.last_error, "row_count": t.row_count} for t in tables],
    }


@app.get("/api/pipelines", response_model=list[schemas.PipelineOut])
def list_pipelines(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    return [_serialize_pipeline(p, db) for p in db.query(models.Pipeline).order_by(models.Pipeline.created_at.desc()).all()]


@app.post("/api/pipelines", response_model=schemas.PipelineOut)
def create_pipeline(payload: schemas.PipelineCreate, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    from . import pipelines as pl
    if db.query(models.Pipeline).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Pipeline '{payload.name}' already exists")
    table_names = [t.table_name for t in payload.tables]
    if len(table_names) != len(set(table_names)):
        raise HTTPException(400, "Declared table names must be unique within a pipeline")
    for name in table_names:
        if db.query(models.Table).filter_by(name=name).first():
            raise HTTPException(400, f"Table name '{name}' is already used in the catalog")

    p = models.Pipeline(name=payload.name, description=payload.description, schedule_cron=payload.schedule_cron, created_by=current.id)
    db.add(p)
    db.flush()
    rows = [models.PipelineTable(pipeline_id=p.id, table_name=t.table_name, sql=t.sql) for t in payload.tables]
    db.add_all(rows)
    db.commit()

    try:
        pl.topological_order(rows)  # validate no circular dependency up front
    except ValueError as e:
        db.delete(p)
        db.commit()
        raise HTTPException(400, str(e))

    scheduler.sync_jobs()
    _audit(db, current, "pipeline.create", resource=payload.name, request=request)
    return _serialize_pipeline(p, db)


@app.delete("/api/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    db.query(models.PipelineTable).filter_by(pipeline_id=pipeline_id).delete()
    p = db.query(models.Pipeline).get(pipeline_id)
    if p:
        db.delete(p)
        db.commit()
    scheduler.sync_jobs()
    return {"ok": True}


@app.post("/api/pipelines/{pipeline_id}/pause")
def pause_pipeline(pipeline_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("editor"))):
    p = db.query(models.Pipeline).get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    p.is_paused = not p.is_paused
    db.commit()
    scheduler.sync_jobs()
    return _serialize_pipeline(p, db)


def run_pipeline_now(db: Session, pipeline: models.Pipeline):
    from . import pipelines as pl
    from .catalog import resolve_table_names
    from .task_runners import run_spark_submit
    import tempfile as tempfile_mod

    tables = db.query(models.PipelineTable).filter_by(pipeline_id=pipeline.id).all()
    try:
        ordered = pl.topological_order(tables)
    except ValueError:
        pipeline.last_run_status = "error"
        pipeline.last_run_at = datetime.utcnow()
        db.commit()
        return

    any_failed = False
    for t in ordered:
        resolved_sql = resolve_table_names(t.sql, db)
        output_path = f"/data/tables/{t.table_name}"
        with tempfile_mod.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, dir="/jobs") as f:
            f.write(resolved_sql)
            sql_path = f.name
        success, logs, output = run_spark_submit("/jobs/materialize_table.py", {
            "args": ["--sql-file", sql_path, "--output", output_path], "_timeout": 1800,
        })
        try:
            os.remove(sql_path)
        except OSError:
            pass

        if success:
            t.last_status = "ok"
            t.last_error = None
            t.row_count = output.get("row_count")
            existing = db.query(models.Table).filter_by(name=t.table_name).first()
            if existing:
                existing.row_count = output.get("row_count")
                existing.columns = output.get("columns", [])
            else:
                db.add(models.Table(
                    name=t.table_name, path=output_path, description=f"Declared in pipeline '{pipeline.name}'",
                    row_count=output.get("row_count"), columns=output.get("columns", []),
                    created_by=pipeline.created_by, origin="pipeline",
                ))
        else:
            t.last_status = "error"
            t.last_error = logs[-1000:]
            any_failed = True
        db.commit()

    pipeline.last_run_status = "error" if any_failed else "ok"
    pipeline.last_run_at = datetime.utcnow()
    db.commit()


@app.post("/api/pipelines/{pipeline_id}/run", response_model=schemas.PipelineOut)
def trigger_pipeline(pipeline_id: str, request: Request, db: Session = Depends(get_db),
                      current: models.User = Depends(security.require_role("editor"))):
    p = db.query(models.Pipeline).get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    run_pipeline_now(db, p)
    _audit(db, current, "pipeline.run", resource=p.name, details={"status": p.last_run_status}, request=request)
    return _serialize_pipeline(p, db)


# --- AI Gateway: unified LLM routing, rate limiting, and logging ---

@app.get("/api/ai-gateway/routes", response_model=list[schemas.AIGatewayRouteOut])
def list_ai_routes(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    rows = db.query(models.AIGatewayRoute).order_by(models.AIGatewayRoute.created_at.desc()).all()
    return [{
        "id": r.id, "name": r.name, "provider": r.provider, "model": r.model,
        "api_key_secret_name": r.api_key_secret_name, "requests_per_minute": r.requests_per_minute,
        "is_enabled": r.is_enabled, "created_at": r.created_at.isoformat(),
    } for r in rows]


@app.post("/api/ai-gateway/routes", response_model=schemas.AIGatewayRouteOut)
def create_ai_route(payload: schemas.AIGatewayRouteCreate, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("admin"))):
    if payload.provider not in ("anthropic", "openai"):
        raise HTTPException(400, "provider must be 'anthropic' or 'openai'")
    if db.query(models.AIGatewayRoute).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Route '{payload.name}' already exists")
    if not db.query(models.Secret).filter_by(name=payload.api_key_secret_name).first():
        raise HTTPException(400, f"Secret '{payload.api_key_secret_name}' not found - add it in the Secrets tab first")
    r = models.AIGatewayRoute(
        name=payload.name, provider=payload.provider, model=payload.model,
        api_key_secret_name=payload.api_key_secret_name, requests_per_minute=payload.requests_per_minute,
        created_by=current.id,
    )
    db.add(r)
    db.commit()
    _audit(db, current, "ai_gateway.create_route", resource=payload.name, request=request)
    return {"id": r.id, "name": r.name, "provider": r.provider, "model": r.model,
            "api_key_secret_name": r.api_key_secret_name, "requests_per_minute": r.requests_per_minute,
            "is_enabled": r.is_enabled, "created_at": r.created_at.isoformat()}


@app.post("/api/ai-gateway/routes/{route_id}/toggle")
def toggle_ai_route(route_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    r = db.query(models.AIGatewayRoute).get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")
    r.is_enabled = not r.is_enabled
    db.commit()
    return {"ok": True, "is_enabled": r.is_enabled}


@app.delete("/api/ai-gateway/routes/{route_id}")
def delete_ai_route(route_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    r = db.query(models.AIGatewayRoute).get(route_id)
    if r:
        db.delete(r)
        db.commit()
    return {"ok": True}


@app.post("/api/ai-gateway/chat", response_model=schemas.AIGatewayChatResponse)
def ai_gateway_chat(payload: schemas.AIGatewayChatRequest, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    from . import ai_gateway
    import time as time_mod

    route = db.query(models.AIGatewayRoute).filter_by(name=payload.route_name).first()
    if not route:
        raise HTTPException(404, f"Route '{payload.route_name}' not found")

    start = time_mod.time()
    log = models.AIGatewayLog(route_id=route.id, user_id=current.id, username=current.username)
    try:
        content = ai_gateway.call_route(db, route, payload.messages, payload.system)
        log.success = True
        log.latency_ms = int((time_mod.time() - start) * 1000)
        db.add(log)
        db.commit()
        return {"content": content}
    except Exception as e:
        log.success = False
        log.error = str(e)
        log.latency_ms = int((time_mod.time() - start) * 1000)
        db.add(log)
        db.commit()
        raise HTTPException(400, str(e))


@app.get("/api/ai-gateway/logs", response_model=list[schemas.AIGatewayLogOut])
def list_ai_gateway_logs(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    rows = db.query(models.AIGatewayLog).order_by(models.AIGatewayLog.created_at.desc()).limit(100).all()
    return [{
        "id": l.id, "route_id": l.route_id, "username": l.username, "success": l.success,
        "latency_ms": l.latency_ms, "error": l.error, "created_at": l.created_at.isoformat(),
    } for l in rows]


# --- git repos: real clone/pull/commit/push, AtlasFlow's Repos/Git folders equivalent ---

def _serialize_git_repo(r: models.GitRepo) -> dict:
    return {
        "id": r.id, "name": r.name, "remote_url": r.remote_url, "branch": r.branch,
        "last_synced_at": r.last_synced_at.isoformat() if r.last_synced_at else None,
        "last_sync_status": r.last_sync_status, "last_sync_error": r.last_sync_error,
        "created_at": r.created_at.isoformat(),
    }


@app.get("/api/git-repos", response_model=list[schemas.GitRepoOut])
def list_git_repos(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    return [_serialize_git_repo(r) for r in db.query(models.GitRepo).order_by(models.GitRepo.created_at.desc()).all()]


@app.post("/api/git-repos", response_model=schemas.GitRepoOut)
def create_git_repo(payload: schemas.GitRepoCreate, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    from . import git_repos
    if not re.match(r"^[a-zA-Z0-9_\-]+$", payload.name):
        raise HTTPException(400, "Repo name must contain only letters, numbers, underscores, hyphens")
    if db.query(models.GitRepo).filter_by(name=payload.name).first():
        raise HTTPException(400, f"Repo '{payload.name}' already exists")

    token = None
    if payload.pat_secret_name:
        secret = db.query(models.Secret).filter_by(name=payload.pat_secret_name).first()
        if not secret:
            raise HTTPException(400, f"Secret '{payload.pat_secret_name}' not found")
        token = security.decrypt_secret(secret.encrypted_value)

    ok, log = git_repos.clone_or_pull(payload.name, payload.remote_url, payload.branch, token)
    r = models.GitRepo(
        name=payload.name, remote_url=payload.remote_url, branch=payload.branch,
        pat_secret_name=payload.pat_secret_name, created_by=current.id,
        last_synced_at=datetime.utcnow(), last_sync_status="ok" if ok else "error",
        last_sync_error=None if ok else log[-1000:],
    )
    db.add(r)
    db.commit()
    _audit(db, current, "git_repo.create", resource=payload.name, details={"ok": ok}, request=request)
    if not ok:
        raise HTTPException(400, f"Clone failed - see logs: {log[-1000:]}")
    return _serialize_git_repo(r)


@app.post("/api/git-repos/{repo_id}/sync", response_model=schemas.GitRepoOut)
def sync_git_repo(repo_id: str, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("editor"))):
    from . import git_repos
    r = db.query(models.GitRepo).get(repo_id)
    if not r:
        raise HTTPException(404, "Repo not found")
    token = None
    if r.pat_secret_name:
        secret = db.query(models.Secret).filter_by(name=r.pat_secret_name).first()
        token = security.decrypt_secret(secret.encrypted_value) if secret else None
    ok, log = git_repos.clone_or_pull(r.name, r.remote_url, r.branch, token)
    r.last_synced_at = datetime.utcnow()
    r.last_sync_status = "ok" if ok else "error"
    r.last_sync_error = None if ok else log[-1000:]
    db.commit()
    _audit(db, current, "git_repo.sync", resource=r.name, details={"ok": ok}, request=request)
    return _serialize_git_repo(r)


@app.get("/api/git-repos/{repo_id}/status")
def git_repo_status(repo_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    from . import git_repos
    r = db.query(models.GitRepo).get(repo_id)
    if not r:
        raise HTTPException(404, "Repo not found")
    return git_repos.status(r.name)


@app.post("/api/git-repos/{repo_id}/commit")
def commit_git_repo(repo_id: str, payload: schemas.GitCommitRequest, request: Request, db: Session = Depends(get_db),
                     current: models.User = Depends(security.require_role("editor"))):
    from . import git_repos
    r = db.query(models.GitRepo).get(repo_id)
    if not r:
        raise HTTPException(404, "Repo not found")
    token = None
    if r.pat_secret_name:
        secret = db.query(models.Secret).filter_by(name=r.pat_secret_name).first()
        token = security.decrypt_secret(secret.encrypted_value) if secret else None
    ok, log = git_repos.commit_and_push(r.name, payload.message, r.remote_url, r.branch, token)
    _audit(db, current, "git_repo.commit", resource=r.name, details={"ok": ok, "message": payload.message}, request=request)
    if not ok:
        raise HTTPException(400, f"Commit/push failed - see logs: {log[-1000:]}")
    return {"ok": True, "log": log[-1000:]}


@app.delete("/api/git-repos/{repo_id}")
def delete_git_repo(repo_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    r = db.query(models.GitRepo).get(repo_id)
    if r:
        db.delete(r)
        db.commit()
    return {"ok": True}


# --- personal access tokens: fine-grained, scoped API tokens ---

@app.get("/api/auth/tokens", response_model=list[schemas.PATOut])
def list_my_tokens(db: Session = Depends(get_db), current: models.User = Depends(security.get_current_user)):
    rows = db.query(models.PersonalAccessToken).filter_by(user_id=current.id).order_by(models.PersonalAccessToken.created_at.desc()).all()
    return [{
        "id": t.id, "name": t.name, "scopes": t.scopes, "token": None,
        "expires_at": t.expires_at.isoformat() if t.expires_at else None,
        "last_used_at": t.last_used_at.isoformat() if t.last_used_at else None,
        "created_at": t.created_at.isoformat(),
    } for t in rows]


@app.get("/api/auth/scopes")
def list_available_scopes(current: models.User = Depends(security.get_current_user)):
    from . import scopes
    return {"scopes": list(scopes.SCOPE_CATALOG.keys())}


@app.post("/api/auth/tokens", response_model=schemas.PATOut)
def create_pat(payload: schemas.PATCreate, request: Request, db: Session = Depends(get_db),
                current: models.User = Depends(security.get_current_user)):
    from . import scopes
    for s in payload.scopes:
        if s not in scopes.SCOPE_CATALOG:
            raise HTTPException(400, f"Unknown scope '{s}' - see GET /api/auth/scopes")
    plaintext = security.generate_pat()
    expires_at = datetime.utcnow() + timedelta(days=payload.expires_in_days) if payload.expires_in_days else None
    t = models.PersonalAccessToken(
        user_id=current.id, name=payload.name, token_hash=security.hash_pat(plaintext),
        scopes=payload.scopes, expires_at=expires_at,
    )
    db.add(t)
    db.commit()
    _audit(db, current, "pat.create", resource=payload.name, details={"scopes": payload.scopes}, request=request)
    return {"id": t.id, "name": t.name, "scopes": t.scopes, "token": plaintext,
            "expires_at": expires_at.isoformat() if expires_at else None, "last_used_at": None,
            "created_at": t.created_at.isoformat()}


@app.delete("/api/auth/tokens/{token_id}")
def revoke_pat(token_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.get_current_user)):
    t = db.query(models.PersonalAccessToken).get(token_id)
    if t and t.user_id == current.id:
        db.delete(t)
        db.commit()
    return {"ok": True}


# --- feature store: tables tagged as feature tables, plus point-in-time join helper ---

@app.get("/api/feature-tables", response_model=list[schemas.TableOut])
def list_feature_tables(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    tables = db.query(models.Table).filter_by(is_feature_table=True).all()
    return [t for t in tables if _can_read_table(t, current, db)]


# --- model serving: deploy an MLflow-registered model as a live prediction endpoint ---

@app.get("/api/models/deployments", response_model=list[schemas.ModelDeploymentOut])
def list_deployments(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    return db.query(models.ModelDeployment).all()


@app.post("/api/models/deploy", response_model=schemas.ModelDeploymentOut)
def deploy_model(payload: schemas.ModelDeployRequest, request: Request, db: Session = Depends(get_db),
                  current: models.User = Depends(security.require_role("editor"))):
    """Runs `mlflow models serve` as a background process and proxies predictions to it.
    Single-instance, no rolling updates or replicas - the honest limit of serving this way
    without a container orchestrator in front of it."""
    import subprocess, socket

    if db.query(models.ModelDeployment).filter_by(model_name=payload.model_name).first():
        raise HTTPException(400, f"Model '{payload.model_name}' is already deployed - stop it first")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    model_uri = f"models:/{payload.model_name}/{payload.model_version}"
    proc = subprocess.Popen(
        ["mlflow", "models", "serve", "-m", model_uri, "-p", str(port), "-h", "0.0.0.0", "--env-manager", "local"],
        env=os.environ.copy(),
    )
    deployment = models.ModelDeployment(
        model_name=payload.model_name, model_version=payload.model_version,
        port=port, pid=proc.pid, status="running", created_by=current.id,
    )
    db.add(deployment)
    db.commit()
    _audit(db, current, "model.deploy", resource=payload.model_name, details={"version": payload.model_version, "port": port}, request=request)
    return deployment


@app.post("/api/models/{deployment_id}/predict")
def predict(deployment_id: str, payload: dict, db: Session = Depends(get_db),
            current: models.User = Depends(security.require_role("viewer"))):
    import urllib.request as urlreq
    d = db.query(models.ModelDeployment).get(deployment_id)
    if not d or d.status != "running":
        raise HTTPException(404, "No running deployment with that id")
    req = urlreq.Request(f"http://localhost:{d.port}/invocations", data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlreq.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        raise HTTPException(502, f"Model server error: {e}")


@app.delete("/api/models/deployments/{deployment_id}")
def stop_deployment(deployment_id: str, db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    import signal
    d = db.query(models.ModelDeployment).get(deployment_id)
    if not d:
        raise HTTPException(404, "Deployment not found")
    if d.pid:
        try:
            os.kill(d.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    db.delete(d)
    db.commit()
    return {"ok": True}


# --- cluster: live, read-only Spark status (honest alternative to fake autoscaling) ---

@app.get("/api/cluster/status")
def cluster_status(current: models.User = Depends(security.require_role("viewer"))):
    """Live status from the real Spark master REST API - worker count, cores, memory,
    running/completed applications. This is read-only visibility, not autoscaling: true
    elastic autoscaling needs a container orchestrator (Kubernetes) or cloud provider APIs
    controlling the infrastructure underneath, which a docker-compose stack doesn't have.
    To add/remove capacity today: `docker compose up --scale spark-worker-1=N`."""
    import urllib.request as urlreq
    try:
        with urlreq.urlopen("http://spark-master:8080/json/", timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return {
            "reachable": True,
            "status": data.get("status"),
            "workers": [{
                "id": w.get("id"), "state": w.get("state"),
                "cores": w.get("cores"), "coresused": w.get("coresused"),
                "memory": w.get("memory"), "memoryused": w.get("memoryused"),
            } for w in data.get("workers", [])],
            "cores_total": data.get("cores"), "cores_used": data.get("coresused"),
            "memory_total_mb": data.get("memory"), "memory_used_mb": data.get("memoryused"),
            "running_apps": len(data.get("activeapps", [])),
            "completed_apps": len(data.get("completedapps", [])),
        }
    except Exception as e:
        return {"reachable": False, "error": str(e)}


@app.get("/api/cluster/autoscaler/status")
def autoscaler_status(current: models.User = Depends(security.require_role("viewer"))):
    from . import autoscaler
    return autoscaler.status()


# --- billing: real Stripe metered billing, plus usage reports (no payment required for reports) ---

@app.get("/api/billing/config", response_model=schemas.BillingConfigOut)
def billing_config():
    from . import billing
    return {"stripe_enabled": billing.is_enabled(), "scim_enabled": scim.is_enabled()}


@app.post("/api/billing/checkout", response_model=schemas.CheckoutOut)
def billing_checkout(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    from . import billing
    if not billing.is_enabled():
        raise HTTPException(404, "Billing is not configured")
    url = billing.create_checkout_session(current)
    db.commit()  # persists stripe_customer_id if create_checkout_session set it
    _audit(db, current, "billing.checkout_started")
    return {"url": url}


@app.post("/api/billing/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    from . import billing
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        event = billing.verify_webhook(payload, sig_header)
    except Exception as e:
        raise HTTPException(400, f"Invalid webhook signature: {e}")

    if event["type"] == "checkout.session.completed":
        billing.handle_checkout_completed(event["data"]["object"], db)
    return {"received": True}


@app.get("/api/billing/usage-report")
def usage_report(start: str = None, end: str = None, group_by: str = "user", format: str = "json",
                  db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    """Aggregates estimated compute cost across runs. `group_by` is 'user' or 'workflow'.
    This is a usage estimate (see COST_PER_WORKER_HOUR), not a Stripe invoice - for that,
    see the Stripe Dashboard once billing is configured."""
    import csv
    import io as io_mod

    query = db.query(models.UsageRecord)
    if start:
        query = query.filter(models.UsageRecord.created_at >= datetime.fromisoformat(start))
    if end:
        query = query.filter(models.UsageRecord.created_at <= datetime.fromisoformat(end))
    records = query.all()

    totals = {}
    counts = {}
    for r in records:
        if group_by == "workflow":
            wf = db.query(models.Workflow).get(r.workflow_id)
            key = wf.name if wf else r.workflow_id
        else:
            user = db.query(models.User).get(r.user_id) if r.user_id else None
            key = user.username if user else "unattributed"
        totals[key] = totals.get(key, 0) + float(r.cost_usd)
        counts[key] = counts.get(key, 0) + 1

    rows = [{"group": k, "total_cost_usd": round(v, 4), "run_count": counts[k]} for k, v in totals.items()]
    rows.sort(key=lambda r: -r["total_cost_usd"])

    if format == "csv":
        from fastapi.responses import Response
        buf = io_mod.StringIO()
        writer = csv.DictWriter(buf, fieldnames=["group", "total_cost_usd", "run_count"])
        writer.writeheader()
        writer.writerows(rows)
        return Response(content=buf.getvalue(), media_type="text/csv",
                         headers={"Content-Disposition": "attachment; filename=usage_report.csv"})
    return rows


# --- ad-hoc Python: quick, in-process analysis (pandas/deltalake preinstalled) ---

@app.post("/api/python/run", response_model=schemas.PythonRunResult)
def run_python_adhoc(payload: schemas.PythonRunRequest, request: Request, db: Session = Depends(get_db),
                      current: models.User = Depends(security.require_role("editor"))):
    from .task_runners import run_python
    tables = {t.name: t.path for t in db.query(models.Table).all() if _can_read_table(t, current, db)}
    success, logs, output = run_python(payload.code, {"_timeout": 120, "tables": tables})
    _audit(db, current, "python.run", details={"code": payload.code[:500]}, request=request)
    return {"success": success, "logs": logs, "output": output}


# --- ad-hoc SQL: run a query against the lakehouse (Delta tables) and see results ---

@app.post("/api/sql/query", response_model=schemas.SqlQueryResult)
def run_sql_query(payload: schemas.SqlQueryRequest, request: Request, db: Session = Depends(get_db),
                   current: models.User = Depends(security.require_role("editor"))):
    from .task_runners import run_sql
    from .catalog import resolve_table_names, referenced_table_names, substitute_params
    sql_with_params = substitute_params(payload.sql, payload.params)
    for name in referenced_table_names(sql_with_params, db):
        _require_table_access(name, current, db)
    resolved_sql = resolve_table_names(sql_with_params, db, current.role)
    success, logs, output = run_sql(resolved_sql, {"limit": payload.limit, "_timeout": 300})
    _audit(db, current, "sql.query", details={"sql": payload.sql[:500], "params": payload.params}, request=request)
    if not success:
        raise HTTPException(400, f"Query failed - see logs: {logs[-2000:]}")
    return {
        "columns": output.get("columns", []),
        "rows": output.get("rows", []),
        "row_count": output.get("row_count", 0),
        "logs": logs[-4000:],
    }


@app.get("/api/sql/history")
def sql_history(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("viewer"))):
    logs = (
        db.query(models.AuditLog).filter_by(action="sql.query")
        .order_by(models.AuditLog.timestamp.desc()).limit(50).all()
    )
    return [{
        "id": l.id, "username": l.username, "sql": (l.details or {}).get("sql", ""),
        "timestamp": l.timestamp.isoformat(),
    } for l in logs]


# --- audit log: admin only ---

@app.get("/api/audit-logs", response_model=list[schemas.AuditLogOut])
def list_audit_logs(db: Session = Depends(get_db), current: models.User = Depends(security.require_role("admin"))):
    logs = db.query(models.AuditLog).order_by(models.AuditLog.timestamp.desc()).limit(200).all()
    return [{
        "id": l.id, "username": l.username, "action": l.action, "resource": l.resource,
        "details": l.details or {}, "timestamp": l.timestamp.isoformat(),
    } for l in logs]


@app.get("/api/health")
def health():
    return {"status": "ok"}
