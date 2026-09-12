"""
DAG execution engine.

Given a Workflow (a set of Tasks with `depends_on` edges), this:
  1. Validates the graph is acyclic
  2. Runs tasks in dependency order, running independent tasks concurrently
  3. Propagates failure: if a task fails, everything downstream of it is
     marked upstream_failed and skipped (same behavior as Databricks Jobs)
  4. Evaluates each task's optional `run_if` expression against upstream task values/status
     before running it - a falsy result marks it (and everything downstream) skipped,
     mirroring Databricks' conditional/"Run if" task feature
  5. Passes task values downstream ({{task.key.field}} placeholders, and the `upstream`
     dict available directly to python/shell/spark runners) - Databricks calls this
     "task values"
  6. Retries a failed task up to `task.retries` times before giving up
  7. Persists status + logs + output for every task run so the UI can poll it
"""
import time
import json
import urllib.request
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from sqlalchemy.orm import Session

from . import models
from .task_runners import RUNNERS
from .secrets_resolver import resolve_task, redact
from .catalog import resolve_table_names


class CycleError(Exception):
    pass


def topological_layers(tasks: list[models.Task]) -> list[list[models.Task]]:
    """Groups tasks into layers that can run in parallel, in dependency order."""
    by_key = {t.key: t for t in tasks}
    indegree = {t.key: len(t.depends_on or []) for t in tasks}
    dependents = defaultdict(list)
    for t in tasks:
        for dep in (t.depends_on or []):
            if dep not in by_key:
                raise ValueError(f"Task '{t.key}' depends on unknown task '{dep}'")
            dependents[dep].append(t.key)

    layers = []
    frontier = deque([k for k, d in indegree.items() if d == 0])
    seen = 0
    remaining_indegree = dict(indegree)
    while frontier:
        layer_keys = list(frontier)
        frontier.clear()
        layers.append([by_key[k] for k in layer_keys])
        seen += len(layer_keys)
        for k in layer_keys:
            for dep_key in dependents[k]:
                remaining_indegree[dep_key] -= 1
                if remaining_indegree[dep_key] == 0:
                    frontier.append(dep_key)

    if seen != len(tasks):
        raise CycleError("Workflow graph has a cycle - check depends_on fields")
    return layers


def _evaluate_run_if(expression: str, upstream_outputs: dict, upstream_status: dict) -> bool:
    """Restricted eval - the expression only ever sees `upstream` (task values) and
    `upstream_status` (dict of task_key -> status string) for this task's direct
    dependencies. No builtins are exposed, so this can't do file/network/system access."""
    try:
        return bool(eval(
            expression,
            {"__builtins__": {}},
            {"upstream": upstream_outputs, "upstream_status": upstream_status},
        ))
    except Exception:
        # a broken condition fails closed (skips the task) rather than crashing the run
        return False


def execute_workflow_run(db: Session, workflow_run_id: str):
    run = db.query(models.WorkflowRun).get(workflow_run_id)
    workflow = run.workflow
    tasks = workflow.tasks

    run.status = models.RunStatus.RUNNING
    db.commit()

    # create a pending TaskRun row per task up front
    task_runs_by_key = {}
    for t in tasks:
        tr = models.TaskRun(workflow_run_id=run.id, task_key=t.key, status=models.RunStatus.PENDING)
        db.add(tr)
        task_runs_by_key[t.key] = tr
    db.commit()

    try:
        layers = topological_layers(tasks)
    except CycleError:
        run.status = models.RunStatus.FAILED
        run.finished_at = datetime.utcnow()
        db.commit()
        return

    failed_keys = set()   # failed, or upstream_failed
    skipped_keys = set()  # run_if evaluated falsy, or upstream was skipped
    blocking_keys = set()  # union of the above - anything that stops a downstream task running

    for layer in layers:
        # tasks in a layer have no dependency on each other -> evaluated/run concurrently
        runnable = [t for t in layer if not any(dep in blocking_keys for dep in (t.depends_on or []))]
        blocked = [t for t in layer if t not in runnable]

        for t in blocked:
            tr = task_runs_by_key[t.key]
            # a task blocked by a skip (not a failure) is itself just skipped, not "upstream_failed"
            if any(dep in failed_keys for dep in (t.depends_on or [])):
                tr.status = models.RunStatus.UPSTREAM_FAILED
                failed_keys.add(t.key)
            else:
                tr.status = models.RunStatus.SKIPPED
                skipped_keys.add(t.key)
            blocking_keys.add(t.key)
        db.commit()

        if not runnable:
            continue

        # Evaluate run_if and resolve {{secret:...}} / {{task....}} placeholders up front,
        # on the main thread (the db session isn't thread-safe to share across the pool below).
        resolved = {}
        for t in runnable:
            upstream_outputs = {dep: task_runs_by_key[dep].output or {} for dep in (t.depends_on or [])}
            upstream_status = {dep: task_runs_by_key[dep].status for dep in (t.depends_on or [])}

            if t.run_if and not _evaluate_run_if(t.run_if, upstream_outputs, upstream_status):
                tr = task_runs_by_key[t.key]
                tr.status = models.RunStatus.SKIPPED
                tr.logs = f"Skipped: run_if condition '{t.run_if}' evaluated falsy"
                tr.finished_at = datetime.utcnow()
                skipped_keys.add(t.key)
                blocking_keys.add(t.key)
                continue

            try:
                task_type = t.type.value if hasattr(t.type, "value") else t.type
                command_text = resolve_table_names(t.command, db) if task_type == "sql" else t.command
                cmd, params, redact_values = resolve_task(command_text, t.params or {}, db, upstream_outputs)
                resolved[t.key] = (cmd, params, redact_values)
            except ValueError as e:
                tr = task_runs_by_key[t.key]
                tr.status = models.RunStatus.FAILED
                tr.logs = str(e)
                tr.finished_at = datetime.utcnow()
                failed_keys.add(t.key)
                blocking_keys.add(t.key)
        db.commit()
        runnable = [t for t in runnable if t.key in resolved]

        with ThreadPoolExecutor(max_workers=max(1, len(runnable))) as pool:
            futures = {
                pool.submit(_run_task_with_retries, t, *resolved[t.key]): t for t in runnable
            }
            for fut in as_completed(futures):
                t = futures[fut]
                tr = task_runs_by_key[t.key]
                success, logs, attempt, output = fut.result()
                tr.status = models.RunStatus.SUCCESS if success else models.RunStatus.FAILED
                tr.logs = logs
                tr.attempt = attempt
                tr.output = output
                tr.finished_at = datetime.utcnow()
                if not success:
                    failed_keys.add(t.key)
                    blocking_keys.add(t.key)
                db.commit()

    run.status = models.RunStatus.FAILED if failed_keys else models.RunStatus.SUCCESS
    run.finished_at = datetime.utcnow()
    db.commit()

    if run.status == models.RunStatus.FAILED and workflow.on_failure_webhook:
        _notify_failure(workflow, run, failed_keys)

    _record_and_report_usage(db, workflow, run)


def _record_and_report_usage(db: Session, workflow: models.Workflow, run: models.WorkflowRun):
    """Records this run's estimated cost for the usage-report endpoint, and reports it to
    Stripe as metered usage if billing is configured for whoever the run is attributed to.
    Never raises - billing issues must never affect whether a workflow run is considered
    successful."""
    try:
        cost = 0.0
        total_seconds = 0
        for tr in run.task_runs:
            if tr.started_at and tr.finished_at:
                total_seconds += (tr.finished_at - tr.started_at).total_seconds()
        import os as _os
        cost = round((total_seconds / 3600) * float(_os.getenv("ATLASFLOW_COST_PER_WORKER_HOUR", "0.40")), 4)

        # attribute to the triggering user if manual, else the workflow's creator
        user = None
        if run.triggered_by and run.triggered_by.startswith("manual:"):
            username = run.triggered_by.split(":", 1)[1]
            user = db.query(models.User).filter_by(username=username).first()
        if not user and workflow.created_by:
            user = db.query(models.User).get(workflow.created_by)

        record = models.UsageRecord(
            workflow_run_id=run.id, workflow_id=workflow.id,
            user_id=user.id if user else None, cost_usd=str(cost),
        )
        db.add(record)
        db.commit()

        from . import billing
        if user and billing.is_enabled():
            reported = billing.report_usage(user, cost, idempotency_key=run.id)
            record.reported_to_stripe = reported
            db.commit()
    except Exception:
        pass  # billing/usage tracking must never break workflow execution


def _notify_failure(workflow: models.Workflow, run: models.WorkflowRun, failed_keys: set):
    """Best-effort POST of a JSON failure summary. Never raises - a broken webhook
    shouldn't be able to make a workflow run look like it failed to record correctly."""
    payload = json.dumps({
        "workflow": workflow.name,
        "run_id": run.id,
        "status": "failed",
        "failed_tasks": sorted(failed_keys),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }).encode()
    try:
        req = urllib.request.Request(
            workflow.on_failure_webhook, data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # notification failures are logged nowhere on purpose - they must never break a run


def _run_task_with_retries(task: models.Task, resolved_command: str, resolved_params: dict, redact_values: list):
    runner = RUNNERS[task.type.value if hasattr(task.type, "value") else task.type]
    params = dict(resolved_params)
    params["_timeout"] = task.timeout_seconds

    attempts = max(1, task.retries + 1)
    logs_accum = ""
    for attempt in range(1, attempts + 1):
        success, logs, output = runner(resolved_command, params)
        logs = redact(logs, redact_values)
        logs_accum += f"\n--- attempt {attempt} ---\n{logs}"
        if success:
            return True, logs_accum, attempt, output
        if attempt < attempts:
            time.sleep(min(2 ** attempt, 30))  # backoff before retrying
    return False, logs_accum, attempts, {}
