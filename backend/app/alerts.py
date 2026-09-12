"""
Evaluates a saved Alert: runs its SQL (through the same Spark SQL engine as the SQL tab),
extracts the watched column's value from the first result row, compares it against the
threshold, and records the outcome. Fires `notify_webhook` (if set) only when the alert's
status changes into "triggered" - so you get one notification per incident, not one per
poll interval.
"""
import json
import urllib.request
from datetime import datetime

from sqlalchemy.orm import Session

from . import models


def _compare(value: float, operator: str, threshold: float) -> bool:
    return {
        ">": value > threshold, "<": value < threshold,
        ">=": value >= threshold, "<=": value <= threshold,
        "==": value == threshold, "!=": value != threshold,
    }.get(operator, False)


def evaluate_alert(db: Session, alert: models.Alert):
    from .task_runners import run_sql
    from .catalog import resolve_table_names

    was_triggered = alert.last_status == "triggered"
    creator = db.query(models.User).get(alert.created_by) if alert.created_by else None
    resolved_sql = resolve_table_names(alert.sql, db, creator.role if creator else None)
    success, logs, output = run_sql(resolved_sql, {"limit": 1, "_timeout": 120})

    if not success:
        _record(db, alert, "error", None, f"Query failed: {logs[-500:]}")
        return

    rows = output.get("rows", [])
    if not rows or alert.value_column not in rows[0]:
        _record(db, alert, "error", None, f"Column '{alert.value_column}' not found in query result")
        return

    try:
        value = float(rows[0][alert.value_column])
        threshold = float(alert.threshold)
    except (TypeError, ValueError):
        _record(db, alert, "error", str(rows[0].get(alert.value_column)), "Value or threshold is not numeric")
        return

    triggered = _compare(value, alert.operator, threshold)
    status = "triggered" if triggered else "ok"
    _record(db, alert, status, str(value), "")

    if triggered and not was_triggered and alert.notify_webhook:
        _notify(alert, value)


def _record(db: Session, alert: models.Alert, status: str, value, message: str):
    alert.last_status = status
    alert.last_value = value
    alert.last_checked_at = datetime.utcnow()
    db.add(models.AlertHistory(alert_id=alert.id, status=status, value=value, message=message))
    db.commit()


def _notify(alert: models.Alert, value: float):
    payload = json.dumps({
        "alert": alert.name, "status": "triggered", "value": value,
        "operator": alert.operator, "threshold": alert.threshold,
    }).encode()
    try:
        req = urllib.request.Request(
            alert.notify_webhook, data=payload, headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # notification failures must never break alert evaluation
