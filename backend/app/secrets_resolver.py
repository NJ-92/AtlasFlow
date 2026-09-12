"""
Placeholder resolution for two things tasks can reference without hardcoding:

  {{secret:my_api_key}}        -> decrypted value from the encrypted secrets store
  {{task.extract.row_count}}   -> a value an upstream task passed downstream via `task_output`
                                   (dot-path into that task's TaskRun.output JSON)

Both are resolved just before a task runs and scrubbed back out of persisted logs, so
neither credentials nor (potentially large) upstream data ever sit in plaintext in a
workflow definition or leak into log output unexpectedly.
"""
import re

from sqlalchemy.orm import Session

from . import models
from .security import decrypt_secret

_SECRET_PATTERN = re.compile(r"\{\{secret:([a-zA-Z0-9_\-]+)\}\}")
_TASK_PATTERN = re.compile(r"\{\{task\.([a-zA-Z0-9_\-]+)((?:\.[a-zA-Z0-9_\-]+)*)\}\}")


def _dig(obj, dotted_path: str):
    """dotted_path like '.row_count.nested' -> obj['row_count']['nested']"""
    cur = obj
    for part in [p for p in dotted_path.split(".") if p]:
        if not isinstance(cur, dict) or part not in cur:
            raise ValueError(f"Path '{dotted_path}' not found in upstream task output")
        cur = cur[part]
    return cur


def _resolve_string(text: str, db: Session, upstream: dict, collected_secrets: list) -> str:
    def _sub_secret(match):
        name = match.group(1)
        secret = db.query(models.Secret).filter_by(name=name).first()
        if not secret:
            raise ValueError(f"Referenced secret '{name}' does not exist")
        value = decrypt_secret(secret.encrypted_value)
        collected_secrets.append(value)
        return value

    def _sub_task(match):
        task_key, path = match.group(1), match.group(2)
        if task_key not in upstream:
            raise ValueError(f"'{task_key}' is not an upstream dependency of this task")
        value = _dig(upstream[task_key], path)
        return value if isinstance(value, str) else str(value)

    text = _SECRET_PATTERN.sub(_sub_secret, text)
    text = _TASK_PATTERN.sub(_sub_task, text)
    return text


def resolve_task(command: str, params: dict, db: Session, upstream: dict):
    """Returns (resolved_command, resolved_params, secret_values_to_redact).
    `upstream` is {task_key: output_dict} for every dependency of this task, also made
    available whole (unresolved) to python/shell/spark runners as params['upstream']."""
    collected: list[str] = []
    resolved_command = _resolve_string(command, db, upstream, collected)
    resolved_params = {}
    for k, v in (params or {}).items():
        resolved_params[k] = _resolve_string(v, db, upstream, collected) if isinstance(v, str) else v
    resolved_params["upstream"] = upstream
    return resolved_command, resolved_params, collected


def redact(logs: str, secret_values: list) -> str:
    for value in secret_values:
        if value:
            logs = logs.replace(value, "***REDACTED***")
    return logs
