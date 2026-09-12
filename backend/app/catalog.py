"""
Lets SQL (and, in principle, any generated query text) reference a table by its friendly
catalog name - `SELECT * FROM sales_data` - instead of a raw storage path. Applied before
secrets/task-value placeholder resolution, and before the query reaches Spark.

Also handles named SQL parameters - `:paramName` tokens bound to values supplied by the
caller, the same pattern Databricks notebook widgets use to parameterize a query. This is
textual substitution (not a driver-level prepared statement), since Spark SQL via
spark-submit has no bind-parameter API to hook into - values are escaped and typed
(numeric vs string) before being substituted in.
"""
import re

from sqlalchemy.orm import Session

from . import models

_PARAM_PATTERN = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


def referenced_table_names(sql: str, db: Session) -> list:
    """Returns catalog table names that appear (as whole words) in the given SQL text -
    used to check access before a query runs, not just to resolve paths."""
    tables = db.query(models.Table).all()
    found = []
    for t in tables:
        if re.search(r"\b" + re.escape(t.name) + r"\b", sql):
            found.append(t.name)
    return found


def resolve_table_names(sql: str, db: Session, user_role: str = None) -> str:
    """Substitutes catalog table names for their storage path. When `user_role` is given,
    also applies row-level filters and column masking configured on the table (see
    Table.row_filters / Table.masked_columns) by wrapping the reference in a subquery -
    real query rewriting, not a client-side filter that a user could bypass by editing
    their own request. `user_role=None` (the default) applies no restrictions - used for
    internal/system contexts (pipelines, workflow tasks) that are already documented as
    running with the creating user's trust level; interactive endpoints (SQL tab,
    dashboards, Genie, alerts) always pass the querying user's actual role."""
    tables = db.query(models.Table).all()
    for t in tables:
        pattern = re.compile(r"\b" + re.escape(t.name) + r"\b")
        if not pattern.search(sql):
            continue
        sql = pattern.sub(_secured_reference(t, user_role), sql)
    return sql


def _secured_reference(t, user_role: str) -> str:
    from .security import ROLE_RANK

    base = f'delta.`{t.path}`'
    if user_role is None:
        return base

    user_rank = ROLE_RANK.get(user_role, 0)

    filter_expr = (t.row_filters or {}).get(user_role)
    if filter_expr:
        base = f'(SELECT * FROM {base} WHERE {filter_expr})'

    mask_exprs = []
    for m in (t.masked_columns or []):
        required_rank = ROLE_RANK.get(m.get("unmask_role", "admin"), 2)
        if user_rank < required_rank:
            mask_exprs.append(f'NULL AS `{m["column"]}`')
    if mask_exprs:
        base = f'(SELECT * REPLACE({", ".join(mask_exprs)}) FROM {base})'

    if filter_expr or mask_exprs:
        return f'{base} AS `{t.name}`'
    return base


def detect_params(sql: str) -> list:
    """Returns the distinct :paramName tokens referenced in the SQL, in first-seen order -
    used by the UI to auto-render an input box per parameter, the way Databricks
    auto-creates a widget for each :param used in a query."""
    seen = []
    for name in _PARAM_PATTERN.findall(sql):
        if name not in seen:
            seen.append(name)
    return seen


def _sql_literal(value: str) -> str:
    """Renders a parameter value as a SQL literal: unquoted if it parses as a number,
    otherwise a single-quoted, escaped string."""
    try:
        float(value)
        return value
    except (TypeError, ValueError):
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


def substitute_params(sql: str, params: dict) -> str:
    if not params:
        return sql

    def _sub(match):
        name = match.group(1)
        if name in params:
            return _sql_literal(params[name])
        return match.group(0)  # leave unrecognized :tokens untouched

    return _PARAM_PATTERN.sub(_sub, sql)
