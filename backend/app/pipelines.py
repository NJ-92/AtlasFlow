"""
Dependency inference for declarative Pipelines: given a set of declared tables (each with
its own SQL), figures out which tables each one reads from (by scanning for other
declared table names in the SQL text, the same word-boundary approach app/catalog.py uses
for the regular table catalog) and returns them in a valid execution order - no manual
`depends_on` wiring required, which is the defining trait of a declarative pipeline vs an
imperative Workflow.
"""
import re

from . import models


def _detect_deps(pipeline_tables: list) -> dict:
    names = [t.table_name for t in pipeline_tables]
    deps = {}
    for t in pipeline_tables:
        refs = [n for n in names if n != t.table_name and re.search(r"\b" + re.escape(n) + r"\b", t.sql)]
        deps[t.table_name] = refs
    return deps


def topological_order(pipeline_tables: list) -> list:
    """Returns pipeline_tables reordered so every table appears after everything it
    depends on. Raises ValueError on a circular dependency."""
    deps = _detect_deps(pipeline_tables)
    by_name = {t.table_name: t for t in pipeline_tables}
    visited, temp_mark, order = set(), set(), []

    def visit(name):
        if name in visited:
            return
        if name in temp_mark:
            raise ValueError(f"Circular dependency detected involving '{name}'")
        temp_mark.add(name)
        for dep in deps.get(name, []):
            if dep in by_name:
                visit(dep)
        temp_mark.discard(name)
        visited.add(name)
        order.append(by_name[name])

    for t in pipeline_tables:
        visit(t.table_name)
    return order
