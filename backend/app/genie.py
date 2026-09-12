"""
Genie: ask a question in plain English, get back generated SQL run against your catalog
tables. A real integration - it calls the actual Anthropic API - not a hand-rolled
approximation, and not a novel AI system of AtlasFlow's own. Requires your own API key.

Enable by setting ATLASFLOW_ANTHROPIC_API_KEY (see .env.example). If unset, the Genie tab
is simply hidden - everything else in AtlasFlow works without it.

Honest limits, worth knowing before relying on this:
  - It only sees table/column names and types from the catalog (Table.columns), not your
    actual data - so it can get column semantics wrong (e.g. confusing a status code
    column's meaning). Always read the generated SQL before trusting the result.
  - No conversation memory across requests in this version - each question is answered
    fresh from the schema, not with awareness of prior questions in the session.
  - Like any LLM SQL generation, it can produce syntactically valid but logically wrong
    queries, especially on ambiguous questions. This is assistive, not authoritative.
"""
import os
import re

from sqlalchemy.orm import Session

from . import models

API_KEY = os.getenv("ATLASFLOW_ANTHROPIC_API_KEY", "")
MODEL = os.getenv("ATLASFLOW_GENIE_MODEL", "claude-sonnet-4-5")

SYSTEM_PROMPT = """You are a SQL generation assistant for a Spark SQL / Delta Lake data platform.
Given a natural-language question and a schema description, generate ONE Spark SQL query that
answers it. Rules:
- Output ONLY the SQL, inside a single ```sql code fence. No prose before or after.
- Use only the tables/columns listed in the schema. Never invent column names.
- Prefer simple, readable SQL. Use LIMIT 200 unless the question implies a specific row count.
- If the question cannot be answered from the given schema, output a SQL comment explaining why
  instead of guessing at table/column names."""


def is_enabled() -> bool:
    return bool(API_KEY)


def build_schema_context(db: Session, accessible_table_names: list) -> str:
    tables = db.query(models.Table).filter(models.Table.name.in_(accessible_table_names)).all()
    if not tables:
        return "No tables are available in the catalog yet."
    lines = []
    for t in tables:
        cols = ", ".join(f"{c.get('name')} ({c.get('type')})" for c in (t.columns or []))
        lines.append(f"- {t.name}: {cols or 'columns unknown'}" + (f" -- {t.description}" if t.description else ""))
    return "\n".join(lines)


def _extract_sql(text: str) -> str:
    match = re.search(r"```sql\s*(.*?)\s*```", text, re.S | re.I)
    return match.group(1).strip() if match else text.strip()


def ask(question: str, schema_context: str) -> dict:
    """Returns {"sql": str, "raw_response": str}. Raises on API errors - callers should
    catch and surface a clean error rather than a raw SDK exception."""
    import anthropic

    client = anthropic.Anthropic(api_key=API_KEY)
    user_message = f"Schema:\n{schema_context}\n\nQuestion: {question}"

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    raw_text = "".join(block.text for block in response.content if hasattr(block, "text"))
    return {"sql": _extract_sql(raw_text), "raw_response": raw_text}
