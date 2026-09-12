"""
Real Postgres-backed transactional (OLTP) tables - AtlasFlow's equivalent of Databricks
Lakebase. Genuinely different from the Data tab's Delta tables: those are batch/analytical
(write once, read many, no single-row update); these support real row-level
insert/update/delete with immediate consistency, the way an application's database does.

Tables live in a dedicated `lakebase` Postgres schema on the same instance already
running in docker-compose.yml - isolated from AtlasFlow's own metadata tables (which live
in the default `public` schema), but not a separate database, to avoid the operational
overhead of provisioning one per table.

Honest limit: "sync to Delta" (see app/main.py) is a periodic full-refresh read via Spark
JDBC, not true log-based CDC streaming - Databricks' real Lakebase uses Postgres logical
replication for near-real-time sync, which is a much larger feature (replication slot
management, schema evolution handling, exactly-once delivery) than fits here.
"""
import re

from sqlalchemy import text
from sqlalchemy.orm import Session

ALLOWED_TYPES = {"text", "integer", "bigint", "boolean", "double precision", "timestamp", "date", "numeric"}
_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, what: str = "identifier"):
    if not _IDENT.match(name):
        raise ValueError(f"Invalid {what} '{name}': must start with a letter/underscore, contain only letters/numbers/underscores")


def ensure_schema(db: Session):
    db.execute(text("CREATE SCHEMA IF NOT EXISTS lakebase"))
    db.commit()


def create_table(db: Session, name: str, columns: list):
    _validate_identifier(name, "table name")
    col_defs = ["id SERIAL PRIMARY KEY"]
    for col in columns:
        _validate_identifier(col["name"], "column name")
        col_type = col["type"].lower()
        if col_type not in ALLOWED_TYPES:
            raise ValueError(f"Column type '{col_type}' not allowed - use one of {sorted(ALLOWED_TYPES)}")
        col_defs.append(f'"{col["name"]}" {col_type}')
    ddl = f'CREATE TABLE lakebase."{name}" ({", ".join(col_defs)})'
    db.execute(text(ddl))
    db.commit()


def drop_table(db: Session, name: str):
    _validate_identifier(name, "table name")
    db.execute(text(f'DROP TABLE IF EXISTS lakebase."{name}"'))
    db.commit()


def insert_row(db: Session, name: str, values: dict) -> dict:
    _validate_identifier(name, "table name")
    for k in values:
        _validate_identifier(k, "column name")
    cols = ", ".join(f'"{k}"' for k in values)
    placeholders = ", ".join(f":{k}" for k in values)
    result = db.execute(text(f'INSERT INTO lakebase."{name}" ({cols}) VALUES ({placeholders}) RETURNING *'), values)
    db.commit()
    row = result.mappings().first()
    return dict(row) if row else {}


def list_rows(db: Session, name: str, limit: int = 200) -> list:
    _validate_identifier(name, "table name")
    result = db.execute(text(f'SELECT * FROM lakebase."{name}" ORDER BY id DESC LIMIT :limit'), {"limit": limit})
    return [dict(r) for r in result.mappings().all()]


def delete_row(db: Session, name: str, row_id: int):
    _validate_identifier(name, "table name")
    db.execute(text(f'DELETE FROM lakebase."{name}" WHERE id = :id'), {"id": row_id})
    db.commit()


def row_count(db: Session, name: str) -> int:
    _validate_identifier(name, "table name")
    result = db.execute(text(f'SELECT COUNT(*) FROM lakebase."{name}"'))
    return result.scalar()
