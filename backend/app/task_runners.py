"""
Each runner executes one task and returns (success: bool, logs: str, output: dict).

`output` is what Databricks calls "task values" - small JSON a task can hand to
downstream tasks (see executor.py's `upstream` context, and secrets_resolver.py's
`{{task.key.field}}` placeholder resolution). Conventions per task type:

  - python:       whatever the task's code assigns to a variable named `task_output`
  - shell:         a line of stdout starting with `ATLASFLOW_OUTPUT_JSON:` (last one wins)
  - spark_submit:  same convention as shell - your job script prints that line
  - sql:           automatic - the query's result rows/columns become the output
  - notebook:      not supported yet (papermill notebooks don't expose this today)

Task types map to real backing systems, not reimplementations:
  - spark_submit / sql -> real Apache Spark cluster, with Delta Lake auto-attached
  - Any task type can call MLflow (MLFLOW_TRACKING_URI is set in the environment)
    to log experiments/models against the real MLflow server
"""
import io
import os
import json
import threading
import subprocess
import contextlib
import traceback
import tempfile

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
NOTEBOOK_OUTPUT_DIR = os.getenv("NOTEBOOK_OUTPUT_DIR", "/data/notebook_runs")
SPARK_DELTA_PACKAGES = os.getenv("SPARK_DELTA_PACKAGES", "io.delta:delta-spark_2.12:3.2.0")
OUTPUT_MARKER = "ATLASFLOW_OUTPUT_JSON:"

_spark_task_lock = threading.Lock()
_spark_task_count = 0


def running_spark_task_count() -> int:
    """Read by app/autoscaler.py to decide whether more Spark worker capacity is needed."""
    with _spark_task_lock:
        return _spark_task_count


def _extract_output_from_text(text: str) -> dict:
    """Finds the last ATLASFLOW_OUTPUT_JSON: line in captured output, if any."""
    output = {}
    for line in text.splitlines():
        if line.strip().startswith(OUTPUT_MARKER):
            try:
                output = json.loads(line.strip()[len(OUTPUT_MARKER):])
            except json.JSONDecodeError:
                pass  # malformed - ignore rather than fail the whole task
    return output


def run_python(command: str, params: dict) -> tuple[bool, str, dict]:
    """Executes a python snippet. `command` is source code; `params` are injected
    into its local namespace as variables, including `upstream` (task values from
    dependencies). Assign to `task_output` to pass values downstream."""
    buf = io.StringIO()
    local_ns = dict(params)
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            exec(command, {"__builtins__": __builtins__}, local_ns)
        output = local_ns.get("task_output", {})
        if not isinstance(output, dict):
            output = {"value": output}
        return True, buf.getvalue(), output
    except Exception:
        return False, buf.getvalue() + "\n" + traceback.format_exc(), {}


def run_shell(command: str, params: dict) -> tuple[bool, str, dict]:
    env = os.environ.copy()
    env.update({k: str(v) for k, v in params.items() if not k.startswith("_")})
    # upstream task values are available to shell scripts as an env var
    env["ATLASFLOW_UPSTREAM_JSON"] = json.dumps(params.get("upstream", {}))
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, env=env, timeout=params.get("_timeout", 3600)
        )
        logs = result.stdout + result.stderr
        return result.returncode == 0, logs, _extract_output_from_text(logs)
    except subprocess.TimeoutExpired as e:
        return False, f"Task timed out: {e}", {}


def run_spark_submit(command: str, params: dict) -> tuple[bool, str, dict]:
    """`command` is the path to a .py Spark job (mounted into the container /jobs volume).
    `params.args` (list) are passed through as CLI args to the job. Delta Lake support
    is attached automatically so any job can read/write Delta tables without extra config.
    `params.extra_packages` (list of Maven coordinates) lets a job pull in additional
    dependencies - e.g. a JDBC driver for the Data Ingestion wizard's database sources."""
    args = params.get("args", [])
    packages = SPARK_DELTA_PACKAGES
    extra = params.get("extra_packages")
    if extra:
        packages = packages + "," + ",".join(extra)
    cli = [
        "spark-submit",
        "--master", SPARK_MASTER_URL,
        "--deploy-mode", "client",
        "--packages", packages,
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.hadoop.fs.s3a.endpoint=" + os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000"),
        "--conf", "spark.hadoop.fs.s3a.access.key=" + os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "--conf", "spark.hadoop.fs.s3a.secret.key=" + os.getenv("AWS_SECRET_ACCESS_KEY", "admin12345"),
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        command,
        *[str(a) for a in args],
    ]
    env = os.environ.copy()
    env["ATLASFLOW_UPSTREAM_JSON"] = json.dumps(params.get("upstream", {}))
    global _spark_task_count
    with _spark_task_lock:
        _spark_task_count += 1
    try:
        result = subprocess.run(cli, capture_output=True, text=True, timeout=params.get("_timeout", 7200), env=env)
        logs = result.stdout + result.stderr
        return result.returncode == 0, logs, _extract_output_from_text(logs)
    except subprocess.TimeoutExpired as e:
        return False, f"Spark job timed out: {e}", {}
    finally:
        with _spark_task_lock:
            _spark_task_count -= 1


def run_sql(command: str, params: dict) -> tuple[bool, str, dict]:
    """`command` is a raw SQL string, executed via Spark SQL against Delta tables in the
    lakehouse (the same engine Databricks SQL warehouses use under the hood). Results are
    captured as structured task output (columns/rows/row_count), truncated to `params.limit`
    rows (default 200) so large result sets don't bloat run history.

    `params.sql_params` (dict) binds named :paramName tokens in the SQL, the same
    substitution the SQL tab and Genie use - see app/catalog.py's substitute_params."""
    from .catalog import substitute_params
    resolved_command = substitute_params(command, params.get("sql_params", {}))

    limit = params.get("limit", 200)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, dir="/jobs") as f:
        f.write(resolved_command)
        sql_path = f.name
    try:
        sql_params = dict(params)
        sql_params["args"] = ["--sql-file", sql_path, "--limit", str(limit)]
        success, logs, output = run_spark_submit("/jobs/run_sql.py", sql_params)
        return success, logs, output
    finally:
        try:
            os.remove(sql_path)
        except OSError:
            pass


def run_notebook(command: str, params: dict) -> tuple[bool, str, dict]:
    """`command` is the path to a .ipynb file. Executes it with papermill,
    injecting `params` as notebook parameters."""
    import papermill as pm

    os.makedirs(NOTEBOOK_OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(NOTEBOOK_OUTPUT_DIR, os.path.basename(command))
    try:
        pm.execute_notebook(command, out_path, parameters=params, log_output=True)
        return True, f"Notebook executed successfully. Output saved to {out_path}", {}
    except Exception:
        return False, traceback.format_exc(), {}


RUNNERS = {
    "python": run_python,
    "shell": run_shell,
    "spark_submit": run_spark_submit,
    "sql": run_sql,
    "notebook": run_notebook,
}
