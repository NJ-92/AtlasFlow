"""
Automatically adds/removes Spark worker containers based on load, using the Docker Engine
API directly (via the mounted docker.sock) - not a simulation, it really creates and
removes containers.

Be clear about what this is NOT: it cannot exceed your host machine's physical CPU/RAM,
because every container - workers you started manually and ones this controller creates -
runs on the same box. This is automated capacity management on fixed hardware, not the
elastic "spin up new cloud VMs" autoscaling Databricks does. If you need real elasticity,
you need a container orchestrator (Kubernetes + cluster autoscaler) or cloud provider
APIs underneath - this controller doesn't replace that, it just automates what you'd
otherwise do by hand with `docker compose up --scale`.

Security note: this requires /var/run/docker.sock mounted into the backend container,
which gives that container control over the whole Docker host (any process that can talk
to the Docker socket can, in practice, break out to the host). Only enable this
(ATLASFLOW_AUTOSCALE_ENABLED=true) on infrastructure you trust the AtlasFlow backend with
root-equivalent access to. See SECURITY.md.

Config (see .env.example):
  ATLASFLOW_AUTOSCALE_ENABLED
  ATLASFLOW_AUTOSCALE_MIN_WORKERS   (default 0 - i.e. only the workers you started manually)
  ATLASFLOW_AUTOSCALE_MAX_EXTRA_WORKERS   (default 4 - cap on auto-created workers)
  ATLASFLOW_AUTOSCALE_POLL_SECONDS  (default 20)
"""
import os
import time
import threading

ENABLED = os.getenv("ATLASFLOW_AUTOSCALE_ENABLED", "false").lower() == "true"
MIN_EXTRA_WORKERS = int(os.getenv("ATLASFLOW_AUTOSCALE_MIN_WORKERS", "0"))
MAX_EXTRA_WORKERS = int(os.getenv("ATLASFLOW_AUTOSCALE_MAX_EXTRA_WORKERS", "4"))
POLL_SECONDS = int(os.getenv("ATLASFLOW_AUTOSCALE_POLL_SECONDS", "20"))
COOLDOWN_SECONDS = int(os.getenv("ATLASFLOW_AUTOSCALE_COOLDOWN_SECONDS", "60"))

AUTOWORKER_PREFIX = "atlasflow-autoworker-"

_state = {
    "running": False,
    "current_extra_workers": 0,
    "last_action": "not started",
    "last_action_at": None,
    "last_error": None,
}
_last_scale_time = 0


def status() -> dict:
    return dict(_state, enabled=ENABLED, min_workers=MIN_EXTRA_WORKERS, max_workers=MAX_EXTRA_WORKERS)


def _client():
    import docker
    return docker.from_env()


def _find_reference_container(client):
    """Finds a manually-started spark-worker container to clone image/env/network from."""
    for c in client.containers.list():
        if "spark-worker" in c.name and not c.name.startswith(AUTOWORKER_PREFIX):
            return c
    return None


def _autoworker_containers(client):
    return [c for c in client.containers.list(all=True) if c.name.startswith(AUTOWORKER_PREFIX)]


def scale_to(target_extra_workers: int) -> str:
    """Ensures exactly `target_extra_workers` auto-created worker containers exist."""
    global _last_scale_time
    client = _client()
    ref = _find_reference_container(client)
    if not ref:
        return "no reference spark-worker container found to clone"

    target_extra_workers = max(MIN_EXTRA_WORKERS, min(MAX_EXTRA_WORKERS, target_extra_workers))
    current = _autoworker_containers(client)

    if len(current) < target_extra_workers:
        ref.reload()
        image = ref.image.tags[0] if ref.image.tags else ref.image.id
        env = {kv.split("=", 1)[0]: kv.split("=", 1)[1] for kv in ref.attrs["Config"]["Env"] if "=" in kv}
        networks = list(ref.attrs["NetworkSettings"]["Networks"].keys())
        network = networks[0] if networks else None

        added = 0
        for i in range(len(current), target_extra_workers):
            name = f"{AUTOWORKER_PREFIX}{i}"
            client.containers.run(
                image, name=name, environment=env, network=network, detach=True,
                labels={"atlasflow.autoscaler": "true"},
            )
            added += 1
        _last_scale_time = time.time()
        return f"scaled up: added {added} worker(s)"

    elif len(current) > target_extra_workers:
        removed = 0
        for c in current[target_extra_workers:]:
            c.stop(timeout=30)
            c.remove()
            removed += 1
        _last_scale_time = time.time()
        return f"scaled down: removed {removed} worker(s)"

    return "no change"


def _decide_and_scale(running_spark_tasks: int):
    client = _client()
    current = len(_autoworker_containers(client))

    since_last_scale = time.time() - _last_scale_time
    # scale up immediately when tasks are queued beyond current capacity
    if running_spark_tasks > current and current < MAX_EXTRA_WORKERS:
        action = scale_to(current + 1)
    # scale down only after a cooldown, to avoid flapping on brief lulls
    elif running_spark_tasks < current and current > MIN_EXTRA_WORKERS and since_last_scale > COOLDOWN_SECONDS:
        action = scale_to(current - 1)
    else:
        action = "no change needed"

    _state["current_extra_workers"] = len(_autoworker_containers(client))
    _state["last_action"] = action
    import datetime
    _state["last_action_at"] = datetime.datetime.utcnow().isoformat()


def _loop():
    from .task_runners import running_spark_task_count
    _state["running"] = True
    while True:
        try:
            _decide_and_scale(running_spark_task_count())
            _state["last_error"] = None
        except Exception as e:
            _state["last_error"] = str(e)
        time.sleep(POLL_SECONDS)


def start_if_enabled():
    if not ENABLED:
        return
    threading.Thread(target=_loop, daemon=True).start()
