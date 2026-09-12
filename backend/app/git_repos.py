"""
Real `git` operations (clone, pull, commit, push) via subprocess against repositories
cloned into the shared notebooks folder - AtlasFlow's equivalent of Databricks
Repos/Git folders. Not a reimplementation of Git; this shells out to the real thing.
"""
import os
import subprocess

NOTEBOOKS_DIR = "/notebooks"


def repo_path(name: str) -> str:
    return os.path.join(NOTEBOOKS_DIR, name)


def _authenticated_url(remote_url: str, token: str = None) -> str:
    if not token or "://" not in remote_url:
        return remote_url
    scheme, rest = remote_url.split("://", 1)
    return f"{scheme}://{token}@{rest}"


def _run(cmd: list, cwd: str = None) -> tuple:
    try:
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        return result.returncode == 0, (result.stdout + result.stderr)
    except subprocess.TimeoutExpired as e:
        return False, f"Command timed out: {e}"


def clone_or_pull(name: str, remote_url: str, branch: str, token: str = None) -> tuple:
    path = repo_path(name)
    url = _authenticated_url(remote_url, token)
    if os.path.isdir(os.path.join(path, ".git")):
        ok, log = _run(["git", "pull", "origin", branch], cwd=path)
    else:
        os.makedirs(NOTEBOOKS_DIR, exist_ok=True)
        ok, log = _run(["git", "clone", "--branch", branch, url, path])
    return ok, log


def status(name: str) -> dict:
    path = repo_path(name)
    if not os.path.isdir(os.path.join(path, ".git")):
        return {"cloned": False}
    _, porcelain = _run(["git", "status", "--porcelain"], cwd=path)
    _, log = _run(["git", "log", "-1", "--format=%H %s (%an, %ar)"], cwd=path)
    return {"cloned": True, "dirty": bool(porcelain.strip()), "changed_files": porcelain.strip().splitlines(), "last_commit": log.strip()}


def commit_and_push(name: str, message: str, remote_url: str, branch: str, token: str = None) -> tuple:
    path = repo_path(name)
    _run(["git", "config", "user.email", "atlasflow@localhost"], cwd=path)
    _run(["git", "config", "user.name", "AtlasFlow"], cwd=path)
    _run(["git", "add", "-A"], cwd=path)
    ok, log = _run(["git", "commit", "-m", message], cwd=path)
    if not ok and "nothing to commit" in log.lower():
        return True, "Nothing to commit"
    if not ok:
        return False, log
    url = _authenticated_url(remote_url, token)
    push_ok, push_log = _run(["git", "push", url, branch], cwd=path)
    return push_ok, log + "\n" + push_log
