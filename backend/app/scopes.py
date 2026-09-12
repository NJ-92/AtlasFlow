"""
Named scopes a Personal Access Token can be restricted to - AtlasFlow's equivalent of
Databricks' fine-grained token scoping. A PAT's scopes are checked against the raw
HTTP method + path of every request (see the middleware in app/main.py) *in addition to*
the normal role check on the endpoint - a PAT can only ever narrow what its owning user
could already do, never grant more.
"""
import fnmatch

SCOPE_CATALOG = {
    "all": [("*", "/api/*")],
    "workflows:read": [("GET", "/api/workflows*"), ("GET", "/api/runs*")],
    "workflows:trigger": [("POST", "/api/workflows/*/trigger")],
    "workflows:write": [("POST", "/api/workflows"), ("DELETE", "/api/workflows/*"), ("POST", "/api/workflows/*")],
    "sql:query": [("POST", "/api/sql/query"), ("GET", "/api/sql/history")],
    "tables:read": [("GET", "/api/tables*")],
    "tables:write": [("POST", "/api/tables*"), ("DELETE", "/api/tables*")],
    "volumes:read": [("GET", "/api/volumes*")],
    "volumes:write": [("POST", "/api/volumes*"), ("DELETE", "/api/volumes*")],
    "genie:ask": [("POST", "/api/genie/ask"), ("GET", "/api/genie/history")],
}


def scope_allows(scopes: list, method: str, path: str) -> bool:
    for scope_name in scopes:
        for allowed_method, allowed_path_pattern in SCOPE_CATALOG.get(scope_name, []):
            if allowed_method in ("*", method) and fnmatch.fnmatch(path, allowed_path_pattern):
                return True
    return False
