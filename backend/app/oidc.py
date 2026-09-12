"""
OIDC (OpenID Connect) single sign-on. This is the modern, broadly-supported equivalent of
what people usually mean by enterprise "SSO" today - every major identity provider (Okta,
Azure AD/Entra ID, Google Workspace, Auth0, Ping, etc.) speaks OIDC. Classic SAML is not
implemented (it's a much larger, XML-signature-heavy protocol with less new adoption) -
if you specifically need SAML, front AtlasFlow with an OIDC-to-SAML bridge (most IdPs,
including Okta and Azure AD, can act as one).

Enable by setting these env vars (see .env.example):
  ATLASFLOW_OIDC_ISSUER          e.g. https://your-org.okta.com
  ATLASFLOW_OIDC_CLIENT_ID
  ATLASFLOW_OIDC_CLIENT_SECRET
  ATLASFLOW_OIDC_REDIRECT_URI    e.g. http://localhost:8000/api/auth/sso/callback

If unset, SSO is simply disabled and local username/password login works as before.
"""
import os
import json
import time
import secrets
import urllib.request
import urllib.parse

from jose import jwt

ISSUER = os.getenv("ATLASFLOW_OIDC_ISSUER", "")
CLIENT_ID = os.getenv("ATLASFLOW_OIDC_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("ATLASFLOW_OIDC_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("ATLASFLOW_OIDC_REDIRECT_URI", "")
SCOPES = os.getenv("ATLASFLOW_OIDC_SCOPES", "openid email profile")

_discovery_cache = {"data": None, "fetched_at": 0}
_jwks_cache = {"data": None, "fetched_at": 0}

# in-memory CSRF state store - fine for a single-process dev/small deployment;
# for multi-replica deployments, back this with Postgres/Redis instead
_pending_states = set()


def is_enabled() -> bool:
    return bool(ISSUER and CLIENT_ID and CLIENT_SECRET and REDIRECT_URI)


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _discover() -> dict:
    if not _discovery_cache["data"] or time.time() - _discovery_cache["fetched_at"] > 3600:
        _discovery_cache["data"] = _get_json(ISSUER.rstrip("/") + "/.well-known/openid-configuration")
        _discovery_cache["fetched_at"] = time.time()
    return _discovery_cache["data"]


def _jwks() -> dict:
    if not _jwks_cache["data"] or time.time() - _jwks_cache["fetched_at"] > 3600:
        _jwks_cache["data"] = _get_json(_discover()["jwks_uri"])
        _jwks_cache["fetched_at"] = time.time()
    return _jwks_cache["data"]


def build_authorize_url() -> str:
    state = secrets.token_urlsafe(24)
    _pending_states.add(state)
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
    }
    return _discover()["authorization_endpoint"] + "?" + urllib.parse.urlencode(params)


def exchange_code(code: str, state: str) -> dict:
    """Returns the verified ID token claims for the authenticated user."""
    if state not in _pending_states:
        raise ValueError("Invalid or expired OIDC state (possible CSRF, or a stale login link)")
    _pending_states.discard(state)

    token_endpoint = _discover()["token_endpoint"]
    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }).encode()
    req = urllib.request.Request(token_endpoint, data=data, method="POST",
                                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        token_response = json.loads(resp.read().decode())

    id_token = token_response.get("id_token")
    if not id_token:
        raise ValueError("Identity provider did not return an id_token")

    claims = jwt.decode(id_token, _jwks(), audience=CLIENT_ID, issuer=ISSUER,
                         options={"verify_at_hash": False})
    return claims


def username_from_claims(claims: dict) -> str:
    return claims.get("preferred_username") or claims.get("email") or claims.get("sub")
