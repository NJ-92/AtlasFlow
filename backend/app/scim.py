"""
SCIM 2.0 user provisioning. Implements the core User resource operations most IdPs
(Okta, Azure AD/Entra ID, OneLogin) actually use for automated provisioning: list, get,
create, replace, patch (activate/deactivate), delete.

Enable by setting ATLASFLOW_SCIM_TOKEN (see .env.example) to a random secret, and give
that same value to your IdP as the SCIM bearer token. Point your IdP's SCIM base URL at:
  http://<your-host>:8000/scim/v2

New users provisioned via SCIM are created as `viewer` by default - promote them via the
Users tab or a follow-up API call if they need `editor`/`admin`.
"""
import os
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from sqlalchemy.orm import Session

from . import models, security
from .database import get_db

router = APIRouter(prefix="/scim/v2", tags=["scim"])

SCIM_TOKEN = os.getenv("ATLASFLOW_SCIM_TOKEN", "")


def is_enabled() -> bool:
    return bool(SCIM_TOKEN)


def require_scim_auth(authorization: str = Header(None)):
    if not is_enabled():
        raise HTTPException(404, "SCIM is not configured")
    if not authorization or authorization != f"Bearer {SCIM_TOKEN}":
        raise HTTPException(401, "Invalid SCIM bearer token")


def _to_scim_user(u: models.User) -> dict:
    return {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "id": u.id,
        "externalId": u.external_id,
        "userName": u.username,
        "active": u.is_active,
        "emails": [{"value": u.username, "primary": True}],
        "atlasflow:role": u.role,
        "meta": {"resourceType": "User", "created": u.created_at.isoformat() if u.created_at else None},
    }


@router.get("/Users", dependencies=[Depends(require_scim_auth)])
def list_users(startIndex: int = 1, count: int = 100, filter: str = None, db: Session = Depends(get_db)):
    query = db.query(models.User)
    # supports the one filter form every major IdP actually sends: userName eq "value"
    if filter and " eq " in filter:
        field, value = filter.split(" eq ", 1)
        value = value.strip().strip('"')
        if field.strip() == "userName":
            query = query.filter(models.User.username == value)
    total = query.count()
    users = query.offset(startIndex - 1).limit(count).all()
    return {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": total,
        "startIndex": startIndex,
        "itemsPerPage": len(users),
        "Resources": [_to_scim_user(u) for u in users],
    }


@router.get("/Users/{user_id}", dependencies=[Depends(require_scim_auth)])
def get_user(user_id: str, db: Session = Depends(get_db)):
    u = db.query(models.User).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    return _to_scim_user(u)


@router.post("/Users", dependencies=[Depends(require_scim_auth)], status_code=201)
async def create_user(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    username = body.get("userName")
    if not username:
        raise HTTPException(400, "userName is required")
    existing = db.query(models.User).filter_by(username=username).first()
    if existing:
        return _to_scim_user(existing)
    u = models.User(
        username=username,
        hashed_password=None,
        role="viewer",
        auth_provider="scim",
        external_id=body.get("externalId"),
        is_active=body.get("active", True),
    )
    db.add(u)
    db.commit()
    return _to_scim_user(u)


@router.put("/Users/{user_id}", dependencies=[Depends(require_scim_auth)])
async def replace_user(user_id: str, request: Request, db: Session = Depends(get_db)):
    u = db.query(models.User).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    body = await request.json()
    if "userName" in body:
        u.username = body["userName"]
    if "active" in body:
        u.is_active = body["active"]
    if "externalId" in body:
        u.external_id = body["externalId"]
    db.commit()
    return _to_scim_user(u)


@router.patch("/Users/{user_id}", dependencies=[Depends(require_scim_auth)])
async def patch_user(user_id: str, request: Request, db: Session = Depends(get_db)):
    """Handles the PATCH shape IdPs actually send for deactivation:
    {"Operations": [{"op": "replace", "path": "active", "value": false}]}"""
    u = db.query(models.User).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    body = await request.json()
    for op in body.get("Operations", []):
        path = (op.get("path") or "").lower()
        value = op.get("value")
        if path == "active":
            u.is_active = bool(value)
        elif path == "username":
            u.username = value
    db.commit()
    return _to_scim_user(u)


@router.delete("/Users/{user_id}", dependencies=[Depends(require_scim_auth)], status_code=204)
def delete_user(user_id: str, db: Session = Depends(get_db)):
    """SCIM DELETE deactivates rather than hard-deletes, so audit logs and workflow
    ownership records stay intact - the common real-world interpretation of this verb."""
    u = db.query(models.User).get(user_id)
    if u:
        u.is_active = False
        db.commit()
    return None
