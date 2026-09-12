"""
Security primitives for AtlasFlow:
  - password hashing (bcrypt)
  - JWT issuance/verification for API authentication
  - symmetric encryption for the secrets store (Fernet)
  - role-based access control dependencies for FastAPI routes

Two required env vars in production (see docker-compose.yml / GETTING_STARTED.md):
  ATLASFLOW_JWT_SECRET   - signs auth tokens; rotate this to invalidate all sessions
  ATLASFLOW_ENCRYPTION_KEY - encrypts values in the secrets store (Fernet key, 32 url-safe base64 bytes)

Both have dev-only fallback defaults below so the stack still boots out of the box, but
GETTING_STARTED.md flags this as required to change before any non-local use.
"""
import os
import hashlib
import secrets as secrets_mod
from datetime import datetime, timedelta
from typing import Optional

from cryptography.fernet import Fernet
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from . import models
from .database import get_db

# --- config -----------------------------------------------------------------

JWT_SECRET = os.getenv("ATLASFLOW_JWT_SECRET", "dev-only-insecure-secret-change-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("ATLASFLOW_JWT_EXPIRE_MINUTES", "480"))  # 8h default

_encryption_key = os.getenv("ATLASFLOW_ENCRYPTION_KEY")
if not _encryption_key:
    # Deterministic dev-only fallback so local `docker compose up` works with zero config.
    # Never used if ATLASFLOW_ENCRYPTION_KEY is set, which GETTING_STARTED.md instructs
    # you to do before any shared/production deployment.
    _encryption_key = Fernet.generate_key().decode()
_fernet = Fernet(_encryption_key.encode() if isinstance(_encryption_key, str) else _encryption_key)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

# role hierarchy: higher number = more privilege
ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}

# Personal Access Tokens (fine-grained, scoped) are distinguished from JWT session tokens
# by this prefix, so get_current_user can tell them apart without ambiguity.
PAT_PREFIX = "atlasflow_pat_"


def generate_pat() -> str:
    return PAT_PREFIX + secrets_mod.token_urlsafe(32)


def hash_pat(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# --- password hashing ---------------------------------------------------------

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# --- JWT ------------------------------------------------------------------

def create_access_token(user: models.User) -> str:
    expire = datetime.utcnow() + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {"sub": user.id, "username": user.username, "role": user.role, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")


# --- FastAPI dependencies ---------------------------------------------------

def get_current_user(token: Optional[str] = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> models.User:
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated", headers={"WWW-Authenticate": "Bearer"})

    if token.startswith(PAT_PREFIX):
        pat = db.query(models.PersonalAccessToken).filter_by(token_hash=hash_pat(token)).first()
        if not pat or (pat.expires_at and pat.expires_at < datetime.utcnow()):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")
        pat.last_used_at = datetime.utcnow()
        db.commit()
        user = db.query(models.User).get(pat.user_id)
        if not user or not user.is_active:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found or disabled")
        return user

    payload = decode_access_token(token)
    user = db.query(models.User).get(payload.get("sub"))
    if not user or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found or disabled")
    return user


def require_role(min_role: str):
    """Dependency factory: require_role("editor") lets editor+admin through, blocks viewer."""
    def _check(user: models.User = Depends(get_current_user)) -> models.User:
        if ROLE_RANK.get(user.role, -1) < ROLE_RANK[min_role]:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"Requires '{min_role}' role or higher")
        return user
    return _check


# --- secrets encryption ------------------------------------------------------

def encrypt_secret(plaintext: str) -> str:
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    return _fernet.decrypt(ciphertext.encode()).decode()
