"""
AI Gateway: a single place to configure LLM endpoints (provider, model, rate limit) and
call them through one interface - AtlasFlow's equivalent of Databricks' AI Gateway.
Real provider calls (Anthropic and OpenAI SDKs), not a mock. API keys are pulled from the
existing encrypted Secrets store by name, so they're never duplicated or stored in a
second place.
"""
import time
from collections import defaultdict

from sqlalchemy.orm import Session

from . import models
from .security import decrypt_secret

# in-memory sliding-window rate limiter per route - fine for a single-process deployment;
# resets on restart, which is an acceptable tradeoff for this scope
_call_history = defaultdict(list)


def _rate_limit_ok(route: models.AIGatewayRoute) -> bool:
    now = time.time()
    window_start = now - 60
    _call_history[route.id] = [t for t in _call_history[route.id] if t > window_start]
    if len(_call_history[route.id]) >= route.requests_per_minute:
        return False
    _call_history[route.id].append(now)
    return True


def _get_api_key(db: Session, secret_name: str) -> str:
    secret = db.query(models.Secret).filter_by(name=secret_name).first()
    if not secret:
        raise ValueError(f"Secret '{secret_name}' not found - configure it in the Secrets tab first")
    return decrypt_secret(secret.encrypted_value)


def call_route(db: Session, route: models.AIGatewayRoute, messages: list, system: str = None) -> str:
    """messages: [{"role": "user"|"assistant", "content": "..."}]. Returns the response text."""
    if not route.is_enabled:
        raise ValueError(f"Route '{route.name}' is disabled")
    if not _rate_limit_ok(route):
        raise ValueError(f"Rate limit exceeded for route '{route.name}' ({route.requests_per_minute}/min)")

    api_key = _get_api_key(db, route.api_key_secret_name)

    if route.provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=route.model, max_tokens=1024, system=system or "You are a helpful assistant.", messages=messages,
        )
        return "".join(block.text for block in response.content if hasattr(block, "text"))

    elif route.provider == "openai":
        import openai
        client = openai.OpenAI(api_key=api_key)
        full_messages = ([{"role": "system", "content": system}] if system else []) + messages
        response = client.chat.completions.create(model=route.model, messages=full_messages, max_tokens=1024)
        return response.choices[0].message.content

    raise ValueError(f"Unknown provider '{route.provider}'")
