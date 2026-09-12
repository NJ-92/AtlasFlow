"""
Real usage-based billing via Stripe metered billing. This actually charges people, using
Stripe's real API - not a simulation. Two things it does NOT do, worth knowing before you
rely on it:
  1. It has not been exercised against a live Stripe account in this environment (no
     network access here to test it). Test it in Stripe test mode before going live.
  2. AtlasFlow is single-tenant in the sense that everyone shares the same workflows/data
     within one instance - "billing" here means charging individual *users* of that one
     instance for their compute usage, not isolating separate customer organizations from
     each other's data. If you need real multi-tenant data isolation (customer A can't see
     customer B's workflows/tables at all), that's a materially bigger change than billing
     alone - flag it if that's actually what you need.

Setup (see .env.example):
  ATLASFLOW_STRIPE_SECRET_KEY       your Stripe secret key (sk_test_... or sk_live_...)
  ATLASFLOW_STRIPE_PRICE_ID         a Stripe metered Price ID, unit = 1 cent of usage
  ATLASFLOW_STRIPE_WEBHOOK_SECRET   from your Stripe webhook endpoint config
  ATLASFLOW_FRONTEND_URL            where to send users back after Stripe Checkout

You configure the Price in your own Stripe Dashboard first (Product with a metered/usage
price, $0.01/unit is the simplest mapping to "cents of estimated compute cost").
"""
import os

import stripe

from . import models

STRIPE_SECRET_KEY = os.getenv("ATLASFLOW_STRIPE_SECRET_KEY", "")
STRIPE_PRICE_ID = os.getenv("ATLASFLOW_STRIPE_PRICE_ID", "")
STRIPE_WEBHOOK_SECRET = os.getenv("ATLASFLOW_STRIPE_WEBHOOK_SECRET", "")
FRONTEND_URL = os.getenv("ATLASFLOW_FRONTEND_URL", "http://localhost:3000")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def is_enabled() -> bool:
    return bool(STRIPE_SECRET_KEY and STRIPE_PRICE_ID)


def create_checkout_session(user: models.User) -> str:
    """Creates (or reuses) a Stripe Customer for this user, then a Checkout Session for a
    metered subscription. Returns the URL to redirect the user's browser to."""
    if not user.stripe_customer_id:
        customer = stripe.Customer.create(name=user.username, metadata={"atlasflow_user_id": user.id})
        user.stripe_customer_id = customer.id
        # caller is responsible for committing this to the db

    session = stripe.checkout.Session.create(
        customer=user.stripe_customer_id,
        mode="subscription",
        line_items=[{"price": STRIPE_PRICE_ID}],
        success_url=f"{FRONTEND_URL}/?billing=success",
        cancel_url=f"{FRONTEND_URL}/?billing=canceled",
    )
    return session.url


def handle_checkout_completed(session: dict, db) -> None:
    """Called from the webhook when checkout.session.completed fires - links the new
    subscription's metered line item to the user so usage reporting knows where to send it."""
    customer_id = session.get("customer")
    subscription_id = session.get("subscription")
    if not customer_id or not subscription_id:
        return
    user = db.query(models.User).filter_by(stripe_customer_id=customer_id).first()
    if not user:
        return
    subscription = stripe.Subscription.retrieve(subscription_id)
    item_id = subscription["items"]["data"][0]["id"]
    user.stripe_subscription_item_id = item_id
    user.billing_status = "active"
    db.commit()


def report_usage(user: models.User, cost_usd: float, idempotency_key: str) -> bool:
    """Reports one run's cost as usage against the user's metered subscription item.
    Quantity is in cents (matches a $0.01/unit Price). Uses `idempotency_key` (the run id)
    so retries/replays never double-bill. Returns False (and does nothing) if billing
    isn't configured or the user has no active subscription - callers should not treat
    that as an error."""
    if not is_enabled() or not user or not user.stripe_subscription_item_id:
        return False
    quantity = max(1, round(cost_usd * 100))  # cents, minimum 1 unit so trivial runs aren't lost
    stripe.SubscriptionItem.create_usage_record(
        user.stripe_subscription_item_id,
        quantity=quantity,
        action="increment",
        idempotency_key=idempotency_key,
    )
    return True


def verify_webhook(payload: bytes, sig_header: str) -> dict:
    return stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
