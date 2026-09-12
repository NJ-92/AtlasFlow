"""
SAML 2.0 single sign-on, using the OneLogin python3-saml toolkit (the standard library
for this in Python - implements the actual XML signature validation, not a hand-rolled
approximation).

Enable by setting (see .env.example):
  ATLASFLOW_SAML_IDP_ENTITY_ID
  ATLASFLOW_SAML_IDP_SSO_URL
  ATLASFLOW_SAML_IDP_X509_CERT     (the IdP's signing certificate, PEM body only, no headers/newlines needed - one line is fine)
  ATLASFLOW_SAML_SP_ENTITY_ID      (defaults to the ACS URL if unset)
  ATLASFLOW_SAML_BASE_URL          e.g. http://localhost:8000  (used to build the ACS URL)

Honest note: this is written correctly against the SAML 2.0 spec via python3-saml, but
has not been exercised against a live IdP in this environment (no network access here to
test it end-to-end). Test it against your actual IdP (Okta, Azure AD, etc.) before relying
on it - SAML deployments are notoriously sensitive to clock skew, certificate format, and
IdP-specific quirks.
"""
import os

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings

IDP_ENTITY_ID = os.getenv("ATLASFLOW_SAML_IDP_ENTITY_ID", "")
IDP_SSO_URL = os.getenv("ATLASFLOW_SAML_IDP_SSO_URL", "")
IDP_X509_CERT = os.getenv("ATLASFLOW_SAML_IDP_X509_CERT", "")
BASE_URL = os.getenv("ATLASFLOW_SAML_BASE_URL", "http://localhost:8000")
SP_ENTITY_ID = os.getenv("ATLASFLOW_SAML_SP_ENTITY_ID", f"{BASE_URL}/api/auth/saml/metadata")
ACS_URL = f"{BASE_URL}/api/auth/saml/acs"


def is_enabled() -> bool:
    return bool(IDP_ENTITY_ID and IDP_SSO_URL and IDP_X509_CERT)


def _settings_dict() -> dict:
    return {
        "strict": True,
        "debug": False,
        "sp": {
            "entityId": SP_ENTITY_ID,
            "assertionConsumerService": {
                "url": ACS_URL,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
        },
        "idp": {
            "entityId": IDP_ENTITY_ID,
            "singleSignOnService": {
                "url": IDP_SSO_URL,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": IDP_X509_CERT,
        },
    }


async def _prepare_request_data(request) -> dict:
    """Builds the dict python3-saml expects, from a FastAPI Request."""
    form = {}
    if request.method == "POST":
        form_data = await request.form()
        form = dict(form_data)
    url = request.url
    return {
        "https": "on" if url.scheme == "https" else "off",
        "http_host": url.hostname,
        "server_port": str(url.port or (443 if url.scheme == "https" else 80)),
        "script_name": url.path,
        "get_data": dict(request.query_params),
        "post_data": form,
    }


async def build_auth(request) -> OneLogin_Saml2_Auth:
    req_data = await _prepare_request_data(request)
    return OneLogin_Saml2_Auth(req_data, _settings_dict())


def metadata_xml() -> tuple:
    """Returns (xml_string, errors) - SP metadata for the IdP's configuration."""
    settings = OneLogin_Saml2_Settings(settings=_settings_dict(), sp_validation_only=True)
    metadata = settings.get_sp_metadata()
    errors = settings.validate_metadata(metadata)
    return metadata, errors
