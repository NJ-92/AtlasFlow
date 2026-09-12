# AtlasFlow — Security

## 1. Authentication

AtlasFlow supports two ways to sign in:

**Local username/password** (always available): every API call (except `/api/auth/login`,
`/api/auth/sso/*`, and `/api/health`) requires a bearer token.

```
POST /api/auth/login
{ "username": "admin", "password": "..." }

→ { "access_token": "<jwt>", "token_type": "bearer", "role": "admin", "username": "admin" }
```

**OIDC SSO** (optional): set `ATLASFLOW_OIDC_ISSUER`, `ATLASFLOW_OIDC_CLIENT_ID`,
`ATLASFLOW_OIDC_CLIENT_SECRET`, and `ATLASFLOW_OIDC_REDIRECT_URI` (see `.env.example`) to
enable a "Sign in with SSO" button that redirects to your identity provider. Works with
any standard OIDC provider - Okta, Azure AD/Entra ID, Google Workspace, Auth0, Ping, etc.
This is the modern, more broadly-supported equivalent of classic SAML SSO; if you
specifically need SAML, most IdPs (including Okta and Azure AD) can act as an
OIDC-to-SAML bridge in front of AtlasFlow. Automated user provisioning (SCIM) is not
implemented - new SSO users are auto-created as `viewer` on first login, and an admin
promotes them from the Users tab as needed.

Send the token on every subsequent request: `Authorization: Bearer <jwt>`. Tokens expire
after `ATLASFLOW_JWT_EXPIRE_MINUTES` (default 480 = 8 hours) and encode the user's id,
username, and role — signed with `ATLASFLOW_JWT_SECRET` (HS256). Rotating that secret
immediately invalidates every outstanding token, which is the fastest way to force a
global logout.

**Bootstrap account:** on first boot, if no users exist, AtlasFlow creates `admin` with the
password from `ATLASFLOW_ADMIN_BOOTSTRAP_PASSWORD` (default `atlasflow-admin` — change this
before first boot, or change the password immediately after logging in).

## 2. Roles

Three roles, each a strict superset of the one before it:

| Role | Can do |
|---|---|
| `viewer` | List/view workflows, runs, logs, and secret *names* (never values) |
| `editor` | Everything viewer can, plus: create workflows, trigger runs, pause/resume, create/update secrets |
| `admin` | Everything editor can, plus: delete workflows, delete secrets, create/list users, view the audit log |

Roles are enforced server-side on every route via the `require_role(...)` FastAPI
dependency (`app/security.py`) — the frontend hiding a button is a UX nicety, not the
security boundary.

Create additional users via `POST /api/auth/users` (admin-only) or the **Users** tab in the
UI.

## 2b. Table-level access control (governance)

Every catalog table (Data tab) has a `visibility`: **public** (any `viewer`+ can read it,
the default) or **restricted** (only its owner, admins, and users explicitly granted
access can read it). Manage this from the **access** button on a restricted table in the
Data tab, or via the API:
```
POST /api/tables/{id}/visibility?visibility=restricted
POST /api/tables/{id}/grants   { "username": "alice" }
```
This is enforced everywhere a table can be read: the SQL tab, the Python tab (restricted
tables are simply omitted from the `tables` dict), and dashboard tile refreshes.

**Honest limit**: this is table-level, not row/column-level. A user with read access to a
table sees the whole thing. Workflow-defined `sql`/`spark_submit` tasks run with the
workflow creator's authority, the same trust model documented in §6 below - table ACLs
govern the interactive Data/SQL/Python surface, not what a workflow's own code can touch.

**SAML SSO** (optional, alongside OIDC): set `ATLASFLOW_SAML_IDP_ENTITY_ID`,
`ATLASFLOW_SAML_IDP_SSO_URL`, and `ATLASFLOW_SAML_IDP_X509_CERT` to enable a "Sign in with
SSO (SAML)" button. Implemented via the standard `python3-saml` toolkit (real XML
signature validation). **Honest caveat**: this has not been exercised against a live IdP
in this environment (no network access here to test end-to-end) — validate it against
your actual identity provider (clock skew, certificate format, and IdP-specific quirks are
the usual SAML integration pain points) before relying on it.

**SCIM provisioning** (optional): set `ATLASFLOW_SCIM_TOKEN` to a random secret and give
that same value to your IdP as its SCIM bearer token, pointed at
`http://<host>:8000/scim/v2`. Supports the operations Okta/Azure AD actually use: list,
get, create, replace, patch (activate/deactivate), delete (which deactivates rather than
hard-deletes, preserving audit history). New SCIM-provisioned users start as `viewer`.

## 2c. Auto-scale controller — real, but read the tradeoff

Setting `ATLASFLOW_AUTOSCALE_ENABLED=true` starts a controller that creates/removes real
Spark worker containers based on load, using the Docker Engine API. This requires mounting
`/var/run/docker.sock` into the backend container (already wired in `docker-compose.yml`,
commented with this warning) — **that mount gives the backend container effective root
access to your Docker host**. Anything that can talk to the Docker socket can, in
practice, escape to the host. Only enable this on infrastructure where you're comfortable
granting AtlasFlow's backend that level of access. It's also bounded by your host's
physical CPU/RAM regardless — see `app/autoscaler.py`'s docstring for what this is and
isn't.

## 2d. Billing — real Stripe integration, know what it actually charges for

If `ATLASFLOW_STRIPE_SECRET_KEY`/`ATLASFLOW_STRIPE_PRICE_ID` are set, the Billing tab lets
a user subscribe to real metered billing via Stripe Checkout, and every completed workflow
run reports its estimated cost as usage against their subscription — actual charges, not a
simulation. Two things worth being deliberate about:
- This has not been exercised against a live Stripe account here (no network access to
  test) — use Stripe test mode first.
- "Billing" here means charging individual users of one shared AtlasFlow instance for
  their compute usage. It does **not** give you multi-tenant data isolation — everyone on
  the instance still shares the same workflows/tables subject to the RBAC/table-ACL rules
  above. If you need to prevent separate paying customers from seeing each other's data
  entirely, that's a materially larger architecture change than billing alone.

## 3. Secrets management

Never put credentials directly in a task's `command` or `params`. Instead:

1. Store it once: `POST /api/secrets { "name": "snowflake_password", "value": "..." }`
   (editor+). The value is encrypted with Fernet (`ATLASFLOW_ENCRYPTION_KEY`) before being
   written to Postgres — the plaintext is never persisted.
2. Reference it in any task: `{{secret:snowflake_password}}` — inside `command` or any
   string value in `params`. The executor resolves it to the decrypted value immediately
   before running the task, on the orchestrator host only.
3. **Log redaction**: after the task runs, every resolved secret value is scrubbed back out
   of the logs before they're stored — so even if a task accidentally echoes a secret to
   stdout, it won't show up in run history.

Secret values are **never returned by any API response** — `GET /api/secrets` only returns
names and creation metadata. There's no "reveal" endpoint.

## 4. Audit logging

Every login (success and failure) and every mutating action (`workflow.create`,
`workflow.trigger`, `workflow.delete`, `secret.create`, `user.create`, etc.) is written to
the `audit_logs` table with the acting username, timestamp, source IP, and a `details`
blob. View it at `GET /api/audit-logs` or the **Audit** tab (admin-only).

This log is intentionally append-only from the API surface — there's no delete/edit
endpoint for it.

## 5. Network & transport hardening

Handled outside the app, since it depends on your deployment:

- **TLS**: put a reverse proxy (nginx, Caddy, Traefik, or your cloud LB) in front of both
  `frontend` and `backend` and terminate HTTPS there. The containers themselves speak
  plain HTTP internally, which is fine on a private Docker network but should never be
  exposed directly to the internet.
- **CORS**: `ATLASFLOW_CORS_ORIGINS` restricts which origins the API will accept
  browser requests from — set it to your real frontend URL(s), not `*`.
- **Rate limiting**: `/api/auth/login` is limited to 5 attempts/minute per IP
  (`slowapi`) to blunt credential-stuffing/brute-force attempts. Extend this to other
  endpoints in `app/main.py` if you expose the API publicly.
- **Jupyter has no auth by default**: `docker-compose.yml` starts the notebook server with
  `--NotebookApp.token='' --NotebookApp.password=''` for frictionless local use. Before
  exposing port 8888 beyond localhost, set a real token/password (see the
  [Jupyter docs](https://jupyter-notebook.readthedocs.io/en/stable/public_server.html)) or
  put it behind the same reverse-proxy auth as everything else.
- **Spark Thrift Server (BI tool JDBC/ODBC access)**: port 10000 has no authentication
  configured by default, matching the rest of the local-dev setup. Before exposing it
  beyond localhost, put it behind network-level access control (VPN/firewall) - Spark's
  Thrift Server supports LDAP/Kerberos auth if you need per-user access control for BI
  tools; that's a Spark-level config change, not an AtlasFlow one.
- **Firewalling internal services**: `postgres` (5432), `minio` (9000/9001), the Spark
  ports (7077/8080), and the Thrift Server (10000) are exposed on the host for local
  development convenience. In a real deployment, keep them on the internal Docker network
  only (drop their `ports:` mappings in `docker-compose.yml`) and access them via a
  VPN/bastion when needed.

## 6. Task execution is still a trust boundary — know this

`python` and `shell` task types execute arbitrary code on the orchestrator host, and
`spark_submit` submits arbitrary jobs to the cluster. RBAC controls *who can create/trigger
workflows*, not what a workflow is allowed to do once it runs — an `editor` can write a
workflow that does anything the orchestrator's own host permissions allow. Treat "editor"
as roughly equivalent to "trusted engineer with shell access," the same trust level
Databricks assumes for anyone with cluster-attach permission on an all-purpose cluster. If
you need to run truly untrusted code, that requires sandboxing (gVisor/Firecracker-style
isolation) that AtlasFlow does not provide today.

## 7. Checklist before going beyond localhost

- [ ] Set `ATLASFLOW_JWT_SECRET` to a random 32+ byte value (`openssl rand -hex 32`)
- [ ] Set `ATLASFLOW_ENCRYPTION_KEY` explicitly (see `.env.example`) — if left unset, a new
      random key is generated on every container restart and previously-encrypted secrets
      become unreadable
- [ ] Change the default admin password (or set `ATLASFLOW_ADMIN_BOOTSTRAP_PASSWORD` before
      first boot)
- [ ] Set `ATLASFLOW_CORS_ORIGINS` to your real frontend origin
- [ ] Change `POSTGRES_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- [ ] Put TLS in front of both `frontend` and `backend`
- [ ] If using SAML, validate it against your actual IdP in a test environment first
- [ ] If using the autoscaler, confirm you're comfortable with the docker.sock access tradeoff (§2c) — otherwise leave `ATLASFLOW_AUTOSCALE_ENABLED=false` and comment out the socket mount
- [ ] If using Stripe billing, confirm you're in test mode until verified, then switch to live keys deliberately
- [ ] Stop publishing `postgres`/`minio`/`spark-master` ports to the host if not needed locally
- [ ] Set a real Jupyter token/password (disabled by default for local convenience)
- [ ] Decide your policy on who gets `editor`/`admin` — see §6 above
