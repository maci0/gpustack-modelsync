# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately, **not** as a public issue:

- Open a private advisory via GitHub: **Security → Advisories → Report a
  vulnerability** on this repository, or
- email the maintainer (see the commit history / profile).

Include a description, affected version/commit, and a reproduction if possible.
You'll get an acknowledgement, and a fix or mitigation coordinated before any
public disclosure.

## Scope and threat model

The orchestrator holds a GPUStack bearer token and a Syncthing API key, and it
**mutates GPUStack and reconfigures Syncthing on every worker**. Treat its API as
a control plane:

- **Authentication.** Set `MODELSYNC_AUTH_TOKEN`. All API routes then require it
  (`X-Auth-Token` / `Bearer`; `/events` via a `?token=` query, which is redacted
  from access logs), compared in constant time. An empty token is refused unless
  `LISTEN_HOST` is loopback (fail-closed at startup). Only `/`, `/app.js`, and
  `/userscript.js` are unauthenticated.
- **Trusted-peer exemption** (`AUTH_EXEMPT_CIDRS`) is matched on the **socket
  peer IP only**, never a forwarded header, so it can't be spoofed through a
  proxy.
- **SSRF / key-exfil guard.** Worker IPs from GPUStack are CIDR-allowlisted
  (`ALLOWED_WORKER_CIDRS`) before the Syncthing key is ever sent to them.
- **Path containment.** Every path stood up as a Syncthing share, deleted, pinned
  or purged must be strictly under `CACHE_ROOTS` (checked after `..`
  normalization), so a compromised GPUStack record can't point operations at
  arbitrary directories.
- **XSS.** All GPUStack-derived strings are HTML-escaped and a CSP forbids
  inline/foreign scripts.
- **Transport.** Prefer `https://` for `GPUSTACK_URL` and set
  `SSL_CERTFILE`/`SSL_KEYFILE` to serve the orchestrator over TLS; otherwise the
  bearer token and API key travel in cleartext (a startup warning fires). Keep
  the Syncthing GUI on the private cluster network, and prefer per-node keys
  (`SYNCTHING_API_KEYS`) over one shared key.

## Supported versions

This is pre-1.0; fixes land on `main` and the latest tagged release. Pin the
image by digest for reproducible deploys.
