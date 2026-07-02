from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Orchestrator config. All from env (prefix MODELSYNC_) or .env."""

    model_config = SettingsConfigDict(env_prefix="MODELSYNC_", env_file=".env")

    # GPUStack server
    gpustack_url: str = "http://localhost:80"
    gpustack_token: str = ""  # Bearer API key
    gpustack_api_prefix: str = "/v2"  # /v2 for GPUStack 2.x, /v1 for older

    # Syncthing on each worker. We assume one shared API key, set at install,
    # and the standard ports. The orchestrator reaches each worker's GUI at
    # http://<worker-ip>:<gui_port>.
    syncthing_api_key: str = ""
    syncthing_gui_port: int = 8384
    syncthing_data_port: int = 22000

    # No cache_dir setting: model paths come straight from the GPUStack API
    # (ModelFile.local_dir, absolute). The only static path is the volume mount
    # in compose/k8s, which must equal GPUStack's own cache mount.

    # After a model finishes syncing to a node, register it in GPUStack (cloning
    # the source model's spec) so GPUStack shows it present there AND its
    # scheduler can place the model on that node. Removing a node deregisters the
    # copy we added (never the user's originals). On by default — this is the
    # whole point of the automation.
    register_in_gpustack: bool = True

    # Background reconcile interval (seconds): re-wire shares, register finished
    # syncs, deregister removed ones. Syncthing/GPUStack events additionally wake
    # the loop immediately, so this is the fallback cadence, not the latency.
    reconcile_interval: int = 15

    # Cap Syncthing transfer rates on every node (KiB/s, 0 = unlimited) so a big
    # model sync can't saturate the LAN while nodes are serving inference.
    sync_max_send_kbps: int = 0
    sync_max_recv_kbps: int = 0

    # Where plan.json + registry.json live (mount a volume here in containers
    # so the plan and our GPUStack registrations survive restarts).
    state_dir: str = "."

    # Bearer token required on the orchestrator's own API. If empty, the API is
    # UNAUTHENTICATED (a warning is logged) — set this for any networked deploy;
    # it mutates GPUStack + every node's Syncthing.
    auth_token: str = ""

    # Requests whose SOCKET peer IP is in these CIDRs skip the token (trusted
    # local access). Matched on the TCP peer only — never a forwarded header, so
    # it can't be spoofed through a proxy. Default: loopback. NOTE: under Docker
    # with a published port, host-local connections arrive from the bridge
    # gateway (e.g. 172.17.0.1), so add that (and/or the host's own IP /32) to
    # make "same machine" tokenless. Do NOT put your whole LAN here unless every
    # device on it may control the cluster.
    auth_exempt_cidrs: str = "127.0.0.0/8,::1/128"

    # Only talk to workers whose IP is in these CIDRs (SSRF guard: a worker's IP
    # comes from GPUStack and we send the shared Syncthing key to it). Defaults =
    # the private ranges, v4 and v6 (fc00::/7 = unique-local). Loopback is NOT
    # included — add 127.0.0.0/8 explicitly for single-host dev.
    allowed_worker_cidrs: str = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7"

    # Only sync model dirs under these roots — rejects an arbitrary path from a
    # compromised/buggy GPUStack from being stood up as a Syncthing share on a
    # root-running daemon. Comma-separated.
    cache_roots: str = "/var/lib/gpustack"

    listen_host: str = "0.0.0.0"
    listen_port: int = 8585


settings = Settings()
