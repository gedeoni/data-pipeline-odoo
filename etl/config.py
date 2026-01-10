"""Config helpers for Odoo connectivity."""

from dataclasses import dataclass
import logging
from urllib.parse import urlparse, urlunparse

from airflow.hooks.base import BaseHook


@dataclass
class OdooConfig:
    url: str
    db: str
    username: str
    password: str
    api_path: str


def get_odoo_config(conn_id: str) -> OdooConfig:
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    protocol = extras.get("protocol", "jsonrpc")
    api_path = extras.get("api_path", "/jsonrpc")
    if protocol != "jsonrpc":
        logging.warning("Protocol %s requested but JSON-RPC is enforced by design", protocol)
    host = conn.host or ""
    scheme = extras.get("scheme", "https")
    if not host.startswith("http"):
        host = f"{scheme}://{host}"
    port = extras.get("port") or conn.port
    parsed = urlparse(host)
    netloc = parsed.netloc
    if port and ":" not in netloc:
        netloc = f"{netloc}:{port}"
        host = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return OdooConfig(
        url=host,
        db=extras.get("db"),
        username=conn.login,
        password=conn.password,
        api_path=api_path,
    )
