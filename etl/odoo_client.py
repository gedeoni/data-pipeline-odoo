"""Odoo JSON-RPC client utilities."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

from .config import OdooConfig
from .constants import BACKOFF_BASE_SECONDS, MAX_RETRIES, REQUEST_TIMEOUT


class OdooClient:
    """Thin JSON-RPC Odoo client with retries, pagination, and safe backoff."""

    def __init__(self, config: OdooConfig):
        self.config = config
        self._uid: Optional[int] = None
        self._session = requests.Session()

    @property
    def endpoint(self) -> str:
        return f"{self.config.url.rstrip('/')}/{self.config.api_path.strip('/')}"

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._session.post(
                    self.endpoint,
                    json=payload,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data
            except Exception as exc:
                if attempt >= MAX_RETRIES:
                    raise
                sleep_for = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                logging.warning("Odoo JSON-RPC retry %s/%s due to %s", attempt, MAX_RETRIES, exc)
                time.sleep(sleep_for)
        raise RuntimeError("Exceeded retries")

    def authenticate(self) -> int:
        if self._uid is not None:
            return self._uid
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "common",
                "method": "login",
                "args": [self.config.db, self.config.username, self.config.password],
            },
            "id": 1,
        }
        result = self._post(payload)
        self._uid = result.get("result")
        if not self._uid:
            raise RuntimeError("Odoo authentication failed")
        return self._uid

    def search_read(
        self,
        model: str,
        domain: List[Any],
        fields: List[str],
        limit: int,
        offset: int,
        order: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [
                    self.config.db,
                    uid,
                    self.config.password,
                    model,
                    "search_read",
                    [domain],
                    {
                        "fields": fields,
                        "limit": limit,
                        "offset": offset,
                        "order": order or "id asc",
                    },
                ],
            },
            "id": 2,
        }
        result = self._post(payload)
        return result.get("result", [])

    def create(self, model: str, values: Dict[str, Any]) -> int:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [self.config.db, uid, self.config.password, model, "create", [values]],
            },
            "id": 3,
        }
        result = self._post(payload)
        return result.get("result")

    def write(self, model: str, record_ids: List[int], values: Dict[str, Any]) -> bool:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [
                    self.config.db,
                    uid,
                    self.config.password,
                    model,
                    "write",
                    [record_ids, values],
                ],
            },
            "id": 4,
        }
        result = self._post(payload)
        return bool(result.get("result"))

    def paginate(
        self,
        model: str,
        domain: List[Any],
        fields: List[str],
        batch_size: int,
        order: str,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Yield batches to avoid large payloads and protect the API."""
        offset = 0
        while True:
            records = self.search_read(
                model=model,
                domain=domain,
                fields=fields,
                limit=batch_size,
                offset=offset,
                order=order,
            )
            if not records:
                break
            yield records
            offset += batch_size
