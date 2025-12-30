from __future__ import annotations

import dataclasses
import json
import random
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


class OdooRPCError(RuntimeError):
    def __init__(self, message: str, *, data: Any | None = None):
        super().__init__(message)
        self.data = data


@dataclasses.dataclass(frozen=True)
class OdooConfig:
    base_url: str
    db: str
    login: str
    password: str
    timeout_s: int = 60
    max_retries: int = 6


def _jitter_sleep_s(attempt: int) -> float:
    base = min(2**attempt, 30)
    return base + random.random() * 0.250


class OdooClient:
    def __init__(self, cfg: OdooConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._uid: int | None = None
        self._session_info: dict[str, Any] | None = None
        self._rpc_id = 0

    @property
    def uid(self) -> int:
        if self._uid is None:
            raise RuntimeError("Not authenticated; call authenticate()")
        return self._uid

    def authenticate(self) -> dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "db": self.cfg.db,
                "login": self.cfg.login,
                "password": self.cfg.password,
            },
            "id": self._next_id(),
        }
        result = self._post_json("/web/session/authenticate", payload)
        if not result.get("result") or not result["result"].get("uid"):
            raise OdooRPCError(f"Authentication failed: {result}")
        self._uid = int(result["result"]["uid"])
        self._session_info = result["result"]
        return result["result"]

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = self.cfg.base_url.rstrip("/") + path
        last_err: Exception | None = None
        for attempt in range(self.cfg.max_retries):
            try:
                resp = self.session.post(url, data=json.dumps(payload), timeout=self.cfg.timeout_s)
                if resp.status_code >= 500:
                    raise OdooRPCError(f"HTTP {resp.status_code} from Odoo", data=resp.text)
                data = resp.json()
                if data.get("error"):
                    raise OdooRPCError(f"Odoo RPC error: {data['error']}", data=data["error"])
                return data
            except (requests.RequestException, json.JSONDecodeError, OdooRPCError) as e:
                last_err = e
                if attempt >= self.cfg.max_retries - 1:
                    break
                time.sleep(_jitter_sleep_s(attempt))
        raise OdooRPCError(f"RPC call failed after retries: {path}") from last_err

    def call_kw(
        self,
        model: str,
        method: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        *,
        context: dict[str, Any] | None = None,
        allowed_company_ids: list[int] | None = None,
        company_id: int | None = None,
    ) -> Any:
        if self._uid is None:
            self.authenticate()
        args = args or []
        kwargs = kwargs or {}
        ctx: dict[str, Any] = dict(context or {})
        if allowed_company_ids is not None:
            ctx["allowed_company_ids"] = allowed_company_ids
        if company_id is not None:
            ctx["company_id"] = company_id
            # Odoo 17+ warns that `force_company` is no longer supported.
        if ctx:
            kwargs["context"] = ctx
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": model,
                "method": method,
                "args": args,
                "kwargs": kwargs,
            },
            "id": self._next_id(),
        }
        result = self._post_json("/web/dataset/call_kw", payload)
        return result.get("result")

    def search(self, model: str, domain: list[Any], *, limit: int | None = None, **ctx) -> list[int]:
        kwargs: dict[str, Any] = {}
        if limit is not None:
            kwargs["limit"] = limit
        return self.call_kw(model, "search", args=[domain], kwargs=kwargs, **ctx)

    def read(self, model: str, ids: list[int], fields: list[str] | None = None, **ctx) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {}
        if fields is not None:
            kwargs["fields"] = fields
        return self.call_kw(model, "read", args=[ids], kwargs=kwargs, **ctx)

    def search_read(
        self,
        model: str,
        domain: list[Any],
        fields: list[str] | None = None,
        *,
        limit: int | None = None,
        order: str | None = None,
        **ctx,
    ) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {}
        if fields is not None:
            kwargs["fields"] = fields
        if limit is not None:
            kwargs["limit"] = limit
        if order is not None:
            kwargs["order"] = order
        return self.call_kw(model, "search_read", args=[domain], kwargs=kwargs, **ctx)

    def create(self, model: str, values: dict[str, Any], **ctx) -> int:
        return int(self.call_kw(model, "create", args=[values], **ctx))

    def write(self, model: str, ids: list[int], values: dict[str, Any], **ctx) -> bool:
        return bool(self.call_kw(model, "write", args=[ids, values], **ctx))

    def unlink(self, model: str, ids: list[int], **ctx) -> bool:
        return bool(self.call_kw(model, "unlink", args=[ids], **ctx))


class IdempotentStore:
    def __init__(self):
        self._cache: dict[tuple[str, str], int] = {}

    def get(self, model: str, key: str) -> int | None:
        return self._cache.get((model, key))

    def set(self, model: str, key: str, record_id: int) -> None:
        self._cache[(model, key)] = record_id
