from __future__ import annotations

from typing import Any, Protocol

# Hint imports for type-checkers; not required at runtime
from database.odoo_client import IdempotentStore, OdooClient


class MasterDataProtocol(Protocol):
    dry_run: bool
    client: OdooClient
    store: IdempotentStore
    _dry_wh_codes: set[str]

    def _fake_id(self, model: str, key: str) -> int: ...
