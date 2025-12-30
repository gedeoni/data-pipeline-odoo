from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from entities import Product, Warehouse


@dataclass(frozen=True)
class AnomalyEvent:
    kind: str
    company: str
    detail: str
    date: dt.date


@dataclass(frozen=True)
class InboundWarehouseContext:
    """Per-warehouse planning context for inbound receipts."""

    warehouse: Warehouse
    profile: "WarehouseProfile"
    weight: float
    months: int
    receipt_days: list[dt.date]
    delayed_days: set[dt.date]


@dataclass(frozen=True)
class InternalWarehouseContext:
    """Per-warehouse planning context for internal transfers."""

    warehouse: Warehouse
    profile: "WarehouseProfile"
    weight: float
    transfer_days: list[dt.date]


@dataclass(frozen=True)
class DamageWarehouseContext:
    """Per-warehouse planning context for damage/shrinkage events."""

    warehouse: Warehouse
    profile: "WarehouseProfile"
    weight: float
    event_days: list[dt.date]


@dataclass(frozen=True)
class OutboundWarehouseContext:
    """Per-warehouse planning context for outbound sales."""

    warehouse: Warehouse
    profile: "WarehouseProfile"
    weight: float


@dataclass(frozen=True)
class InternalTransferDetails:
    product: Product
    qty: float
    src_loc_id: int
    transit_loc_id: int
    dst_loc_id: int


@dataclass(frozen=True)
class WarehouseProfile:
    size: str
    weight: float
    active_products: list[Product]
