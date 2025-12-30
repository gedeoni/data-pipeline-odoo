from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class Warehouse:
    warehouse_id: int
    name: str
    code: str
    view_location_id: int
    stock_location_id: int
    picking_type_in_id: int
    picking_type_internal_id: int
    picking_type_out_id: int


@dataclasses.dataclass(frozen=True)
class Company:
    company_id: int
    name: str
    country_code: str
    customer_id: int
    warehouses: list[Warehouse]
    locations: dict[str, dict[str, int]]
    # locations[warehouse_code]["GOOD|TRANSIT|DAMAGED::<base_slug>"] = location_id


@dataclasses.dataclass(frozen=True)
class Product:
    product_tmpl_id: int
    product_id: int
    default_code: str
    name: str
    category: str
    uom_id: int
    uom_name: str

@dataclasses.dataclass(frozen=True)
class StockPicking:
    origin: str
    company: str
    warehouse: str
    kind: str
    scheduled_date: str
    source_location_id: int
    dest_location_id: int
    lines: int
    note: str = ""


@dataclasses.dataclass(frozen=True)
class StockMove:
    origin: str
    company: str
    warehouse: str
    kind: str
    scheduled_date: str
    product: str
    product_name: str
    category: str
    qty_requested: float
    qty_done: float
    uom: str
    source_location_id: int
    dest_location_id: int
    note: str = ""