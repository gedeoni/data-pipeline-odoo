"""Extraction helpers for Odoo models."""

from __future__ import annotations

from datetime import datetime
from datetime import date as dt_date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .constants import BATCH_SIZE, STRING_FIELDS
from .odoo_client import OdooClient


def watermark_key(model: str) -> str:
    return f"odoo_watermark__{model}"


def normalize_value(field: str, value: Any) -> Any:
    """Normalize Odoo search_read values into ClickHouse-friendly scalars."""
    if isinstance(value, (list, tuple)) and value:
        value = value[0]
    if field in STRING_FIELDS:
        if value is None or value is False:
            return ""
        if not isinstance(value, str):
            return str(value)
    if isinstance(value, bool):
        return int(value)
    if value is None:
        if field.endswith("_id") or field in {"company_id"}:
            return 0
        if field in {"quantity", "qty_done", "product_uom_qty", "quantity_done", "value", "standard_price", "list_price"}:
            return 0.0
        return None
    if field.endswith("_date") or field in {
        "date",
        "date_done",
        "date_deadline",
        "write_date",
        "create_date",
        "date_expected",
        "date_order",
        "date_planned",
    }:
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                try:
                    return datetime.combine(dt_date.fromisoformat(value), datetime.min.time())
                except ValueError:
                    return value
        return value
    return value


def _chunked(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_ir_property_prices(
    client: OdooClient,
    company_id: Optional[int],
    model: str,
    record_ids: Sequence[int],
) -> Dict[int, float]:
    if not record_ids:
        return {}

    prices: Dict[int, Tuple[float, Optional[int]]] = {}
    fields = ["company_id", "res_id", "value_float"]
    for chunk in _chunked([f"{model},{record_id}" for record_id in record_ids], 1000):
        domain: List[Any] = [
            ["name", "=", "standard_price"],
            ["res_id", "in", list(chunk)],
        ]
        if company_id:
            domain = [
                "|",
                ["company_id", "=", company_id],
                ["company_id", "=", False],
            ] + domain

        for batch in client.paginate(
            model="ir.property",
            domain=domain,
            fields=fields,
            batch_size=BATCH_SIZE,
            order="id asc",
        ):
            for prop in batch:
                res_id = prop.get("res_id") or ""
                try:
                    _, record_id_str = res_id.split(",", 1)
                    record_id = int(record_id_str)
                except (ValueError, TypeError):
                    continue
                value = prop.get("value_float")
                if value is None:
                    continue
                prop_company_id = prop.get("company_id")
                if isinstance(prop_company_id, (list, tuple)) and prop_company_id:
                    prop_company_id = prop_company_id[0]
                if not prop_company_id:
                    prop_company_id = None
                if record_id not in prices or (company_id and prop_company_id == company_id):
                    prices[record_id] = (float(value), prop_company_id)

    return {record_id: price for record_id, (price, _) in prices.items()}


def fetch_standard_prices_for_templates(
    client: OdooClient,
    company_id: Optional[int],
    template_ids: Sequence[int],
) -> Dict[int, float]:
    template_prices = fetch_ir_property_prices(client, company_id, "product.template", template_ids)
    missing_templates = [template_id for template_id in template_ids if template_id not in template_prices]
    if not missing_templates:
        return template_prices

    product_template_map: Dict[int, int] = {}
    for batch in client.paginate(
        model="product.product",
        domain=[["product_tmpl_id", "in", missing_templates]],
        fields=["id", "product_tmpl_id"],
        batch_size=BATCH_SIZE,
        order="id asc",
    ):
        for product in batch:
            product_id = product.get("id")
            template_id = product.get("product_tmpl_id")
            if isinstance(template_id, (list, tuple)) and template_id:
                template_id = template_id[0]
            if not product_id or not template_id:
                continue
            product_template_map[int(product_id)] = int(template_id)

    if not product_template_map:
        return template_prices

    product_prices = fetch_ir_property_prices(
        client,
        company_id,
        "product.product",
        list(product_template_map.keys()),
    )
    for product_id, template_id in product_template_map.items():
        if template_id in template_prices:
            continue
        price = product_prices.get(product_id)
        if price is not None:
            template_prices[template_id] = price

    return template_prices
