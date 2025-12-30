from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional, Tuple

from entities import Company, Product, Warehouse
from services.master_data.geo_data import WarehouseGeo, readable
from database.odoo_client import IdempotentStore, OdooClient
from services.master_data.company_seeder import CompanySeeder
from services.master_data.partner_seeder import PartnerSeeder
from services.master_data.product_seeder import ProductSeeder
from services.master_data.warehouse_seeder import WarehouseSeeder


def _stable_int_seed(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


class MasterSeeder:
    def __init__(self, client: OdooClient, *, dataset_key: str, dry_run: bool = False):
        self.client = client
        self.dataset_key = dataset_key
        self.dry_run = dry_run
        self.store = IdempotentStore()
        self._dry_wh_codes: set[str] = set()

        # Initialize service classes
        self.company_seeder = CompanySeeder(self)
        self.partner_seeder = PartnerSeeder(self)
        self.product_seeder = ProductSeeder(self)
        self.warehouse_seeder = WarehouseSeeder(self)

    def _fake_id(self, model: str, key: str) -> int:
        return int(_stable_int_seed(f"{model}:{key}") % 900_000_000 + 100_000_000)

    # Delegate methods to service classes for backward compatibility
    def ensure_country_id(self, country_code: str) -> int:
        return self.company_seeder.ensure_country_id(country_code)

    def ensure_company(self, name: str, *, country_code: str) -> int:
        return self.company_seeder.ensure_company(name, country_code=country_code)

    def ensure_partner(self, name: str, *, country_code: str, is_vendor: bool, company_id: int | None = None) -> int:
        return self.partner_seeder.ensure_partner(name, country_code=country_code, is_vendor=is_vendor, company_id=company_id)

    def ensure_product_category(self, name: str) -> int:
        return self.product_seeder.ensure_product_category(name)

    def ensure_uom(self, *, kind: str) -> tuple[int, str]:
        return self.product_seeder.ensure_uom(kind=kind)

    def ensure_warehouse(self, *, company_id: int, company_name: str, wh_name: str) -> Warehouse:
        return self.warehouse_seeder.ensure_warehouse(company_id=company_id, company_name=company_name, wh_name=wh_name)

    def ensure_internal_location(
        self,
        *,
        company_id: int,
        parent_location_id: int,
        name: str,
    ) -> int:
        return self.warehouse_seeder.ensure_internal_location(
            company_id=company_id, parent_location_id=parent_location_id, name=name
        )

    def ensure_product(self, *, default_code: str, name: str, categ_id: int, uom_id: int, uom_po_id: int) -> Product:
        return self.product_seeder.ensure_product(
            default_code=default_code, name=name, categ_id=categ_id, uom_id=uom_id, uom_po_id=uom_po_id
        )

    def set_prices(self, *, product_tmpl_id: int, company_id: int, standard_cost: float, list_price: float) -> None:
        return self.product_seeder.set_prices(
            product_tmpl_id=product_tmpl_id, company_id=company_id, standard_cost=standard_cost, list_price=list_price
        )

    def ensure_supplierinfo(
        self,
        *,
        product_tmpl_id: int,
        vendor_id: int,
        company_id: int,
        price: float,
        delay_days: int,
    ) -> int:
        return self.product_seeder.ensure_supplierinfo(
            product_tmpl_id=product_tmpl_id,
            vendor_id=vendor_id,
            company_id=company_id,
            price=price,
            delay_days=delay_days,
        )

    def seed_companies_warehouses_locations(
        self,
        *,
        company_name: str,
        country_code: str,
        geo: list[WarehouseGeo],
    ) -> Company:
        return self.company_seeder.seed_companies_warehouses_locations(
            company_name=company_name, country_code=country_code, geo=geo
        )

    def seed_products_and_vendors(
        self,
        *,
        company: Company,
        min_products: int = 80,
        max_products: int = 120,
    ) -> tuple[list[Product], dict[str, list[int]]]:
        return self.product_seeder.seed_products_and_vendors(
            company=company, min_products=min_products, max_products=max_products
        )
