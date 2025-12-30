from __future__ import annotations

from entities import Warehouse
from services.master_data.geo_data import slugify
from services.interfaces.master_data_protocol import MasterDataProtocol


def _short_code(slug: str, *, max_len: int = 5) -> str:
    s = slug.replace("_", "")
    s = s[:max_len]
    return s or "WH"


class WarehouseSeeder:
    """Encapsulates warehouse and location seeding logic."""

    def __init__(self, master_seeder: MasterDataProtocol):
        self.master = master_seeder

    def ensure_warehouse(self, *, company_id: int, company_name: str, wh_name: str) -> Warehouse:
        key = f"wh:{company_name}:{wh_name}"
        cached = self.master.store.get("stock.warehouse", key)
        if cached:
            rec = self.master.client.read(
                "stock.warehouse",
                [cached],
                fields=[
                    "id",
                    "name",
                    "code",
                    "view_location_id",
                    "lot_stock_id",
                    "in_type_id",
                    "int_type_id",
                    "out_type_id",
                ],
                allowed_company_ids=[company_id],
                company_id=company_id,
            )[0]
            return Warehouse(
                warehouse_id=int(rec["id"]),
                name=str(rec["name"]),
                code=str(rec["code"]),
                view_location_id=int(rec["view_location_id"][0]),
                stock_location_id=int(rec["lot_stock_id"][0]),
                picking_type_in_id=int(rec["in_type_id"][0]),
                picking_type_internal_id=int(rec["int_type_id"][0]),
                picking_type_out_id=int(rec["out_type_id"][0]),
            )

        if self.master.dry_run:
            base = _short_code(slugify(wh_name))
            code = base
            if code in self.master._dry_wh_codes:
                for i in range(1, 100):
                    candidate = (base[: max(0, 5 - len(str(i)))] + str(i))[:5]
                    if candidate not in self.master._dry_wh_codes:
                        code = candidate
                        break
            self.master._dry_wh_codes.add(code)
            wid = self.master._fake_id("stock.warehouse", key)
            self.master.store.set("stock.warehouse", key, wid)
            return Warehouse(
                warehouse_id=wid,
                name=wh_name,
                code=code,
                view_location_id=self.master._fake_id("stock.location", f"view:{company_id}:{code}"),
                stock_location_id=self.master._fake_id("stock.location", f"stock:{company_id}:{code}"),
                picking_type_in_id=self.master._fake_id("stock.picking.type", f"in:{company_id}:{code}"),
                picking_type_internal_id=self.master._fake_id("stock.picking.type", f"int:{company_id}:{code}"),
                picking_type_out_id=self.master._fake_id("stock.picking.type", f"out:{company_id}:{code}"),
            )

        domain = [["name", "=", wh_name], ["company_id", "=", company_id]]
        recs = self.master.client.search_read(
            "stock.warehouse",
            domain,
            fields=[
                "id",
                "name",
                "code",
                "view_location_id",
                "lot_stock_id",
                "in_type_id",
                "int_type_id",
                "out_type_id",
            ],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if recs:
            rec = recs[0]
            wid = int(rec["id"])
            self.master.store.set("stock.warehouse", key, wid)
            return Warehouse(
                warehouse_id=wid,
                name=str(rec["name"]),
                code=str(rec["code"]),
                view_location_id=int(rec["view_location_id"][0]),
                stock_location_id=int(rec["lot_stock_id"][0]),
                picking_type_in_id=int(rec["in_type_id"][0]),
                picking_type_internal_id=int(rec["int_type_id"][0]),
                picking_type_out_id=int(rec["out_type_id"][0]),
            )

        # Generate a unique 5-char warehouse code.
        base = _short_code(slugify(wh_name))
        existing = set(
            r["code"]
            for r in self.master.client.search_read(
                "stock.warehouse",
                [["company_id", "=", company_id]],
                fields=["code"],
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
        )
        code = base
        if code in existing:
            for i in range(1, 100):
                candidate = (base[: max(0, 5 - len(str(i)))] + str(i))[:5]
                if candidate not in existing:
                    code = candidate
                    break

        wid = self.master.client.create(
            "stock.warehouse",
            {"name": wh_name, "code": code, "company_id": company_id},
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        rec = self.master.client.read(
            "stock.warehouse",
            [wid],
            fields=[
                "name",
                "code",
                "view_location_id",
                "lot_stock_id",
                "in_type_id",
                "int_type_id",
                "out_type_id",
            ],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )[0]
        self.master.store.set("stock.warehouse", key, wid)
        return Warehouse(
            warehouse_id=int(wid),
            name=str(rec["name"]),
            code=str(rec["code"]),
            view_location_id=int(rec["view_location_id"][0]) if rec["view_location_id"] else 0,
            stock_location_id=int(rec["lot_stock_id"][0]) if rec["lot_stock_id"] else 0,
            picking_type_in_id=int(rec["in_type_id"][0]) if rec["in_type_id"] else 0,
            picking_type_internal_id=int(rec["int_type_id"][0]) if rec["int_type_id"] else 0,
            picking_type_out_id=int(rec["out_type_id"][0]) if rec["out_type_id"] else 0,
        )

    def ensure_internal_location(
        self,
        *,
        company_id: int,
        parent_location_id: int,
        name: str,
    ) -> int:
        key = f"loc:{company_id}:{parent_location_id}:{name}"
        cached = self.master.store.get("stock.location", key)
        if cached:
            return cached
        if self.master.dry_run:
            lid = self.master._fake_id("stock.location", key)
            self.master.store.set("stock.location", key, lid)
            return lid
        domain = [
            ["name", "=", name],
            ["location_id", "=", parent_location_id],
            ["company_id", "=", company_id],
        ]
        recs = self.master.client.search_read(
            "stock.location",
            domain,
            fields=["id", "name"],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if recs:
            lid = int(recs[0]["id"])
            self.master.store.set("stock.location", key, lid)
            return lid
        lid = self.master.client.create(
            "stock.location",
            {
                "name": name,
                "usage": "internal",
                "location_id": parent_location_id,
                "company_id": company_id,
            },
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        self.master.store.set("stock.location", key, lid)
        return lid
