from __future__ import annotations

import dataclasses
import random
from services.interfaces.master_data_protocol import MasterDataProtocol

from entities import Product

PRODUCT_CATEGORIES = ["Seeds", "Fertilizer", "Pesticides", "Tools", "Spare Parts", "Packaging"]


def _stable_int_seed(value: str) -> int:
    import hashlib
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


class ProductSeeder:
    """Encapsulates product, category, UOM, and supplier info seeding logic."""

    def __init__(self, master_seeder: MasterDataProtocol):
        self.master = master_seeder

    def ensure_product_category(self, name: str) -> int:
        key = f"categ:{name}"
        cached = self.master.store.get("product.category", key)
        if cached:
            return cached
        if self.master.dry_run:
            cid = self.master._fake_id("product.category", key)
            self.master.store.set("product.category", key, cid)
            return cid
        recs = self.master.client.search_read(
            "product.category",
            [["name", "=", name]],
            fields=["id", "name"],
            limit=1,
        )
        if recs:
            cid = int(recs[0]["id"])
            self.master.store.set("product.category", key, cid)
            return cid
        cid = self.master.client.create("product.category", {"name": name})
        self.master.store.set("product.category", key, cid)
        return cid

    def ensure_uom(self, *, kind: str) -> tuple[int, str]:
        kind = kind.lower()
        key = f"uom:{kind}"
        cached = self.master.store.get("uom.uom", key)
        if cached:
            name = "kg" if kind == "kg" else "Unit(s)"
            return cached, name

        if self.master.dry_run:
            uid = self.master._fake_id("uom.uom", key)
            name = "kg" if kind == "kg" else "Unit(s)"
            self.master.store.set("uom.uom", key, uid)
            return uid, name

        if kind == "kg":
            candidates = ["kg", "Kilogram", "Kilograms"]
        elif kind == "unit":
            candidates = ["Unit(s)", "Units"]
        else:
            raise ValueError("kind must be kg|unit")

        for c in candidates:
            recs = self.master.client.search_read("uom.uom", [["name", "ilike", c]], fields=["id", "name"], limit=1)
            if recs:
                uid = int(recs[0]["id"])
                self.master.store.set("uom.uom", key, uid)
                return uid, str(recs[0]["name"])
        raise RuntimeError(f"Could not find uom for kind={kind}")

    def ensure_product(self, *, default_code: str, name: str, categ_id: int, uom_id: int, uom_po_id: int) -> Product:
        key = f"prod:{default_code}"
        cached = self.master.store.get("product.product", key)
        if cached:
            rec = self.master.client.read(
                "product.product",
                [cached],
                fields=["id", "default_code", "name", "product_tmpl_id", "uom_id", "categ_id"],
            )[0]
            tmpl_id = int(rec["product_tmpl_id"][0])
            tmpl = self.master.client.read("product.template", [tmpl_id], fields=["uom_id", "categ_id"])[0]
            return Product(
                product_tmpl_id=tmpl_id,
                product_id=int(rec["id"]),
                default_code=str(rec.get("default_code") or default_code),
                name=str(rec["name"]),
                category=str(tmpl["categ_id"][1]),
                uom_id=int(tmpl["uom_id"][0]),
                uom_name=str(tmpl["uom_id"][1]),
            )

        if self.master.dry_run:
            pid = self.master._fake_id("product.product", key)
            tmpl_id = self.master._fake_id("product.template", key)
            self.master.store.set("product.product", key, pid)
            return Product(
                product_tmpl_id=tmpl_id,
                product_id=pid,
                default_code=default_code,
                name=name,
                category="",
                uom_id=uom_id,
                uom_name="",
            )

        recs = self.master.client.search_read(
            "product.product",
            [["default_code", "=", default_code]],
            fields=["id", "default_code", "name", "product_tmpl_id"],
            limit=1,
        )
        if recs:
            pid = int(recs[0]["id"])
            self.master.store.set("product.product", key, pid)
            tmpl_id = int(recs[0]["product_tmpl_id"][0])
            tmpl = self.master.client.read("product.template", [tmpl_id], fields=["uom_id", "categ_id"])[0]
            return Product(
                product_tmpl_id=tmpl_id,
                product_id=pid,
                default_code=default_code,
                name=str(recs[0]["name"]),
                category=str(tmpl["categ_id"][1]),
                uom_id=int(tmpl["uom_id"][0]),
                uom_name=str(tmpl["uom_id"][1]),
            )

        tmpl_id = self.master.client.create(
            "product.template",
            {
                "name": name,
                "type": "product",
                "categ_id": categ_id,
                "uom_id": uom_id,
                "uom_po_id": uom_po_id,
            },
        )
        # Set SKU on the product variant.
        variant = self.master.client.search_read(
            "product.product",
            [["product_tmpl_id", "=", tmpl_id]],
            fields=["id", "default_code"],
            limit=1,
        )
        if not variant:
            raise RuntimeError(f"No product variant created for template {tmpl_id}")
        pid = int(variant[0]["id"])
        self.master.client.write("product.product", [pid], {"default_code": default_code})

        self.master.store.set("product.product", key, pid)
        return Product(
            product_tmpl_id=int(tmpl_id),
            product_id=int(pid),
            default_code=default_code,
            name=name,
            category="",
            uom_id=uom_id,
            uom_name="",
        )

    def set_prices(self, *, product_tmpl_id: int, company_id: int, standard_cost: float, list_price: float) -> None:
        if self.master.dry_run:
            return
        # standard_price is company-dependent; set under company context.
        self.master.client.write(
            "product.template",
            [product_tmpl_id],
            {"standard_price": float(standard_cost), "list_price": float(list_price)},
            allowed_company_ids=[company_id],
            company_id=company_id,
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
        key = f"seller:{product_tmpl_id}:{vendor_id}:{company_id}"
        cached = self.master.store.get("product.supplierinfo", key)
        if cached:
            return cached
        if self.master.dry_run:
            sid = self.master._fake_id("product.supplierinfo", key)
            self.master.store.set("product.supplierinfo", key, sid)
            return sid
        domain = [
            ["product_tmpl_id", "=", product_tmpl_id],
            ["partner_id", "=", vendor_id],
            ["company_id", "=", company_id],
        ]
        recs = self.master.client.search_read(
            "product.supplierinfo",
            domain,
            fields=["id"],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if recs:
            sid = int(recs[0]["id"])
            self.master.store.set("product.supplierinfo", key, sid)
            return sid
        sid = self.master.client.create(
            "product.supplierinfo",
            {
                "partner_id": vendor_id,
                "product_tmpl_id": product_tmpl_id,
                "company_id": company_id,
                "min_qty": 1.0,
                "price": float(price),
                "delay": int(delay_days),
            },
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        self.master.store.set("product.supplierinfo", key, sid)
        return sid

    def seed_products_and_vendors(
        self,
        *,
        company,
        min_products: int = 80,
        max_products: int = 120,
    ) -> tuple[list[Product], dict[str, list[int]]]:
        from services.master_data.geo_data import slugify
        from services.master_data.partner_seeder import PartnerSeeder

        rng = random.Random(_stable_int_seed(f"{self.master.dataset_key}:{company.country_code}:products"))

        categ_ids = {name: self.ensure_product_category(name) for name in PRODUCT_CATEGORIES}
        uom_unit_id, uom_unit_name = self.ensure_uom(kind="unit")
        uom_kg_id, uom_kg_name = self.ensure_uom(kind="kg")

        # Vendor pools by category.
        partner_seeder = PartnerSeeder(self.master)
        vendor_ids_by_category: dict[str, list[int]] = {c: [] for c in PRODUCT_CATEGORIES}
        vendors_per_country = rng.randint(5, 10)
        for i in range(1, vendors_per_country + 1):
            vendor_id = partner_seeder.ensure_partner(
                f"Vendor {company.country_code.upper()} {i:02d}",
                country_code=company.country_code,
                is_vendor=True,
                company_id=company.company_id,
            )
            # Assign each vendor 1-3 primary categories.
            cats = rng.sample(PRODUCT_CATEGORIES, k=rng.randint(1, 3))
            for c in cats:
                vendor_ids_by_category[c].append(vendor_id)

        # Ensure every category has at least one vendor.
        for c in PRODUCT_CATEGORIES:
            if not vendor_ids_by_category[c]:
                vendor_id = partner_seeder.ensure_partner(
                    f"Vendor {company.country_code.upper()} {c} 01",
                    country_code=company.country_code,
                    is_vendor=True,
                    company_id=company.company_id,
                )
                vendor_ids_by_category[c].append(vendor_id)

        target_n = rng.randint(min_products, max_products)
        mix = {
            "Seeds": int(target_n * 0.26),
            "Fertilizer": int(target_n * 0.22),
            "Pesticides": int(target_n * 0.16),
            "Tools": int(target_n * 0.18),
            "Spare Parts": int(target_n * 0.10),
            "Packaging": target_n,
        }
        mix["Packaging"] = max(8, target_n - sum(v for k, v in mix.items() if k != "Packaging"))

        products: list[Product] = []
        seq_by_cat: dict[str, int] = {c: 0 for c in PRODUCT_CATEGORIES}
        for category, count in mix.items():
            for _ in range(count):
                seq_by_cat[category] += 1
                seq = seq_by_cat[category]
                prefix = slugify(category)[:5]
                default_code = f"{prefix}-{seq:03d}"
                name = f"{category} {seq:03d}"
                uom_id = uom_kg_id if category in ("Seeds", "Fertilizer") else uom_unit_id
                uom_po_id = uom_id
                sku_rng = random.Random(_stable_int_seed(f"{self.master.dataset_key}:{default_code}:pricing"))
                if category == "Seeds":
                    pref_cost = sku_rng.uniform(1.5, 6.0)
                    pref_price = pref_cost * sku_rng.uniform(1.25, 1.55)
                elif category == "Fertilizer":
                    pref_cost = sku_rng.uniform(0.6, 2.2)
                    pref_price = pref_cost * sku_rng.uniform(1.18, 1.45)
                elif category == "Pesticides":
                    pref_cost = sku_rng.uniform(4.0, 18.0)
                    pref_price = pref_cost * sku_rng.uniform(1.25, 1.70)
                elif category == "Tools":
                    pref_cost = sku_rng.uniform(6.0, 45.0)
                    pref_price = pref_cost * sku_rng.uniform(1.20, 1.60)
                elif category == "Spare Parts":
                    pref_cost = sku_rng.uniform(2.0, 25.0)
                    pref_price = pref_cost * sku_rng.uniform(1.18, 1.55)
                elif category == "Packaging":
                    pref_cost = sku_rng.uniform(0.10, 1.5)
                    pref_price = pref_cost * sku_rng.uniform(1.30, 1.80)
                else:
                    raise ValueError(category)
                pref_vendor = rng.choice(vendor_ids_by_category[category])
                prod = self.ensure_product(
                    default_code=default_code,
                    name=name,
                    categ_id=categ_ids[category],
                    uom_id=uom_id,
                    uom_po_id=uom_po_id,
                )
                products.append(
                    dataclasses.replace(
                        prod,
                        category=category,
                        uom_name=uom_kg_name if uom_id == uom_kg_id else uom_unit_name,
                    )
                )
                if prod.product_tmpl_id:
                    self.set_prices(
                        product_tmpl_id=prod.product_tmpl_id,
                        company_id=company.company_id,
                        standard_cost=pref_cost,
                        list_price=pref_price,
                    )
                    self.ensure_supplierinfo(
                        product_tmpl_id=prod.product_tmpl_id,
                        vendor_id=pref_vendor,
                        company_id=company.company_id,
                        price=pref_cost * rng.uniform(1.02, 1.15),
                        delay_days=rng.randint(3, 14),
                    )

        return products, vendor_ids_by_category
