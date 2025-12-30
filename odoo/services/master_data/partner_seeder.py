from __future__ import annotations

from typing import Any

from services.interfaces.master_data_protocol import MasterDataProtocol


class PartnerSeeder:
    """Encapsulates partner (vendor/customer) seeding logic."""

    def __init__(self, master_seeder: MasterDataProtocol):
        self.master = master_seeder

    def ensure_partner(self, name: str, *, country_code: str, is_vendor: bool, company_id: int | None = None) -> int:
        key = f"partner:{name}"
        cached = self.master.store.get("res.partner", key)
        if cached:
            return cached

        if self.master.dry_run:
            pid = self.master._fake_id("res.partner", key)
            self.master.store.set("res.partner", key, pid)
            return pid

        domain = [["name", "=", name]]
        recs = self.master.client.search_read("res.partner", domain, fields=["id", "name"], limit=1)
        if recs:
            pid = int(recs[0]["id"])
            self.master.store.set("res.partner", key, pid)
            return pid

        from services.master_data.company_seeder import CompanySeeder
        company_seeder = CompanySeeder(self.master)
        country_id = company_seeder.ensure_country_id(country_code)
        vals: dict[str, Any] = {
            "name": name,
            "country_id": country_id,
        }
        if is_vendor:
            vals["supplier_rank"] = 1
        else:
            vals["customer_rank"] = 1
        # Partners are typically shared across companies; keep company_id unset.
        pid = self.master.client.create("res.partner", vals, company_id=company_id)
        self.master.store.set("res.partner", key, pid)
        return pid
