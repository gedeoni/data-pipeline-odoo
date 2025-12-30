from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

from dotenv import load_dotenv

from services.master_data.geo_data import geo_plan
from database.odoo_client import OdooClient, OdooConfig
from services.seed_master import MasterSeeder
from services.seed_movements import MovementSeeder


COUNTRY_COMPANY = {
    "rw": "Rwanda",
    "ug": "Uganda",
    "ke": "Kenya",
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed realistic inventory + 180 days stock movements into Odoo 17")
    p.add_argument("--base-url", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    p.add_argument("--db", default=os.getenv("ODOO_DB", "odoo"))
    p.add_argument("--user", default=os.getenv("ODOO_USER", "odoo@gmail.com"))
    p.add_argument("--password", default=os.getenv("ODOO_PASSWORD", "odoo"))

    p.add_argument("--days", type=int, default=180)
    p.add_argument("--scale", choices=["small", "medium", "large"], default="medium")
    p.add_argument("--countries", default="rw,ug,ke", help="Comma-separated: rw,ug,ke")
    p.add_argument("--full-geo", action="store_true", help="Create more base-unit locations per warehouse")
    p.add_argument("--dry-run", action="store_true", help="Generate CSV + logs without calling Odoo")
    p.add_argument("--out-dir", default=os.path.join(os.getcwd(), "seed_output"))
    return p.parse_args(argv)


def check_modules(client: OdooClient) -> None:
    mods = client.search_read(
        "ir.module.module",
        [["name", "in", ["stock", "purchase", "sale"]]],
        fields=["name", "state"],
        limit=50,
    )
    by_name = {m["name"]: m["state"] for m in mods}
    missing = [n for n in ("stock",) if by_name.get(n) not in ("installed", "to upgrade")]
    if missing:
        raise SystemExit(f"Missing required Odoo modules: {missing}. Install Inventory (stock) in the UI first.")


def main(argv: list[str]) -> int:
    load_dotenv()
    args = parse_args(argv)
    countries = [c.strip().lower() for c in args.countries.split(",") if c.strip()]
    # convert this into a validation private function for countries
    for c in countries:
        if c not in COUNTRY_COMPANY:
            raise SystemExit(f"Unsupported country code: {c} (expected rw, ug, ke)")

    end_date = dt.date.today()
    dataset_key = f"{end_date.isoformat()}_{args.days}d"

    client = OdooClient(
        OdooConfig(
            base_url=args.base_url,
            db=args.db,
            login=args.user,
            password=args.password,
        )
    )
    if not args.dry_run:
        client.authenticate()
        check_modules(client)

    master = MasterSeeder(client, dataset_key=dataset_key, dry_run=args.dry_run)
    mover = MovementSeeder(client, dataset_key=dataset_key, dry_run=args.dry_run, out_dir=args.out_dir)

    print(f"Dataset key: {dataset_key}")
    print(f"Dry-run: {args.dry_run}")
    print(f"Output dir: {args.out_dir}")

    summaries: list[tuple[str, dict]] = []
    for country_code in countries:
        company_name = COUNTRY_COMPANY[country_code]
        geo = geo_plan(country_code, scale=args.scale, full_geo=args.full_geo)
        company = master.seed_companies_warehouses_locations(company_name=company_name, country_code=country_code, geo=geo)
        products, vendors_by_cat = master.seed_products_and_vendors(company=company)

        summary = mover.seed_movements(
            company=company,
            products=products,
            vendor_ids_by_category=vendors_by_cat,
            days=args.days,
            scale=args.scale,
        )
        summaries.append((company_name, summary))

    print("\nSummary")
    for company_name, s in summaries:
        print(f"- {company_name}:")
        print(f"  - CSV pickings: {s['pickings_csv']}")
        print(f"  - CSV moves: {s['moves_csv']}")
        print(f"  - Picking counts: {s['picking_counts']}")
        print(f"  - Top outbound SKUs: {s['top_outbound_skus']}")
        print("  - Lowest days-of-cover (approx):")
        for doc, sku, stock, rate in s["lowest_days_of_cover"]:
            print(f"    - {sku}: {doc:.1f} days (stock={stock:.1f}, avg_out/day={rate:.2f})")

    if mover.anomalies:
        print("\nAnomalies injected")
        for a in mover.anomalies:
            print(f"- {a.company} {a.kind} {a.date.isoformat()}: {a.detail}")

    print("\nOdoo modules")
    print("- Required: Inventory (stock)")
    print("- Optional: Purchase (purchase), Sales (sale)\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
