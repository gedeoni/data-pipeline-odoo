"""Airflow DAG: Odoo -> ClickHouse inventory analytics and BI marts.

Design notes:
- Extract once to raw ClickHouse tables, reuse for marts via SQL.
- Incremental extraction with per-model watermarks stored in Airflow Variables.
- Idempotent loads via ReplacingMergeTree on write_date for raw tables.
- Keep XCom payloads small (counts + watermark only).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from collections import defaultdict
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from etl.clickhouse import execute_sql, get_clickhouse_client, insert_rows
from etl.config import get_odoo_config
from etl.constants import (
    BATCH_SIZE,
    CLICKHOUSE_CONN_ID,
    DAG_ID,
    DEFAULT_ARGS,
    EMAIL_CONN_ID,
    MODEL_FIELD_ALIASES,
    ODOO_CONN_ID,
    RAW_TABLES,
    VAR_ABC_A_PCT,
    VAR_ABC_B_PCT,
    VAR_ABC_C_PCT,
    VAR_ANOMALY_PCT,
    VAR_DEAD_STOCK_DAYS,
    VAR_FORECAST_SCOPE,
    VAR_FULL_REFRESH_MODELS,
    VAR_HELPDESK_TEAM_ID,
    VAR_OTIF_THRESHOLD,
    VAR_PROCUREMENT_EMAIL,
)
from etl.extract import fetch_standard_prices_for_templates, normalize_value, watermark_key
from etl.odoo_client import OdooClient
from etl.sql import (
    MART_SQL_ABC,
    MART_SQL_COST_ANOMALIES,
    MART_SQL_INVENTORY_TURNOVER,
    MART_SQL_LIQUIDATION,
    MART_SQL_STOCKOUT_RISK,
    MART_SQL_TOUCH_RATIO,
    MART_SQL_VENDOR,
    MART_TABLE_DDL,
    RAW_TABLE_DDL,
)

# Demand forecasting uses Python for flexibility; results inserted into mart_demand_forecast.

# -------------------- DAG --------------------


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
# Odoo to ClickHouse Analytics

- Extract canonical Odoo entities to ClickHouse raw tables once per run.
- Build marts directly in ClickHouse SQL for Superset consumption.
- Uses incremental watermarks per model stored in Airflow Variables.
""",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    @task
    def init_clickhouse_tables() -> None:
        client = get_clickhouse_client(CLICKHOUSE_CONN_ID)
        execute_sql(client, RAW_TABLE_DDL)
        execute_sql(client, MART_TABLE_DDL)

    @task
    def fetch_active_companies() -> List[int]:
        """Fetch list of all active company IDs from Odoo, minus excluded ones."""
        config = get_odoo_config(ODOO_CONN_ID)
        client = OdooClient(config)
        companies = client.search_read("res.company", [], ["id"], limit=100, offset=0)
        excluded_raw = Variable.get("excluded_company_ids", default_var="[]")
        try:
            excluded_ids = {int(cid) for cid in json.loads(excluded_raw)}
        except (ValueError, json.JSONDecodeError, TypeError):
            excluded_ids = set()
        return [c["id"] for c in companies if c["id"] not in excluded_ids]

    @task
    def extract_model_to_clickhouse(
        model: str,
        fields: List[str],
        domain: List[Any],
        order: str,
        required_fields: List[str],
        company_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Extract incrementally from Odoo API and insert into ClickHouse raw table.

        Uses write_date watermark for idempotent loads and stores it only after load succeeds.
        """
        config = get_odoo_config(ODOO_CONN_ID)

        # If running per-company, force the Odoo context to that specific company.
        if company_id:
            config.company_id = company_id
            config.allowed_company_ids = [company_id]

        client = OdooClient(config)
        ch_client = get_clickhouse_client(CLICKHOUSE_CONN_ID)

        watermark_key_name = watermark_key(model)
        if company_id:
            watermark_key_name = f"{watermark_key_name}_{company_id}"
        full_refresh_raw = Variable.get(VAR_FULL_REFRESH_MODELS, default_var="[]")
        try:
            full_refresh_models = {m for m in json.loads(full_refresh_raw) if isinstance(m, str)}
        except (json.JSONDecodeError, TypeError):
            full_refresh_models = {m.strip() for m in str(full_refresh_raw).split(",") if m.strip()}

        if model in full_refresh_models:
            last_watermark = "1970-01-01 00:00:00"
            incremental_domain = list(domain)
        else:
            last_watermark = Variable.get(watermark_key_name, default_var="1970-01-01 00:00:00")
            incremental_domain = list(domain)
            incremental_domain.append(["write_date", ">", last_watermark])

        table = RAW_TABLES[model]
        aliases = MODEL_FIELD_ALIASES.get(model, {})
        total_inserted = 0
        max_write_date = last_watermark
        invalid_rows = 0

        for batch in client.paginate(
            model=model,
            domain=incremental_domain,
            fields=fields,
            batch_size=BATCH_SIZE,
            order=order,
        ):
            # Protect Odoo by rate limiting large scans.
            time.sleep(0.2)

            standard_price_overrides: Dict[int, float] = {}
            if model == "product.template":
                template_ids = [rec.get("id") for rec in batch if rec.get("id")]
                standard_price_overrides = fetch_standard_prices_for_templates(
                    client,
                    company_id,
                    template_ids,
                )

            rows = []
            for rec in batch:
                write_date = rec.get("write_date") or last_watermark
                if write_date > max_write_date:
                    max_write_date = write_date

                # If running per-company, ensure the record is attributed to that company
                # even if Odoo returns False/0 (which happens for shared records like products).
                if company_id and not rec.get("company_id"):
                    rec["company_id"] = company_id

                # Basic data-quality guardrails to avoid null keys and dates in raw tables.
                normalized = {field: normalize_value(field, rec.get(field)) for field in fields}
                for source_field, target_field in aliases.items():
                    if source_field in normalized:
                        normalized[target_field] = normalized.pop(source_field)
                if model == "product.template":
                    template_id = normalized.get("id")
                    if template_id in standard_price_overrides:
                        normalized["standard_price"] = standard_price_overrides[template_id]
                    if normalized.get("standard_price") in (0, 0.0) and normalized.get("list_price"):
                        normalized["standard_price"] = float(normalized["list_price"]) * 0.7
                if model in full_refresh_models and "write_date" in normalized:
                    # Force a fresh version so ReplacingMergeTree keeps the new values.
                    normalized["write_date"] = datetime.utcnow()
                if any(not normalized.get(field) for field in required_fields):
                    invalid_rows += 1
                    continue
                rows.append(normalized)

            inserted = insert_rows(ch_client, table, rows)
            total_inserted += inserted

        if total_inserted > 0 and model not in full_refresh_models:
            Variable.set(watermark_key_name, max_write_date)

        logging.info(
            "Model %s (company %s) inserted %s rows, invalid %s rows, watermark %s",
            model,
            company_id,
            total_inserted,
            invalid_rows,
            max_write_date,
        )
        # Keep XCom payloads tiny: only metadata, no row data.
        return {"model": model, "rows": total_inserted, "watermark": max_write_date}

    @task
    def run_mart_sql(sql: str, sql_params: Dict[str, Any]) -> None:
        client = get_clickhouse_client(CLICKHOUSE_CONN_ID)
        logging.info("Running mart SQL with params %s" % sql_params)
        execute_sql(client, sql, sql_params)

    @task
    def fetch_dead_stock_days() -> int:
        return int(Variable.get(VAR_DEAD_STOCK_DAYS, default_var=90))

    @task
    def fetch_anomaly_pct() -> float:
        return float(Variable.get(VAR_ANOMALY_PCT, default_var=0.2))

    @task
    def fetch_abc_thresholds() -> Dict[str, float]:
        return {
            "abc_a_pct": float(Variable.get(VAR_ABC_A_PCT, default_var=0.80)),
            "abc_b_pct": float(Variable.get(VAR_ABC_B_PCT, default_var=0.15)),
            "abc_c_pct": float(Variable.get(VAR_ABC_C_PCT, default_var=0.05)),
        }

    @task.short_circuit
    def otif_below_threshold() -> bool:
        """Only notify if any vendor falls below the configured OTIF threshold."""
        threshold = float(Variable.get(VAR_OTIF_THRESHOLD, default_var=0.8))
        email = Variable.get(VAR_PROCUREMENT_EMAIL, default_var="")
        if not email:
            logging.info("No procurement email configured; skipping OTIF alert")
            return False
        client = get_clickhouse_client(CLICKHOUSE_CONN_ID)
        rows = client.execute(
            """
            SELECT min(overall_score)
            FROM mart_vendor_rating
            WHERE month = toStartOfMonth(now())
            """
        )
        if not rows or rows[0][0] is None:
            return False
        return rows[0][0] < threshold

    @task
    def build_demand_forecast() -> int:
        """Forecast next month demand for Class A (or configured scope).

        Uses statsmodels if available; falls back to a moving-average baseline otherwise.
        """

        client = get_clickhouse_client(CLICKHOUSE_CONN_ID)
        company_rows = client.execute("SELECT id, name FROM raw_res_company")
        company_names = {row[0]: row[1] for row in company_rows}
        product_rows = client.execute(
            "SELECT company_id, id, default_code FROM raw_product_product"
        )
        product_codes = {(row[0], row[1]): row[2] for row in product_rows}

        scope = Variable.get(VAR_FORECAST_SCOPE, default_var="only_class_a")
        query = """
        SELECT
            m.company_id,
            m.product_id,
            toStartOfMonth(m.date_done) AS month,
            sum(m.quantity_done) AS qty
        FROM raw_stock_move m
        WHERE m.state = 'done'
          AND m.date_done >= now() - INTERVAL 12 MONTH
        GROUP BY company_id, product_id, month
        ORDER BY company_id, product_id, month
        """
        rows = client.execute(query)

        # Optional filter to class A to limit compute.
        class_a_set = set()
        top_n_set = set()
        if scope == "only_class_a":
            class_a_rows = client.execute(
                """
                SELECT company_id, product_id
                FROM mart_abc_classification
                WHERE snapshot_date = toDate(now()) AND abc_class = 'A'
                """
            )
            class_a_set = {(r[0], r[1]) for r in class_a_rows}
        if scope.startswith("top_n:"):
            try:
                n = int(scope.split(":", 1)[1])
            except ValueError:
                n = 0
            if n > 0:
                top_n_rows = client.execute(
                    """
                    SELECT company_id, product_id
                    FROM mart_abc_classification
                    WHERE snapshot_date = toDate(now())
                    ORDER BY total_value_moved DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": n},
                )
                top_n_set = {(r[0], r[1]) for r in top_n_rows}

        series = defaultdict(list)
        for company_id, product_id, month, qty in rows:
            if class_a_set and (company_id, product_id) not in class_a_set:
                continue
            if top_n_set and (company_id, product_id) not in top_n_set:
                continue
            series[(company_id, product_id)].append((month, qty))

        forecast_rows = []
        for (company_id, product_id), values in series.items():
            values = sorted(values, key=lambda x: x[0])
            qtys = [v[1] for v in values]
            if not qtys:
                continue
            forecast_month = (values[-1][0] + timedelta(days=32)).replace(day=1)
            avg = sum(qtys[-3:]) / max(1, len(qtys[-3:]))
            forecast_qty = avg
            try:
                from statsmodels.tsa.holtwinters import ExponentialSmoothing

                if len(qtys) >= 4:
                    model = ExponentialSmoothing(qtys, trend="add", seasonal=None, initialization_method="estimated")
                    fit = model.fit()
                    forecast_qty = float(fit.forecast(1)[0])
            except Exception as exc:
                logging.info("Statsmodels unavailable or failed (%s); using moving average", exc)
            company_name = company_names.get(company_id, "")
            product_default_code = product_codes.get((company_id, product_id), "")
            if not product_default_code:
                product_default_code = product_codes.get((0, product_id), "")
            forecast_rows.append(
                {
                    "forecast_month": forecast_month,
                    "company_id": company_id,
                    "company_name": company_name,
                    "product_id": product_id,
                    "product_default_code": product_default_code,
                    "forecast_qty": float(forecast_qty),
                    "lower_ci": float(forecast_qty * 0.85),
                    "upper_ci": float(forecast_qty * 1.15),
                }
            )

        inserted = insert_rows(client, "mart_demand_forecast", forecast_rows)
        logging.info("Inserted %s demand forecast rows", inserted)
        return inserted

    @task
    def reverse_etl_cost_anomalies() -> int:
        """Create Helpdesk tickets for cost anomalies; dedupe by product/day.

        If your Odoo model differs (helpdesk.ticket or mail.message), adjust here.
        """
        config = get_odoo_config(ODOO_CONN_ID)
        client = OdooClient(config)
        ch_client = get_clickhouse_client(CLICKHOUSE_CONN_ID)
        team_id = Variable.get(VAR_HELPDESK_TEAM_ID, default_var=None)
        if not team_id:
            logging.info("No helpdesk team configured; skipping ticket creation")
            return 0

        rows = ch_client.execute(
            """
            SELECT company_id, product_id, deviation_pct
            FROM mart_cost_anomalies
            WHERE snapshot_date = toDate(now())
            """
        )

        created = 0
        for company_id, product_id, deviation_pct in rows:
            subject = f"COGS anomaly product {product_id}"
            description = (
                f"Check item {product_id}. Cost deviated {deviation_pct:.2%} from 30-day average."
            )
            # Dedupe: check for existing ticket by name and day.
            existing = client.search_read(
                "helpdesk.ticket",
                [["name", "=", subject], ["create_date", ">=", datetime.utcnow().strftime("%Y-%m-%d")]],
                ["id"],
                limit=1,
                offset=0,
                order="id desc",
            )
            if existing:
                continue
            values = {
                "name": subject,
                "description": description,
                "team_id": int(team_id),
            }
            client.create("helpdesk.ticket", values)
            created += 1
            time.sleep(0.2)

        logging.info("Created %s helpdesk tickets", created)
        return created

    @task
    def reverse_etl_abc_classification() -> int:
        """Update Odoo product.template with ABC classification in batches."""
        config = get_odoo_config(ODOO_CONN_ID)
        client = OdooClient(config)
        ch_client = get_clickhouse_client(CLICKHOUSE_CONN_ID)

        rows = ch_client.execute(
            """
            SELECT pp.product_tmpl_id, a.abc_class
            FROM mart_abc_classification a
            LEFT JOIN raw_product_product FINAL pp ON a.product_id = pp.id
            WHERE a.snapshot_date = toDate(now())
            """
        )

        updated = 0
        for product_tmpl_id, abc_class in rows:
            if not product_tmpl_id:
                continue
            client.write("product.template", [int(product_tmpl_id)], {"x_abc_classification": abc_class})
            updated += 1
            time.sleep(0.1)

        logging.info("Updated %s product.template ABC classifications", updated)
        return updated

    with TaskGroup(group_id="extract_to_clickhouse_tg") as extract_to_clickhouse_tg:
        # Extract once from Odoo into raw ClickHouse tables; all marts reuse these tables.
        init_clickhouse_tables_task = init_clickhouse_tables()
        companies = fetch_active_companies()

        extract_stock_move_line = extract_model_to_clickhouse.partial(
            model="stock.move.line",
            fields=[
                "id",
                "company_id",
                "product_id",
                "quantity",
                "location_id",
                "location_dest_id",
                "state",
                "date",
                "write_date",
                "create_date",
            ],
            domain=[["state", "=", "done"]],
            order="write_date asc",
            required_fields=["id", "product_id", "date_done", "write_date"],
        ).expand(company_id=companies)

        extract_stock_quant = extract_model_to_clickhouse.partial(
            model="stock.quant",
            fields=[
                "id",
                "company_id",
                "product_id",
                "location_id",
                "quantity",
                "reserved_quantity",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_id", "write_date"],
        ).expand(company_id=companies)

        extract_stock_move = extract_model_to_clickhouse.partial(
            model="stock.move",
            fields=[
                "id",
                "company_id",
                "product_id",
                "product_uom_qty",
                "quantity",
                "location_id",
                "location_dest_id",
                "state",
                "date_deadline",
                "date",
                "origin",
                "write_date",
                "create_date",
            ],
            domain=[["state", "=", "done"]],
            order="write_date asc",
            required_fields=["id", "product_id", "date_done", "write_date"],
        ).expand(company_id=companies)

        extract_purchase_order = extract_model_to_clickhouse.partial(
            model="purchase.order",
            fields=[
                "id",
                "company_id",
                "partner_id",
                "name",
                "date_order",
                "date_planned",
                "state",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "partner_id", "date_order", "write_date"],
        ).expand(company_id=companies)

        extract_product_product = extract_model_to_clickhouse.partial(
            model="product.product",
            fields=[
                "id",
                "company_id",
                "product_tmpl_id",
                "default_code",
                "active",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_tmpl_id", "write_date"],
        ).expand(company_id=companies)

        extract_product_template = extract_model_to_clickhouse.partial(
            model="product.template",
            fields=[
                "id",
                "company_id",
                "name",
                "standard_price",
                "list_price",
                "type",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        extract_stock_valuation_layer = extract_model_to_clickhouse.partial(
            model="stock.valuation.layer",
            fields=[
                "id",
                "company_id",
                "product_id",
                "quantity",
                "value",
                "stock_move_id",
                "create_date",
                "write_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_id", "create_date", "write_date"],
        ).expand(company_id=companies)

        extract_stock_location = extract_model_to_clickhouse.partial(
            model="stock.location",
            fields=[
                "id",
                "company_id",
                "name",
                "usage",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        extract_res_company = extract_model_to_clickhouse(
            model="res.company",
            fields=[
                "id",
                "name",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        )

        extract_res_partner = extract_model_to_clickhouse.partial(
            model="res.partner",
            fields=[
                "id",
                "company_id",
                "name",
                "supplier_rank",
                "write_date",
                "create_date",
            ],
            domain=[["supplier_rank", ">", 0]],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        init_clickhouse_tables_task >> [
            extract_stock_move_line,
            extract_stock_quant,
            extract_stock_move,
            extract_purchase_order,
            extract_product_product,
            extract_product_template,
            extract_stock_valuation_layer,
            extract_stock_location,
            extract_res_company,
            extract_res_partner,
        ]

    with TaskGroup(group_id="dead_stock_inventory_health_tg") as dead_stock_inventory_health_tg:
        dead_stock_days = fetch_dead_stock_days()
        load_dead_stock = run_mart_sql(MART_SQL_LIQUIDATION, {"dead_stock_days": dead_stock_days})

    with TaskGroup(group_id="otif_vendor_scorecard_tg") as otif_vendor_scorecard_tg:
        load_vendor_scorecard = run_mart_sql(MART_SQL_VENDOR, {})
        # should_notify = otif_below_threshold()
        # notify_procurement = EmailOperator(
        #     task_id="notify_procurement",
        #     email_on_failure=False,
        #     email_on_retry=False,
        #     to=Variable.get(VAR_PROCUREMENT_EMAIL, default_var=""),
        #     subject="OTIF score below threshold",
        #     html_content="OTIF score below threshold for latest month. Please review in Superset.",
        #     conn_id=EMAIL_CONN_ID,
        #     trigger_rule=TriggerRule.ALL_DONE,
        # )
        # load_vendor_scorecard >> should_notify >> notify_procurement

    with TaskGroup(group_id="warehouse_efficiency_tg") as warehouse_efficiency_tg:
        load_touch_ratio = run_mart_sql(MART_SQL_TOUCH_RATIO, {})

    with TaskGroup(group_id="stockout_risk_tg") as stockout_risk_tg:
        load_stockout_risk = run_mart_sql(MART_SQL_STOCKOUT_RISK, {})

    with TaskGroup(group_id="inventory_turnover_tg") as inventory_turnover_tg:
        load_inventory_turnover = run_mart_sql(MART_SQL_INVENTORY_TURNOVER, {})

    with TaskGroup(group_id="margin_cogs_anomaly_tg") as margin_cogs_anomaly_tg:
        anomaly_pct = fetch_anomaly_pct()
        load_anomalies = run_mart_sql(MART_SQL_COST_ANOMALIES, {"anomaly_pct": anomaly_pct})
        # create_tickets = reverse_etl_cost_anomalies()
        # load_anomalies >> create_tickets

    with TaskGroup(group_id="demand_forecast_abc_tg") as demand_forecast_abc_tg:
        abc_thresholds = fetch_abc_thresholds()
        load_abc = run_mart_sql(MART_SQL_ABC, abc_thresholds)
        forecast = build_demand_forecast()
        # update_abc = reverse_etl_abc_classification()
        # load_abc >> [forecast, update_abc]
        load_abc >> [forecast]

    # Superset cache refresh placeholder; wire auth via connection extras or custom headers as needed.
    refresh_superset = EmptyOperator(task_id="refresh_superset_cache", trigger_rule=TriggerRule.ALL_DONE)

    start >> extract_to_clickhouse_tg
    extract_to_clickhouse_tg >> [
        dead_stock_inventory_health_tg,
        otif_vendor_scorecard_tg,
        warehouse_efficiency_tg,
        stockout_risk_tg,
        inventory_turnover_tg,
        margin_cogs_anomaly_tg,
        demand_forecast_abc_tg,
    ]
    [
        dead_stock_inventory_health_tg,
        otif_vendor_scorecard_tg,
        warehouse_efficiency_tg,
        stockout_risk_tg,
        inventory_turnover_tg,
        margin_cogs_anomaly_tg,
        demand_forecast_abc_tg,
    ] >> refresh_superset
    refresh_superset >> end

if __name__ == "__main__":
    # This allows you to run 'python odoo_clickhouse_inventory_analytics.py' to test locally
    dag.test()
