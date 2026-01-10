"""Shared constants for the Odoo ClickHouse analytics pipeline."""

from datetime import timedelta

DAG_ID = "odoo_clickhouse_inventory_analytics"

# Connection IDs: These strings must match the 'Conn Id' configured in
# Airflow Admin -> Connections. Do not hardcode credentials here.
ODOO_CONN_ID = "odoo_default"
CLICKHOUSE_CONN_ID = "clickhouse_default"
SUPERSET_CONN_ID = "superset_default"
EMAIL_CONN_ID = "email_default"

# Airflow Variable Keys: Configure the values for these keys in Airflow Admin -> Variables.
# The strings below are the keys, not the values.
VAR_DEAD_STOCK_DAYS = "dead_stock_days"
VAR_ANOMALY_PCT = "anomaly_pct"
VAR_OTIF_THRESHOLD = "otif_threshold"
VAR_ABC_A_PCT = "abc_a_pct"
VAR_ABC_B_PCT = "abc_b_pct"
VAR_ABC_C_PCT = "abc_c_pct"
VAR_FULL_REFRESH_MODELS = "full_refresh_models"
VAR_FORECAST_SCOPE = "forecast_top_n_or_only_class_a"
VAR_PROCUREMENT_EMAIL = "procurement_manager_email"
VAR_HELPDESK_TEAM_ID = "odoo_helpdesk_team_id"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "depends_on_past": False,
}

BATCH_SIZE = 5000
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2

RAW_TABLES = {
    "stock.move.line": "raw_stock_move_line",
    "stock.quant": "raw_stock_quant",
    "stock.move": "raw_stock_move",
    "purchase.order": "raw_purchase_order",
    "product.product": "raw_product_product",
    "product.template": "raw_product_template",
    "stock.valuation.layer": "raw_stock_valuation_layer",
    "stock.location": "raw_stock_location",
    "res.company": "raw_res_company",
    "res.partner": "raw_res_partner",
}

STRING_FIELDS = {
    "state",
    "origin",
    "name",
    "usage",
    "default_code",
    "type",
}

MODEL_FIELD_ALIASES = {
    # Odoo 17 uses `quantity` and `date` on stock.move.line.
    "stock.move.line": {
        "quantity": "qty_done",
        "date": "date_done",
    },
    # Odoo 17 uses `quantity`, `date`, and `date_deadline` on stock.move.
    "stock.move": {
        "quantity": "quantity_done",
        "date": "date_done",
        "date_deadline": "date_expected",
    },
}
