"""SQL definitions for raw tables and marts."""

RAW_TABLE_DDL = """
-- Raw tables use ReplacingMergeTree on write_date to dedupe incremental loads.
CREATE TABLE IF NOT EXISTS raw_stock_move_line (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    qty_done Float64,
    location_id UInt64,
    location_dest_id UInt64,
    state String,
    date_done DateTime,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_done)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_quant (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    location_id UInt64,
    quantity Float64,
    reserved_quantity Float64,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_move (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    product_uom_qty Float64,
    quantity_done Float64,
    location_id UInt64,
    location_dest_id UInt64,
    state String,
    date_expected DateTime,
    date_done DateTime,
    origin String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_done)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_purchase_order (
    id UInt64,
    company_id UInt64,
    partner_id UInt64,
    name String,
    date_order DateTime,
    date_planned DateTime,
    state String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_order)
ORDER BY (company_id, partner_id, id);

CREATE TABLE IF NOT EXISTS raw_product_product (
    id UInt64,
    company_id UInt64,
    product_tmpl_id UInt64,
    default_code String,
    active UInt8,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, product_tmpl_id, id);

CREATE TABLE IF NOT EXISTS raw_product_template (
    id UInt64,
    company_id UInt64,
    name String,
    standard_price Float64,
    list_price Float64,
    type String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_valuation_layer (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    quantity Float64,
    value Float64,
    stock_move_id UInt64,
    create_date DateTime,
    write_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(create_date)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_location (
    id UInt64,
    company_id UInt64,
    name String,
    usage String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);

CREATE TABLE IF NOT EXISTS raw_res_company (
    id UInt64,
    name String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw_res_partner (
    id UInt64,
    company_id UInt64,
    name String,
    supplier_rank UInt64,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);
"""

MART_TABLE_DDL = """
-- 1. Liquidation Candidates: Identifies 'dead stock'—products with no movement for a configurable period (e.g., 90 days).
-- Insight: Helps finance and ops teams decide what to liquidate to free up cash flow and warehouse space.
CREATE TABLE IF NOT EXISTS mart_liquidation_candidates (
    snapshot_date Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    last_movement_date Date,
    days_since_last_move UInt32,
    on_hand_qty Float64,
    standard_price Float64,
    value_at_risk Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 2. Vendor Scorecard (OTIF): Tracks On-Time and In-Full performance for vendors.
-- Insight: Used for vendor negotiations and identifying supply chain risks.
CREATE TABLE IF NOT EXISTS mart_vendor_rating (
    month Date,
    company_id UInt64,
    company_name String,
    partner_id UInt64,
    on_time_pct Float64,
    in_full_pct Float64,
    overall_score Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, partner_id, month);

-- 3. Warehouse Efficiency (Touch Ratio): Measures how many times an item is moved internally vs. shipped out.
-- Insight: High ratios indicate inefficient warehouse layout or excessive handling processes.
CREATE TABLE IF NOT EXISTS mart_warehouse_touch_ratio (
    month Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    internal_moves UInt64,
    outgoing_moves UInt64,
    touch_ratio Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 3b. Stockout Risk: Tracks unfulfilled demand by product.
CREATE TABLE IF NOT EXISTS mart_stockout_risk (
    month Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    demand_qty Float64,
    fulfilled_qty Float64,
    unmet_qty Float64,
    move_count UInt64,
    stockout_moves UInt64,
    stockout_rate Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 3c. Inventory Turnover: Outbound value moved relative to on-hand value.
CREATE TABLE IF NOT EXISTS mart_inventory_turnover (
    month Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    moved_qty Float64,
    moved_value Float64,
    on_hand_qty Float64,
    on_hand_value Float64,
    turnover_ratio Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 4. Cost Anomalies: Detects sudden spikes or drops in unit cost compared to a 30-day average.
-- Insight: Flags potential data entry errors or supplier pricing issues for immediate review.
CREATE TABLE IF NOT EXISTS mart_cost_anomalies (
    snapshot_date Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    unit_cost Float64,
    avg_30d_cost Float64,
    deviation_pct Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 5. ABC Classification: Segments inventory by value usage (Pareto Principle: 80% value from 20% items).
-- Insight: Prioritizes cycle counting and forecasting efforts on Class A items.
CREATE TABLE IF NOT EXISTS mart_abc_classification (
    snapshot_date Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    total_value_moved Float64,
    cumulative_share Float64,
    abc_class String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 6. Demand Forecast: Predicts future quantity requirements based on historical trends (Exponential Smoothing).
-- Insight: Supports purchasing decisions to prevent stockouts on high-value items.
CREATE TABLE IF NOT EXISTS mart_demand_forecast (
    forecast_month Date,
    company_id UInt64,
    company_name String,
    product_id UInt64,
    product_default_code String,
    forecast_qty Float64,
    lower_ci Float64,
    upper_ci Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(forecast_month)
ORDER BY (company_id, product_id, forecast_month);

"""

MART_SQL_LIQUIDATION = """
INSERT INTO mart_liquidation_candidates (
    snapshot_date,
    company_id,
    company_name,
    product_id,
    product_default_code,
    last_movement_date,
    days_since_last_move,
    on_hand_qty,
    standard_price,
    value_at_risk
)
SELECT
    toDate(now()) AS snapshot_date,
    lm.company_id,
    coalesce(rc.name, '') AS company_name,
    lm.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    lm.last_movement_date,
    dateDiff('day', lm.last_movement_date, toDate(now())) AS days_since_last_move,
    q.on_hand_qty,
    coalesce(pt.standard_price, pt_any.standard_price, 0.0) AS standard_price,
    q.on_hand_qty * coalesce(pt.standard_price, pt_any.standard_price, 0.0) AS value_at_risk
FROM (
    SELECT company_id, product_id, max(toDate(date_done)) AS last_movement_date
    FROM raw_stock_move_line
    WHERE state = 'done'
      AND company_id NOT IN %(excluded_company_ids)s
    GROUP BY company_id, product_id
) lm
LEFT JOIN (
    SELECT rq.company_id, rq.product_id, sum(rq.quantity) AS on_hand_qty
    FROM raw_stock_quant rq
    LEFT JOIN raw_stock_location l
        ON rq.location_id = l.id
        AND (l.company_id = rq.company_id OR l.company_id = 0)
    WHERE l.usage = 'internal'
      AND rq.company_id NOT IN %(excluded_company_ids)s
    GROUP BY rq.company_id, rq.product_id
) q ON lm.company_id = q.company_id AND lm.product_id = q.product_id
LEFT JOIN raw_product_product pp
    ON lm.product_id = pp.id AND (pp.company_id = lm.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON lm.product_id = pp_any.id
LEFT JOIN raw_product_template pt
    ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt.id
    AND (pt.company_id = lm.company_id OR pt.company_id = 0)
LEFT JOIN raw_product_template pt_any
    ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt_any.id
LEFT JOIN raw_res_company rc ON lm.company_id = rc.id
HAVING days_since_last_move > %(dead_stock_days)s;
"""

MART_SQL_VENDOR = """
-- On-time/in-full is limited to incoming receipts based on supplier -> internal locations.
INSERT INTO mart_vendor_rating (
    month,
    company_id,
    company_name,
    partner_id,
    on_time_pct,
    in_full_pct,
    overall_score
)
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    coalesce(rc.name, '') AS company_name,
    po.partner_id,
    avg(if(m.date_done <= po.date_planned, 1.0, 0.0)) AS on_time_pct,
    avg(if(m.quantity_done >= m.product_uom_qty, 1.0, 0.0)) AS in_full_pct,
    (avg(if(m.date_done <= po.date_planned, 1.0, 0.0)) * 0.5)
      + (avg(if(m.quantity_done >= m.product_uom_qty, 1.0, 0.0)) * 0.5) AS overall_score
FROM raw_stock_move m
LEFT JOIN raw_purchase_order po ON m.origin = po.name AND m.company_id = po.company_id
LEFT JOIN raw_stock_location src ON m.location_id = src.id AND m.company_id = src.company_id
LEFT JOIN raw_stock_location dst ON m.location_dest_id = dst.id AND m.company_id = dst.company_id
LEFT JOIN raw_res_company rc ON m.company_id = rc.id
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'supplier'
  AND dst.usage = 'internal'
  AND m.company_id NOT IN %(excluded_company_ids)s
GROUP BY month, m.company_id, company_name, po.partner_id;
"""

MART_SQL_TOUCH_RATIO = """
INSERT INTO mart_warehouse_touch_ratio (
    month,
    company_id,
    company_name,
    product_id,
    product_default_code,
    internal_moves,
    outgoing_moves,
    touch_ratio
)
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    coalesce(rc.name, '') AS company_name,
    m.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    countIf(src.usage = 'internal' AND dst.usage = 'internal') AS internal_moves,
    countIf(dst.usage = 'customer') AS outgoing_moves,
    if(outgoing_moves = 0, 0.0, internal_moves / outgoing_moves) AS touch_ratio
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
LEFT JOIN raw_product_product pp
    ON m.product_id = pp.id AND (pp.company_id = m.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON m.product_id = pp_any.id
LEFT JOIN raw_res_company rc ON m.company_id = rc.id
WHERE m.state = 'done'
  AND m.company_id NOT IN %(excluded_company_ids)s
GROUP BY month, m.company_id, company_name, m.product_id, product_default_code;
"""

MART_SQL_STOCKOUT_RISK = """
INSERT INTO mart_stockout_risk (
    month,
    company_id,
    company_name,
    product_id,
    product_default_code,
    demand_qty,
    fulfilled_qty,
    unmet_qty,
    move_count,
    stockout_moves,
    stockout_rate
)
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    coalesce(rc.name, '') AS company_name,
    m.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    sum(m.product_uom_qty) AS demand_qty,
    sum(m.quantity_done) AS fulfilled_qty,
    sumIf(m.product_uom_qty - m.quantity_done, m.quantity_done < m.product_uom_qty) AS unmet_qty,
    count() AS move_count,
    countIf(m.quantity_done < m.product_uom_qty) AS stockout_moves,
    if(move_count = 0, 0.0, stockout_moves / move_count) AS stockout_rate
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
LEFT JOIN raw_product_product pp
    ON m.product_id = pp.id AND (pp.company_id = m.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON m.product_id = pp_any.id
LEFT JOIN raw_res_company rc ON m.company_id = rc.id
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'internal'
  AND dst.usage = 'customer'
  AND m.company_id NOT IN %(excluded_company_ids)s
GROUP BY month, m.company_id, company_name, m.product_id, product_default_code;
"""

MART_SQL_INVENTORY_TURNOVER = """
INSERT INTO mart_inventory_turnover (
    month,
    company_id,
    company_name,
    product_id,
    product_default_code,
    moved_qty,
    moved_value,
    on_hand_qty,
    on_hand_value,
    turnover_ratio
)
WITH on_hand AS (
    SELECT
        company_id,
        product_id,
        sum(quantity) AS on_hand_qty
    FROM raw_stock_quant rq
    LEFT JOIN raw_stock_location l
        ON rq.location_id = l.id
        AND (l.company_id = rq.company_id OR l.company_id = 0)
    WHERE l.usage = 'internal'
      AND rq.company_id NOT IN %(excluded_company_ids)s
    GROUP BY rq.company_id, rq.product_id
)
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    coalesce(rc.name, '') AS company_name,
    m.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    sum(m.quantity_done) AS moved_qty,
    sum(m.quantity_done * coalesce(pt.standard_price, pt_any.standard_price, 0.0)) AS moved_value,
    coalesce(on_hand.on_hand_qty, 0) AS on_hand_qty,
    coalesce(on_hand.on_hand_qty, 0) * coalesce(pt.standard_price, pt_any.standard_price, 0.0) AS on_hand_value,
    if(on_hand_value = 0, 0.0, moved_value / on_hand_value) AS turnover_ratio
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
LEFT JOIN raw_product_product pp
    ON m.product_id = pp.id AND (pp.company_id = m.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON m.product_id = pp_any.id
LEFT JOIN raw_product_template pt
    ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt.id
    AND (pt.company_id = m.company_id OR pt.company_id = 0)
LEFT JOIN raw_product_template pt_any
    ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt_any.id
LEFT JOIN on_hand
    ON on_hand.company_id = m.company_id AND on_hand.product_id = m.product_id
LEFT JOIN raw_res_company rc ON m.company_id = rc.id
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'internal'
  AND dst.usage = 'customer'
  AND m.company_id NOT IN %(excluded_company_ids)s
GROUP BY month, m.company_id, company_name, m.product_id, product_default_code, on_hand_qty, pt.standard_price, pt_any.standard_price;
"""

MART_SQL_COST_ANOMALIES = """
INSERT INTO mart_cost_anomalies (
    snapshot_date,
    company_id,
    company_name,
    product_id,
    product_default_code,
    unit_cost,
    avg_30d_cost,
    deviation_pct
)
SELECT
    toDate(now()) AS snapshot_date,
    t.company_id,
    coalesce(rc.name, '') AS company_name,
    t.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    t.today_cost AS unit_cost,
    t.avg_30d_cost,
    if(t.avg_30d_cost = 0, 0.0, (t.today_cost - t.avg_30d_cost) / t.avg_30d_cost) AS deviation_pct
FROM (
    SELECT
        company_id,
        product_id,
        avgIf(value / quantity, quantity > 0 AND toDate(create_date) = toDate(now())) AS today_cost,
        avgIf(value / quantity, quantity > 0 AND create_date >= now() - INTERVAL 30 DAY) AS avg_30d_cost
    FROM raw_stock_valuation_layer
    WHERE company_id NOT IN %(excluded_company_ids)s
    GROUP BY company_id, product_id
) t
LEFT JOIN raw_product_product pp
    ON t.product_id = pp.id AND (pp.company_id = t.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON t.product_id = pp_any.id
LEFT JOIN raw_res_company rc ON t.company_id = rc.id
WHERE abs(deviation_pct) > %(anomaly_pct)s;
"""

MART_SQL_ABC = """
INSERT INTO mart_abc_classification (
    snapshot_date,
    company_id,
    company_name,
    product_id,
    product_default_code,
    total_value_moved,
    cumulative_share,
    abc_class
)
SELECT
    -- Logic: Rank products by total movement value (Cost * Qty) over the last 12 months.
    -- This implements the Pareto Principle (80/20 rule) based on inventory throughput, not sales revenue.
    toDate(now()) AS snapshot_date,
    t.company_id,
    coalesce(rc.name, '') AS company_name,
    t.product_id,
    coalesce(pp.default_code, pp_any.default_code, '') AS product_default_code,
    total_value_moved,
    cumulative_share,
    multiIf(
        cumulative_share <= %(abc_a_pct)s, 'A',
        cumulative_share <= (%(abc_a_pct)s + %(abc_b_pct)s), 'B',
        'C'
    ) AS abc_class
FROM (
    SELECT
        company_id,
        product_id,
        total_value_moved,
        sum(total_value_moved) OVER (
            PARTITION BY company_id
            ORDER BY total_value_moved DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / sum(total_value_moved) OVER (PARTITION BY company_id) AS cumulative_share
    FROM (
        SELECT
            m.company_id AS company_id,
            m.product_id AS product_id,
            sum(m.quantity_done * coalesce(pt.standard_price, pt_any.standard_price, 0.0)) AS total_value_moved
        FROM raw_stock_move m
        LEFT JOIN raw_product_product pp
            ON m.product_id = pp.id AND (pp.company_id = m.company_id OR pp.company_id = 0)
        LEFT JOIN raw_product_product pp_any
            ON m.product_id = pp_any.id
        LEFT JOIN raw_product_template pt
            ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt.id
            AND (pt.company_id = m.company_id OR pt.company_id = 0)
        LEFT JOIN raw_product_template pt_any
            ON coalesce(pp.product_tmpl_id, pp_any.product_tmpl_id) = pt_any.id
        WHERE m.state = 'done'
          AND m.date_done >= now() - INTERVAL 12 MONTH
          AND m.company_id NOT IN %(excluded_company_ids)s
        GROUP BY m.company_id, m.product_id
    )
) t
LEFT JOIN raw_product_product pp
    ON t.product_id = pp.id AND (pp.company_id = t.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_product pp_any
    ON t.product_id = pp_any.id
LEFT JOIN raw_res_company rc ON t.company_id = rc.id;
"""
