"""ClickHouse helpers for the analytics pipeline."""

from typing import Any, Dict, Optional, Sequence

from airflow.hooks.base import BaseHook
from clickhouse_driver import Client as ClickHouseClient


def get_clickhouse_client(conn_id: str) -> ClickHouseClient:
    conn = BaseHook.get_connection(conn_id)
    return ClickHouseClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "default",
    )


def execute_sql(client: ClickHouseClient, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    for stmt in statements:
        if params:
            client.execute(stmt, params)
        else:
            client.execute(stmt)


def insert_rows(
    client: ClickHouseClient,
    table: str,
    rows: Sequence[Dict[str, Any]],
    batch_size: int = 10000,
) -> int:
    if not rows:
        return 0
    columns = sorted(rows[0].keys())
    total = 0
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        values = [[row.get(col) for col in columns] for row in chunk]
        client.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
            values,
        )
        total += len(chunk)
    return total
