from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from db import SQLiteAdapter
from init_db import DEFAULT_DB_PATH, create_database


mcp = FastMCP("SQLite Lab MCP Server")


def create_adapter() -> SQLiteAdapter:
    db_path = Path(os.environ.get("SQLITE_LAB_DB", DEFAULT_DB_PATH))
    if not db_path.exists():
        create_database(db_path)
    return SQLiteAdapter(db_path)


adapter = create_adapter()


@mcp.tool(name="search")
def search(
    table: str,
    filters: dict[str, Any] | list[dict[str, Any]] | None = None,
    columns: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
    order_by: str | None = None,
    descending: bool = False,
) -> dict[str, Any]:
    """
    Search rows in a validated table with optional filters, ordering, and pagination.

    Filters may be {"cohort": "A1"} or [{"column": "score", "op": ">=", "value": 90}].
    Supported operators: =, !=, >, >=, <, <=, like, in, is_null.
    """
    return adapter.search(
        table=table,
        filters=filters,
        columns=columns,
        limit=limit,
        offset=offset,
        order_by=order_by,
        descending=descending,
    )


@mcp.tool(name="insert")
def insert(table: str, values: dict[str, Any]) -> dict[str, Any]:
    """Insert one row into a validated table and return the inserted payload."""
    return adapter.insert(table=table, values=values)


@mcp.tool(name="aggregate")
def aggregate(
    table: str,
    metric: str,
    column: str | None = None,
    filters: dict[str, Any] | list[dict[str, Any]] | None = None,
    group_by: str | list[str] | None = None,
) -> dict[str, Any]:
    """
    Run count, avg, sum, min, or max against a validated table.

    Use group_by to compute grouped values, for example avg score grouped by cohort.
    """
    return adapter.aggregate(
        table=table,
        metric=metric,
        column=column,
        filters=filters,
        group_by=group_by,
    )


@mcp.resource("schema://database")
def database_schema() -> str:
    """Return the full SQLite schema as formatted JSON text."""
    return adapter.schema_json()


@mcp.resource("schema://table/{table_name}")
def table_schema(table_name: str) -> str:
    """Return one table schema as formatted JSON text."""
    return adapter.schema_json(table_name)


if __name__ == "__main__":
    mcp.run()
