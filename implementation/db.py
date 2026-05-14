from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class ValidationError(ValueError):
    """Raised when a request cannot be safely executed."""


SUPPORTED_OPERATORS = {"=", "!=", ">", ">=", "<", "<=", "like", "in", "is_null"}
SUPPORTED_AGGREGATES = {"count", "avg", "sum", "min", "max"}
DEFAULT_LIMIT = 20
MAX_LIMIT = 100


class SQLiteAdapter:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def list_tables(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
        return [row["name"] for row in rows]

    def get_table_schema(self, table: str) -> dict[str, Any]:
        self._validate_table(table)
        with self.connect() as conn:
            columns = conn.execute(f"PRAGMA table_info({self._quote(table)})").fetchall()
            foreign_keys = conn.execute(f"PRAGMA foreign_key_list({self._quote(table)})").fetchall()

        return {
            "table": table,
            "columns": [
                {
                    "name": column["name"],
                    "type": column["type"],
                    "not_null": bool(column["notnull"]),
                    "default": column["dflt_value"],
                    "primary_key": bool(column["pk"]),
                }
                for column in columns
            ],
            "foreign_keys": [
                {
                    "column": key["from"],
                    "references_table": key["table"],
                    "references_column": key["to"],
                    "on_update": key["on_update"],
                    "on_delete": key["on_delete"],
                }
                for key in foreign_keys
            ],
        }

    def get_database_schema(self) -> dict[str, Any]:
        return {"tables": [self.get_table_schema(table) for table in self.list_tables()]}

    def schema_json(self, table: str | None = None) -> str:
        payload = self.get_table_schema(table) if table else self.get_database_schema()
        return json.dumps(payload, indent=2)

    def search(
        self,
        table: str,
        columns: list[str] | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
        order_by: str | None = None,
        descending: bool = False,
    ) -> dict[str, Any]:
        table_columns = self._columns_for(table)
        selected_columns = self._validate_selected_columns(columns, table_columns)
        limit = self._validate_limit(limit)
        offset = self._validate_offset(offset)

        where_sql, params = self._build_where_clause(table_columns, filters)
        order_sql = ""
        if order_by is not None:
            self._validate_column(order_by, table_columns)
            direction = "DESC" if descending else "ASC"
            order_sql = f" ORDER BY {self._quote(order_by)} {direction}"

        column_sql = ", ".join(self._quote(column) for column in selected_columns)
        sql = (
            f"SELECT {column_sql} FROM {self._quote(table)}"
            f"{where_sql}{order_sql} LIMIT ? OFFSET ?"
        )

        with self.connect() as conn:
            rows = conn.execute(sql, [*params, limit, offset]).fetchall()

        return {
            "table": table,
            "columns": selected_columns,
            "limit": limit,
            "offset": offset,
            "count": len(rows),
            "rows": [dict(row) for row in rows],
        }

    def insert(self, table: str, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values, dict) or not values:
            raise ValidationError("Insert values must be a non-empty object.")

        table_columns = self._columns_for(table)
        for column in values:
            self._validate_column(column, table_columns)

        column_sql = ", ".join(self._quote(column) for column in values)
        placeholder_sql = ", ".join("?" for _ in values)
        sql = f"INSERT INTO {self._quote(table)} ({column_sql}) VALUES ({placeholder_sql})"

        with self.connect() as conn:
            cursor = conn.execute(sql, list(values.values()))
            inserted_id = cursor.lastrowid
            conn.commit()

        inserted = {"id": inserted_id, **values}
        return {"table": table, "inserted_id": inserted_id, "row": inserted}

    def aggregate(
        self,
        table: str,
        metric: str,
        column: str | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        group_by: str | list[str] | None = None,
    ) -> dict[str, Any]:
        table_columns = self._columns_for(table)
        metric = self._validate_metric(metric)
        group_columns = self._validate_group_by(group_by, table_columns)

        if metric == "count" and column is None:
            aggregate_target = "*"
        else:
            if column is None:
                raise ValidationError(f"Aggregate metric '{metric}' requires a column.")
            self._validate_column(column, table_columns)
            aggregate_target = self._quote(column)

        where_sql, params = self._build_where_clause(table_columns, filters)
        group_select = ", ".join(self._quote(column) for column in group_columns)
        select_parts = [group_select] if group_select else []
        select_parts.append(f"{metric.upper()}({aggregate_target}) AS value")
        group_sql = ""
        if group_columns:
            group_sql = " GROUP BY " + ", ".join(self._quote(column) for column in group_columns)

        sql = (
            f"SELECT {', '.join(select_parts)} FROM {self._quote(table)}"
            f"{where_sql}{group_sql}"
        )

        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return {
            "table": table,
            "metric": metric,
            "column": column,
            "group_by": group_columns,
            "rows": [dict(row) for row in rows],
        }

    def _columns_for(self, table: str) -> set[str]:
        schema = self.get_table_schema(table)
        return {column["name"] for column in schema["columns"]}

    def _validate_table(self, table: str) -> None:
        if not isinstance(table, str) or not table:
            raise ValidationError("Table name must be a non-empty string.")
        if table not in self.list_tables():
            raise ValidationError(f"Unknown table '{table}'.")

    def _validate_column(self, column: str, table_columns: set[str]) -> None:
        if not isinstance(column, str) or not column:
            raise ValidationError("Column name must be a non-empty string.")
        if column not in table_columns:
            raise ValidationError(f"Unknown column '{column}'.")

    def _validate_selected_columns(
        self, columns: list[str] | None, table_columns: set[str]
    ) -> list[str]:
        if columns is None:
            return sorted(table_columns)
        if not isinstance(columns, list) or not columns:
            raise ValidationError("Columns must be a non-empty list when provided.")
        for column in columns:
            self._validate_column(column, table_columns)
        return columns

    def _validate_group_by(
        self, group_by: str | list[str] | None, table_columns: set[str]
    ) -> list[str]:
        if group_by is None:
            return []
        group_columns = [group_by] if isinstance(group_by, str) else group_by
        if not isinstance(group_columns, list) or not group_columns:
            raise ValidationError("group_by must be a column name or a non-empty list.")
        for column in group_columns:
            self._validate_column(column, table_columns)
        return group_columns

    def _validate_metric(self, metric: str) -> str:
        if not isinstance(metric, str):
            raise ValidationError("Aggregate metric must be a string.")
        normalized = metric.lower()
        if normalized not in SUPPORTED_AGGREGATES:
            raise ValidationError(
                f"Unsupported aggregate metric '{metric}'. "
                f"Supported metrics: {', '.join(sorted(SUPPORTED_AGGREGATES))}."
            )
        return normalized

    def _validate_limit(self, limit: int) -> int:
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise ValidationError("Limit must be an integer.")
        if limit < 1 or limit > MAX_LIMIT:
            raise ValidationError(f"Limit must be between 1 and {MAX_LIMIT}.")
        return limit

    def _validate_offset(self, offset: int) -> int:
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValidationError("Offset must be a non-negative integer.")
        return offset

    def _build_where_clause(
        self,
        table_columns: set[str],
        filters: dict[str, Any] | list[dict[str, Any]] | None,
    ) -> tuple[str, list[Any]]:
        normalized_filters = self._normalize_filters(filters)
        if not normalized_filters:
            return "", []

        clauses: list[str] = []
        params: list[Any] = []
        for item in normalized_filters:
            column = item["column"]
            operator = item["op"].lower() if isinstance(item["op"], str) else item["op"]
            value = item.get("value")
            self._validate_column(column, table_columns)

            if operator not in SUPPORTED_OPERATORS:
                raise ValidationError(f"Unsupported filter operator '{operator}'.")

            quoted_column = self._quote(column)
            if operator == "is_null":
                clauses.append(f"{quoted_column} IS {'NOT ' if value is False else ''}NULL")
            elif operator == "in":
                if not isinstance(value, list) or not value:
                    raise ValidationError("The 'in' operator requires a non-empty list value.")
                placeholders = ", ".join("?" for _ in value)
                clauses.append(f"{quoted_column} IN ({placeholders})")
                params.extend(value)
            else:
                clauses.append(f"{quoted_column} {operator.upper()} ?")
                params.append(value)

        return " WHERE " + " AND ".join(clauses), params

    def _normalize_filters(
        self, filters: dict[str, Any] | list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        if filters is None:
            return []

        if isinstance(filters, list):
            normalized = []
            for item in filters:
                if not isinstance(item, dict):
                    raise ValidationError("Each filter must be an object.")
                column = item.get("column")
                operator = item.get("op", "=")
                normalized.append({"column": column, "op": operator, "value": item.get("value")})
            return normalized

        if isinstance(filters, dict):
            normalized = []
            for column, spec in filters.items():
                if isinstance(spec, dict):
                    normalized.append(
                        {"column": column, "op": spec.get("op", "="), "value": spec.get("value")}
                    )
                else:
                    normalized.append({"column": column, "op": "=", "value": spec})
            return normalized

        raise ValidationError("Filters must be an object, a list of objects, or null.")

    def _quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'
