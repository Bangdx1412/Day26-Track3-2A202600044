from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest


IMPLEMENTATION_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(IMPLEMENTATION_DIR))

from db import SQLiteAdapter, ValidationError
from init_db import create_database


@pytest.fixture()
def adapter() -> SQLiteAdapter:
    runtime_dir = IMPLEMENTATION_DIR / "test_runtime"
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / f"{uuid.uuid4().hex}.db"
    return SQLiteAdapter(create_database(db_path))


def test_search_filters_ordering_and_pagination(adapter: SQLiteAdapter) -> None:
    result = adapter.search(
        table="students",
        filters={"cohort": "A1"},
        columns=["name", "score"],
        order_by="score",
        descending=True,
        limit=1,
    )

    assert result["count"] == 1
    assert result["rows"] == [{"name": "Ada Lovelace", "score": 96.5}]


def test_insert_returns_inserted_payload(adapter: SQLiteAdapter) -> None:
    result = adapter.insert(
        table="students",
        values={
            "name": "Annie Easley",
            "cohort": "A1",
            "email": "annie@example.edu",
            "score": 89.0,
        },
    )

    assert result["inserted_id"] > 0
    assert result["row"]["name"] == "Annie Easley"
    search_result = adapter.search(
        table="students",
        filters={"email": "annie@example.edu"},
        columns=["name", "cohort", "score"],
    )
    assert search_result["rows"] == [{"name": "Annie Easley", "cohort": "A1", "score": 89.0}]


def test_aggregate_avg_by_cohort(adapter: SQLiteAdapter) -> None:
    result = adapter.aggregate(
        table="students",
        metric="avg",
        column="score",
        group_by="cohort",
    )

    by_cohort = {row["cohort"]: row["value"] for row in result["rows"]}
    assert by_cohort["A1"] == pytest.approx(93.75)
    assert by_cohort["B2"] == pytest.approx(93.25)


def test_count_all_rows(adapter: SQLiteAdapter) -> None:
    result = adapter.aggregate(table="students", metric="count")

    assert result["rows"] == [{"value": 5}]


def test_database_and_table_schema(adapter: SQLiteAdapter) -> None:
    database_schema = adapter.get_database_schema()
    table_schema = adapter.get_table_schema("students")

    assert {table["table"] for table in database_schema["tables"]} == {
        "courses",
        "enrollments",
        "students",
    }
    assert "cohort" in {column["name"] for column in table_schema["columns"]}


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda db: db.search(table="missing"), "Unknown table"),
        (lambda db: db.search(table="students", columns=["missing"]), "Unknown column"),
        (
            lambda db: db.search(
                table="students",
                filters=[{"column": "score", "op": "between", "value": [80, 90]}],
            ),
            "Unsupported filter operator",
        ),
        (lambda db: db.insert(table="students", values={}), "non-empty object"),
        (lambda db: db.aggregate(table="students", metric="median", column="score"), "Unsupported"),
        (lambda db: db.aggregate(table="students", metric="avg"), "requires a column"),
    ],
)
def test_invalid_requests_are_rejected(adapter: SQLiteAdapter, call, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        call(adapter)
