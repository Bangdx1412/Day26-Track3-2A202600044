from __future__ import annotations

import json
from pathlib import Path

from db import SQLiteAdapter, ValidationError
from init_db import create_database


def show(label: str, payload: object) -> None:
    print(f"\n== {label} ==")
    print(json.dumps(payload, indent=2, sort_keys=True))


def main() -> None:
    db_path = create_database(Path(__file__).with_name("verify_runtime.db"))
    adapter = SQLiteAdapter(db_path)

    show("schema resource payload", adapter.get_database_schema())
    show(
        "search students in cohort A1",
        adapter.search(
            table="students",
            filters={"cohort": "A1"},
            columns=["id", "name", "cohort", "score"],
            order_by="score",
            descending=True,
        ),
    )
    show(
        "insert student",
        adapter.insert(
            table="students",
            values={
                "name": "Annie Easley",
                "cohort": "A1",
                "email": "annie@example.edu",
                "score": 89.0,
            },
        ),
    )
    show(
        "average score by cohort",
        adapter.aggregate(
            table="students",
            metric="avg",
            column="score",
            group_by="cohort",
        ),
    )

    try:
        adapter.search(table="missing_table")
    except ValidationError as exc:
        show("expected invalid request error", {"error": str(exc)})

    print("\nAdapter verification completed.")
    print("For MCP tool/resource discovery, run the Inspector command from README.md.")


if __name__ == "__main__":
    main()
