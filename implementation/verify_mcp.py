from __future__ import annotations

import asyncio
import json

from fastmcp import Client

from mcp_server import mcp


def object_names(items: list[object]) -> list[str]:
    return sorted(str(getattr(item, "name")) for item in items)


async def main() -> None:
    async with Client(mcp) as client:
        tools = await client.list_tools()
        resources = await client.list_resources()
        templates = await client.list_resource_templates()

        tool_names = object_names(tools)
        assert {"aggregate", "insert", "search"} <= set(tool_names)
        assert any(str(getattr(resource, "uri")) == "schema://database" for resource in resources)
        assert any("schema://table" in str(getattr(template, "uriTemplate")) for template in templates)

        search_result = await client.call_tool(
            "search",
            {
                "table": "students",
                "filters": {"cohort": "A1"},
                "columns": ["id", "name", "cohort", "score"],
                "order_by": "score",
                "descending": True,
            },
        )
        schema_result = await client.read_resource("schema://table/students")

    print("MCP verification completed.")
    print(json.dumps({"tools": tool_names}, indent=2))
    print(f"search result content items: {len(getattr(search_result, 'content', []))}")
    print(f"schema result content items: {len(schema_result)}")


if __name__ == "__main__":
    asyncio.run(main())
