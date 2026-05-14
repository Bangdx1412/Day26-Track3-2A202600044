# Lab: Build a Database MCP Server with FastMCP and SQLite

## Goal

Build a Model Context Protocol (MCP) server using FastMCP that exposes a small database through:

- `search`
- `insert`
- `aggregate`

You must also expose the database schema as an MCP resource, test the server with Inspector or equivalent tooling, and show the server working from at least one MCP client.

## Learning Outcomes

By the end of this lab, students should be able to:

- explain what MCP tools and resources are
- build a FastMCP server in Python
- connect FastMCP to a SQLite database
- safely validate database requests before executing SQL
- expose dynamic schema context through `@mcp.resource(...)`
- test tool schemas, normal calls, and error responses
- connect the server to an MCP client such as Claude Code, Codex, or Gemini CLI

## Required Features

### Part 1: MCP Server

Implement a FastMCP server that exposes exactly these tool categories:

1. `search`
2. `insert`
3. `aggregate`

Your server may use SQLite for the main implementation. If you want to support PostgreSQL too, design the code so the database layer can be swapped later.

### Part 2: Resource

Expose database schema information as MCP resources:

- one resource for the full database schema
- one dynamic resource template for a single table schema

Suggested URIs:

- `schema://database`
- `schema://table/{table_name}`

### Part 3: Validation and Error Handling

Your tools must reject unsafe or invalid requests:

- unknown table names
- unknown column names
- unsupported filter operators
- invalid aggregate requests
- empty inserts

Do not build SQL by blindly concatenating raw user input.

### Part 4: Testing and Verification

Verify all of the following:

1. the server starts correctly
2. the three tools are discoverable
3. the schema resource is discoverable
4. valid tool calls return useful results
5. invalid tool calls return clear errors
6. at least one MCP client can connect and use the server

### Part 5: Demo Deliverables

Prepare:

- GitHub repository
- setup instructions
- tool descriptions
- testing steps
- at least one client configuration example
- short demo video, around 2 minutes

Inspector screenshots are recommended if you use MCP Inspector.

## Suggested Project Structure

```text
implementation/
  db.py
  init_db.py
  mcp_server.py
  verify_server.py
  tests/
    test_server.py
```

## Recommended Data Model

Use a small relational dataset so `search`, `insert`, and `aggregate` are easy to demo. Example:

- `students`
- `courses`
- `enrollments`

## Example Tasks to Demonstrate

- search all students in cohort `A1`
- insert a new student
- count rows in a table
- compute average score by cohort
- read the full schema resource
- read `schema://table/students`
- show an invalid request, such as searching a missing table

## FastMCP and Inspector References

- FastMCP quickstart: https://gofastmcp.com/v2/getting-started/quickstart
- FastMCP resources: https://gofastmcp.com/v2/servers/resources
- MCP Inspector: https://modelcontextprotocol.io/docs/tools/inspector

## Client Setup Notes

### Claude Code

Anthropic documents local JSON config and `claude mcp add` flows here:

- https://code.claude.com/docs/en/mcp

Claude Code supports MCP resources via `@server:resource-uri` references and supports environment variable expansion in `.mcp.json`.

### Codex

OpenAI documents Codex MCP setup here:

- https://developers.openai.com/learn/docs-mcp

Codex supports MCP server configuration through the CLI and `~/.codex/config.toml`.

### Gemini CLI

Gemini CLI has a built-in MCP manager. In the verified local workflow, the simplest path is:

```bash
gemini mcp add sqlite-lab /ABSOLUTE/PATH/TO/python /ABSOLUTE/PATH/TO/implementation/mcp_server.py --description "SQLite lab FastMCP server" --timeout 10000
gemini mcp list
```

Gemini CLI also documents configuration details here:

- https://github.com/google-gemini/gemini-cli/blob/main/docs/reference/configuration.md

Expected outcome:

- the server appears as `Connected`
- Gemini can discover `search`, `insert`, and `aggregate`
- a headless smoke test works with `gemini --allowed-mcp-server-names sqlite-lab --yolo -p "..."`

### Antigravity

Antigravity commonly uses an `mcp_config.json` file with a shape similar to Gemini CLI. Verify the current product behavior in your installed version before grading against exact UI steps.

## Deliverable Checklist

- working FastMCP server
- SQLite database and seed data
- `search`, `insert`, `aggregate` tools
- schema resource and schema resource template
- verification steps
- automated tests or repeatable verification script
- client configuration example
- README with setup and demo steps
- Inspector startup command or helper script
- at least one verified Gemini CLI or Claude/Codex client test

## Bonus

Optional bonus:

- add authentication for SSE or HTTP transport
- support both SQLite and PostgreSQL with the same MCP surface
- add richer output annotations or pagination

## Implementation Notes

This repository includes a working implementation under `implementation/`.

### Project Structure

```text
implementation/
  db.py
  init_db.py
  mcp_server.py
  verify_server.py
  tests/
    test_server.py
```

### Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python implementation\init_db.py
```

`implementation/init_db.py` creates a reproducible SQLite database with:

- `students`
- `courses`
- `enrollments`

The MCP server also auto-creates the database on startup if `implementation/sqlite_lab.db` does not exist.

### Run The MCP Server

```powershell
python implementation\mcp_server.py
```

The default transport is stdio, suitable for MCP clients and Inspector.

### Tools

`search`

- Searches a validated table.
- Supports selected columns, filters, ordering, `limit`, and `offset`.
- Supported filter operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `like`, `in`, `is_null`.

Example payload:

```json
{
  "table": "students",
  "filters": { "cohort": "A1" },
  "columns": ["id", "name", "cohort", "score"],
  "order_by": "score",
  "descending": true,
  "limit": 10
}
```

`insert`

- Inserts one row into a validated table.
- Rejects empty inserts and unknown columns.
- Returns the inserted payload and generated id.

Example payload:

```json
{
  "table": "students",
  "values": {
    "name": "Annie Easley",
    "cohort": "A1",
    "email": "annie@example.edu",
    "score": 89
  }
}
```

`aggregate`

- Supports `count`, `avg`, `sum`, `min`, and `max`.
- Supports optional filters and `group_by`.

Example payload:

```json
{
  "table": "students",
  "metric": "avg",
  "column": "score",
  "group_by": "cohort"
}
```

### Resources

- `schema://database`
- `schema://table/{table_name}`

Example:

```text
schema://table/students
```

### Verification

Run automated tests:

```powershell
python -m pytest implementation\tests -p no:cacheprovider
```

Run the repeatable verification script:

```powershell
python implementation\verify_server.py
```

Verify MCP tool and resource discovery with FastMCP's in-memory client:

```powershell
python implementation\verify_mcp.py
```

The verification script demonstrates:

- full schema payload
- valid `search`
- valid `insert`
- valid `aggregate`
- invalid table error

### Inspector

From the repository root, replace the paths with absolute paths for your machine:

```powershell
npx -y @modelcontextprotocol/inspector python E:\Day26-Track3-MCP-tool-integration\implementation\mcp_server.py
```

Checklist inside Inspector:

- `search`, `insert`, and `aggregate` appear as tools
- `schema://database` appears as a resource
- `schema://table/{table_name}` is readable with `students`
- valid calls return rows
- invalid calls return clear validation errors

### Codex Client Example

Example `~/.codex/config.toml`:

```toml
[mcp_servers.sqlite_lab]
command = "python"
args = ["E:\\Day26-Track3-MCP-tool-integration\\implementation\\mcp_server.py"]
```

### Gemini CLI Example

```powershell
gemini mcp add sqlite-lab python E:\Day26-Track3-MCP-tool-integration\implementation\mcp_server.py --description "SQLite lab FastMCP server" --timeout 10000
gemini mcp list
gemini --allowed-mcp-server-names sqlite-lab --yolo -p "Use the sqlite-lab MCP server and show me the top 2 students by score."
```

### Demo Script

For a short demo, show these actions in order:

1. Start the server in Inspector.
2. Open `schema://database`.
3. Call `search` for students in cohort `A1`.
4. Call `insert` to add a student.
5. Call `aggregate` to average score by cohort.
6. Call `search` with a missing table and show the clear error.
