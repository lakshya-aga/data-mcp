# Agentic Tool Addition Workflow (findata MCP)

## Flow
1. User calls MCP `request_tool_addition`.
2. MCP writes request spec in `.tool_builder/requests/<id>.json`.
3. Optional builder command (`FINDATA_TOOL_BUILDER_CMD`) is spawned.
4. Builder script (`scripts/agent_pipeline.py`) runs viability triage.
5. If viable, code is added and committed to `agent` branch.
6. Human reviews PR and merges.
7. Pull merged branch + restart MCP service.

## Configure auto-builder

```bash
export FINDATA_TOOL_BUILDER_CMD='python scripts/agent_pipeline.py --request {request_file} --push'
```

## Post-merge update

```bash
git checkout main && git pull
sudo systemctl restart data-mcp
```
