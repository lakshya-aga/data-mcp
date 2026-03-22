#!/usr/bin/env python3
"""Agentic tool-build pipeline helper.

Flow:
1) Read tool request spec JSON (from request_tool_addition)
2) Triage viability
3) If viable, materialize code/docs placeholder update
4) Commit to `agent` branch and push

This script is intentionally conservative: human review happens via PR.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=REPO)


def triage(spec: dict) -> tuple[bool, str]:
    required = ["tool_name", "module_path", "summary", "code"]
    missing = [k for k in required if not str(spec.get(k, "")).strip()]
    if missing:
        return False, f"missing required fields: {', '.join(missing)}"

    server_text = (REPO / "findata_mcp/server.py").read_text(encoding="utf-8")
    if f'"name": "{spec["tool_name"]}"' in server_text:
        return False, f"tool_name already exists in registry: {spec['tool_name']}"

    if "import os" in spec["code"] and "subprocess" in spec["code"]:
        return False, "unsafe pattern detected (os+subprocess)."

    return True, "viable"


def apply_changes(spec: dict, request_id: str) -> None:
    target = REPO / spec["module_path"]
    target.parent.mkdir(parents=True, exist_ok=True)

    if not target.exists():
        target.write_text("", encoding="utf-8")

    with target.open("a", encoding="utf-8") as f:
        f.write("\n\n# ---- agentic tool request {rid} ----\n".format(rid=request_id))
        f.write(spec["code"].rstrip() + "\n")

    req_log = REPO / "AGENT_REQUESTS.md"
    with req_log.open("a", encoding="utf-8") as f:
        f.write(
            f"\n## {request_id}\n"
            f"- tool_name: `{spec['tool_name']}`\n"
            f"- module_path: `{spec['module_path']}`\n"
            f"- summary: {spec['summary']}\n"
            f"- status: implemented on agent branch (pending PR review)\n"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--request", required=True, help="Path to JSON request spec")
    ap.add_argument("--push", action="store_true", help="Push agent branch")
    args = ap.parse_args()

    req_path = Path(args.request).resolve()
    spec = json.loads(req_path.read_text(encoding="utf-8"))
    request_id = spec.get("request_id", req_path.stem)

    ok, reason = triage(spec)
    report = REPO / ".tool_builder" / "reports"
    report.mkdir(parents=True, exist_ok=True)
    (report / f"{request_id}.json").write_text(json.dumps({"viable": ok, "reason": reason}, indent=2), encoding="utf-8")

    if not ok:
        print(f"Rejected: {reason}")
        return

    run(["git", "checkout", "-B", "agent"])
    apply_changes(spec, request_id)
    run(["git", "add", spec["module_path"], "AGENT_REQUESTS.md", str(report / f"{request_id}.json")])
    run(["git", "commit", "-m", f"agent: implement requested tool {spec['tool_name']} ({request_id})"])

    if args.push:
        run(["git", "push", "-u", "origin", "agent"])

    print("Implemented on agent branch. Open PR for human approval.")


if __name__ == "__main__":
    main()
