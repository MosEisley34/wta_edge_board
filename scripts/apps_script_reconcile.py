#!/usr/bin/env python3
"""Compare Google Apps Script live files with repo modules and optionally clean stale files.

Usage:
  python scripts/apps_script_reconcile.py --script-id <ID> --access-token <TOKEN> --apply-delete

By default runs in dry-run mode and prints a JSON report.
"""
from __future__ import annotations

import argparse
import datetime as dt
import difflib
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

API_URL = "https://script.googleapis.com/v1/projects/{script_id}/content"
TOP_LEVEL_DECL_PATTERNS = [
    re.compile(r"^\s*function\s+([A-Za-z_$][\w$]*)\s*\("),
    re.compile(r"^\s*(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*="),
]


@dataclass
class LiveFile:
    name: str
    file_type: str
    source: str


def _http_json(url: str, method: str, token: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    req = urllib.request.Request(url=url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    try:
        with urllib.request.urlopen(req, data=data, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {err.code} {err.reason}: {body}") from err


def fetch_live_files(script_id: str, token: str) -> list[LiveFile]:
    payload = _http_json(API_URL.format(script_id=script_id), "GET", token)
    files = []
    for item in payload.get("files", []):
        files.append(
            LiveFile(
                name=item.get("name", ""),
                file_type=item.get("type", ""),
                source=item.get("source", ""),
            )
        )
    return files


def update_live_files(script_id: str, token: str, files: list[dict[str, str]]) -> None:
    _http_json(API_URL.format(script_id=script_id), "PUT", token, payload={"files": files})


def repo_modules(repo_root: pathlib.Path) -> dict[str, str]:
    modules: dict[str, str] = {}
    for path in sorted(repo_root.glob("*.gs")):
        modules[path.stem] = path.read_text(encoding="utf-8")
    return modules


def _top_level_lines(source: str) -> list[str]:
    lines = source.splitlines()
    out: list[str] = []
    depth = 0
    for line in lines:
        if depth == 0:
            out.append(line)
        # Approximate JS brace depth; sufficient for duplicate-global detection.
        opens = line.count("{")
        closes = line.count("}")
        depth += opens - closes
        if depth < 0:
            depth = 0
    return out


def extract_globals(sources: dict[str, str]) -> dict[str, list[str]]:
    seen: dict[str, list[str]] = {}
    for module_name, source in sources.items():
        for line in _top_level_lines(source):
            for pattern in TOP_LEVEL_DECL_PATTERNS:
                match = pattern.search(line)
                if not match:
                    continue
                ident = match.group(1)
                seen.setdefault(ident, []).append(module_name)
    return {k: sorted(set(v)) for k, v in seen.items() if len(set(v)) > 1}


def near_duplicates(names_a: list[str], names_b: list[str], cutoff: float = 0.80) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for a in names_a:
        for b in names_b:
            if a == b:
                continue
            score = difflib.SequenceMatcher(a=a.lower(), b=b.lower()).ratio()
            if score >= cutoff:
                findings.append({"left": a, "right": b, "similarity": round(score, 3)})
    dedup = {}
    for item in findings:
        key = tuple(sorted([item["left"], item["right"]]))
        dedup[key] = item
    return sorted(dedup.values(), key=lambda x: (-x["similarity"], x["left"], x["right"]))


def archive_name(name: str) -> str:
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d")
    return f"ARCHIVE_{stamp}_{name}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--script-id", default=os.getenv("APPS_SCRIPT_ID"))
    parser.add_argument("--access-token", default=os.getenv("GOOGLE_ACCESS_TOKEN"))
    parser.add_argument("--apply-delete", action="store_true", help="Remove stale files from live project")
    parser.add_argument("--apply-archive", action="store_true", help="Rename stale files with ARCHIVE_* prefix")
    parser.add_argument("--include-tests", action="store_true", help="Keep *_tests modules as canonical")
    args = parser.parse_args()

    if args.apply_delete and args.apply_archive:
        parser.error("Choose only one of --apply-delete or --apply-archive")

    repo_root = pathlib.Path(args.repo_root).resolve()
    repo_sources = repo_modules(repo_root)
    canonical = sorted(repo_sources.keys())
    if not args.include_tests:
        canonical = [name for name in canonical if not name.endswith("_tests")]

    report: dict[str, Any] = {
        "repo_module_count": len(canonical),
        "repo_modules": canonical,
    }

    duplicate_globals = extract_globals({k: v for k, v in repo_sources.items() if k in canonical})
    report["duplicate_repo_globals"] = duplicate_globals

    if not args.script_id or not args.access_token:
        report["live_project"] = "skipped (missing --script-id/--access-token)"
        print(json.dumps(report, indent=2))
        return 0

    live_files = fetch_live_files(args.script_id, args.access_token)
    live_js = [f for f in live_files if f.file_type == "SERVER_JS"]
    live_names = sorted(f.name for f in live_js)

    stale = [name for name in live_names if name not in canonical]
    missing_in_live = [name for name in canonical if name not in live_names]
    near_dup = near_duplicates(canonical, stale)

    report["live_server_js_count"] = len(live_names)
    report["live_server_js"] = live_names
    report["stale_in_live"] = stale
    report["missing_in_live"] = missing_in_live
    report["near_duplicate_name_alerts"] = near_dup

    if args.apply_delete or args.apply_archive:
        keep_files: list[dict[str, str]] = []
        for f in live_files:
            if f.file_type != "SERVER_JS" or f.name in canonical:
                keep_files.append({"name": f.name, "type": f.file_type, "source": f.source})
            elif args.apply_archive:
                keep_files.append({"name": archive_name(f.name), "type": f.file_type, "source": f.source})

        # Ensure canonical repo code replaces live canonical modules.
        current_names = {f["name"] for f in keep_files if f["type"] == "SERVER_JS"}
        for name in canonical:
            source = repo_sources[name]
            if name in current_names:
                for entry in keep_files:
                    if entry["type"] == "SERVER_JS" and entry["name"] == name:
                        entry["source"] = source
                        break
            else:
                keep_files.append({"name": name, "type": "SERVER_JS", "source": source})

        update_live_files(args.script_id, args.access_token, keep_files)
        report["applied"] = "delete" if args.apply_delete else "archive"

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
