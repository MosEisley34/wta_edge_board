#!/usr/bin/env python3
"""Validate raw source payloads for extraction readiness.

Reads probe artifacts from OUT_DIR/raw and emits OUT_DIR/validation_report.json.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class ValidationResult:
    source: str
    ready_for_extraction: bool
    reason_code: str
    evidence_samples: list[str]
    evidence_counts: dict[str, int]
    payload_path: str | None


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def _load_json(path: Path) -> object | None:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return None


def _find_payload(raw_dir: Path, source: str) -> Path | None:
    preferred = raw_dir / f"{source}.body"
    if preferred.exists():
        return preferred
    candidates = sorted(raw_dir.glob(f"{source}*"))
    return candidates[0] if candidates else None


def validate_ta_leaders(payload: str, source: str, payload_path: Path | None) -> ValidationResult:
    pointer_matches = re.findall(
        r"(?:https?:)?//[^\"'\s]*jsmatches/[^\"'\s]*leadersource[^\"'\s]*wta\.js|/?jsmatches/[^\"'\s]*leadersource[^\"'\s]*wta\.js",
        payload,
        flags=re.IGNORECASE,
    )
    matchmx_rows = re.findall(r"\bmatchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[", payload)

    if pointer_matches:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="ta_leaders_js_pointer_detected",
            evidence_samples=pointer_matches[:3],
            evidence_counts={"js_pointer_count": len(pointer_matches), "matchmx_row_markers": len(matchmx_rows)},
            payload_path=str(payload_path) if payload_path else None,
        )

    if matchmx_rows:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="ta_leaders_matchmx_rows_detected",
            evidence_samples=matchmx_rows[:3],
            evidence_counts={"js_pointer_count": len(pointer_matches), "matchmx_row_markers": len(matchmx_rows)},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="ta_leaders_no_pointer_or_matchmx",
        evidence_samples=[],
        evidence_counts={"js_pointer_count": 0, "matchmx_row_markers": 0},
        payload_path=str(payload_path) if payload_path else None,
    )


def validate_ta_h2h(payload: str, source: str, payload_path: Path | None) -> ValidationResult:
    anchor_matches = re.findall(r"/cgi-bin/(?:player-classic|h2h)\.cgi\?[^\"'\s<]+", payload)
    matrix_markers = re.findall(r"h2hMatrix|<table[^>]*>", payload, flags=re.IGNORECASE)
    explicit_empty = bool(
        re.search(
            r"ta_h2h_empty_table|no\s+matches\s+found|no\s+data\s+available|empty\s+table",
            payload,
            flags=re.IGNORECASE,
        )
    )

    if anchor_matches or matrix_markers:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="ta_h2h_patterns_detected",
            evidence_samples=(anchor_matches[:2] + matrix_markers[:2])[:4],
            evidence_counts={"anchor_count": len(anchor_matches), "matrix_marker_count": len(matrix_markers)},
            payload_path=str(payload_path) if payload_path else None,
        )

    if explicit_empty:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="ta_h2h_explicit_empty_table",
            evidence_samples=["explicit_empty_table_marker_detected"],
            evidence_counts={"anchor_count": 0, "matrix_marker_count": 0},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="ta_h2h_no_matrix_or_anchor_patterns",
        evidence_samples=[],
        evidence_counts={"anchor_count": len(anchor_matches), "matrix_marker_count": len(matrix_markers)},
        payload_path=str(payload_path) if payload_path else None,
    )


def validate_wta_stats_zone(payload: str, source: str, payload_path: Path | None) -> ValidationResult:
    markers = [
        "__NEXT_DATA__",
        "window.__INITIAL_STATE__",
        "window.__NUXT__",
        "_next/static",
        "application/ld+json",
    ]
    hits = [marker for marker in markers if marker in payload]
    if hits:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="wta_structural_markers_detected",
            evidence_samples=hits[:4],
            evidence_counts={"marker_count": len(hits)},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="wta_missing_structural_markers",
        evidence_samples=[],
        evidence_counts={"marker_count": 0},
        payload_path=str(payload_path) if payload_path else None,
    )


def _json_path_exists(obj: object, path: tuple[str, ...]) -> bool:
    node = obj
    for key in path:
        if isinstance(node, dict) and key in node:
            node = node[key]
        else:
            return False
    return True


def _json_is_404_error_payload(data: object) -> bool:
    if not isinstance(data, dict):
        return False

    candidates = [data.get("code"), data.get("status"), data.get("statusCode")]
    for value in candidates:
        if value is None:
            continue
        if str(value).strip() == "404":
            return True

    error_value = data.get("error")
    if isinstance(error_value, dict):
        nested_candidates = [error_value.get("code"), error_value.get("status"), error_value.get("statusCode")]
        for value in nested_candidates:
            if value is None:
                continue
            if str(value).strip() == "404":
                return True
    return False


def validate_json_source(payload: str, source: str, payload_path: Path | None, marker_paths: list[tuple[str, ...]]) -> ValidationResult:
    data = _load_json(payload_path) if payload_path else None
    if data is None:
        bootstrap_hits = re.findall(r"__NEXT_DATA__|window\.__INITIAL_STATE__|bootstrap", payload, flags=re.IGNORECASE)
        if bootstrap_hits:
            return ValidationResult(
                source=source,
                ready_for_extraction=True,
                reason_code="json_bootstrap_marker_detected",
                evidence_samples=bootstrap_hits[:3],
                evidence_counts={"bootstrap_marker_count": len(bootstrap_hits)},
                payload_path=str(payload_path) if payload_path else None,
            )
        return ValidationResult(
            source=source,
            ready_for_extraction=False,
            reason_code="invalid_json_or_missing_bootstrap",
            evidence_samples=[],
            evidence_counts={"bootstrap_marker_count": 0},
            payload_path=str(payload_path) if payload_path else None,
        )

    if _json_is_404_error_payload(data):
        return ValidationResult(
            source=source,
            ready_for_extraction=False,
            reason_code="json_404_error_payload",
            evidence_samples=["404_error_payload_detected"],
            evidence_counts={"http_404_payload": 1},
            payload_path=str(payload_path) if payload_path else None,
        )

    if isinstance(data, list):
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="json_array_payload",
            evidence_samples=["top_level=list"],
            evidence_counts={"top_level_list_length": len(data)},
            payload_path=str(payload_path) if payload_path else None,
        )

    path_hits = [".".join(path) for path in marker_paths if _json_path_exists(data, path)]
    if path_hits:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="json_structural_markers_detected",
            evidence_samples=path_hits[:4],
            evidence_counts={"marker_path_count": len(path_hits)},
            payload_path=str(payload_path) if payload_path else None,
        )

    key_count = len(data.keys()) if isinstance(data, dict) else 0
    if key_count > 0:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="json_nonempty_object_fallback",
            evidence_samples=["top_level=dict"],
            evidence_counts={"top_level_key_count": key_count},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="json_empty_or_unrecognized",
        evidence_samples=[],
        evidence_counts={"top_level_key_count": key_count},
        payload_path=str(payload_path) if payload_path else None,
    )


def validate_tennisexplorer(payload: str, source: str, payload_path: Path | None) -> ValidationResult:
    markers = re.findall(r"<table|ranking|ranklist|tennisexplorer", payload, flags=re.IGNORECASE)
    if markers:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="tennisexplorer_markers_detected",
            evidence_samples=markers[:4],
            evidence_counts={"marker_count": len(markers)},
            payload_path=str(payload_path) if payload_path else None,
        )

    bootstrap = re.findall(r"__NEXT_DATA__|window\.__INITIAL_STATE__", payload)
    if bootstrap:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="tennisexplorer_bootstrap_marker_detected",
            evidence_samples=bootstrap[:3],
            evidence_counts={"bootstrap_marker_count": len(bootstrap)},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="tennisexplorer_no_expected_markers",
        evidence_samples=[],
        evidence_counts={"marker_count": 0},
        payload_path=str(payload_path) if payload_path else None,
    )


def _missing_payload_result(source: str) -> ValidationResult:
    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="payload_missing",
        evidence_samples=[],
        evidence_counts={},
        payload_path=None,
    )


def run_validations(out_dir: Path, mandatory_sources: set[str]) -> tuple[list[ValidationResult], bool]:
    raw_dir = out_dir / "raw"
    validators: dict[str, Callable[[str, str, Path | None], ValidationResult]] = {
        "tennisabstract_leaders": validate_ta_leaders,
        "ta_h2h": validate_ta_h2h,
        "wta_stats_zone": validate_wta_stats_zone,
        "itf": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("data",), ("rankings",), ("results",), ("players",)]
        ),
        "tennisexplorer": validate_tennisexplorer,
        "sofascore_events_live": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("events",)]
        ),
        "sofascore_scheduled_events": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("events",)]
        ),
        "sofascore_player_detail": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("player",)]
        ),
        "sofascore_player_recent": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("events",)]
        ),
        "sofascore_player_stats_overall": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("statistics",)]
        ),
        "sofascore_player_stats_last52": lambda payload, source, payload_path: validate_json_source(
            payload, source, payload_path, marker_paths=[("statistics",)]
        ),
    }

    results: list[ValidationResult] = []
    all_mandatory_ready = True

    for source, validator in validators.items():
        payload_path = _find_payload(raw_dir, source)
        if payload_path is None:
            result = _missing_payload_result(source)
        else:
            payload = _read_text(payload_path)
            result = validator(payload, source, payload_path)
        results.append(result)

        if source in mandatory_sources and not result.ready_for_extraction:
            all_mandatory_ready = False

    return results, all_mandatory_ready


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate source payloads for extraction readiness")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Probe output directory containing raw/ (default: $OUT_DIR or ./tmp/source_probes)",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Path for validation_report.json (default: <out_dir>/validation_report.json)",
    )
    parser.add_argument(
        "--mandatory-sources",
        default="tennisabstract_leaders,ta_h2h,wta_stats_zone,itf,tennisexplorer,sofascore_events_live,sofascore_scheduled_events,sofascore_player_detail,sofascore_player_recent,sofascore_player_stats_overall,sofascore_player_stats_last52",
        help="Comma-separated source keys that must be extraction-ready for zero exit",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir or Path.cwd() / "tmp" / "source_probes")
    if not args.out_dir and "OUT_DIR" in os.environ:
        out_dir = Path(os.environ["OUT_DIR"])

    mandatory_sources = {item.strip() for item in args.mandatory_sources.split(",") if item.strip()}
    results, all_mandatory_ready = run_validations(out_dir, mandatory_sources)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_dir": str(out_dir),
        "raw_dir": str(out_dir / "raw"),
        "mandatory_sources": sorted(mandatory_sources),
        "all_mandatory_ready": all_mandatory_ready,
        "results": [r.__dict__ for r in results],
    }

    report_path = Path(args.report_path) if args.report_path else out_dir / "validation_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote validation report: {report_path}")
    print(f"all_mandatory_ready={str(all_mandatory_ready).lower()}")

    return 0 if all_mandatory_ready else 1


if __name__ == "__main__":
    sys.exit(main())
