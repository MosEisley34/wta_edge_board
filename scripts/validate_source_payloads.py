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
from fnmatch import fnmatch


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


def _load_json_if_exists(path: Path) -> object | None:
    if not path.exists():
        return None
    return _load_json(path)


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


def validate_ta_leadersource_wta(payload: str, source: str, payload_path: Path | None) -> ValidationResult:
    matchmx_rows = re.findall(r"\bmatchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[", payload)
    if matchmx_rows:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="ta_leadersource_matchmx_rows_detected",
            evidence_samples=matchmx_rows[:3],
            evidence_counts={"matchmx_row_markers": len(matchmx_rows)},
            payload_path=str(payload_path) if payload_path else None,
        )

    return ValidationResult(
        source=source,
        ready_for_extraction=False,
        reason_code="ta_leadersource_no_matchmx",
        evidence_samples=[],
        evidence_counts={"matchmx_row_markers": 0},
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


def _json_has_api_error(data: object) -> bool:
    if not isinstance(data, dict):
        return False

    error_value = data.get("error")
    if not isinstance(error_value, dict):
        return False

    error_code = error_value.get("code")
    error_message = error_value.get("message")
    return error_code is not None and error_message is not None


def validate_json_source(
    payload: str,
    source: str,
    payload_path: Path | None,
    marker_paths: list[tuple[str, ...]],
    required_paths: list[tuple[str, ...]],
    allow_structural_marker_fallback: bool = False,
) -> ValidationResult:
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

    if _json_has_api_error(data):
        return ValidationResult(
            source=source,
            ready_for_extraction=False,
            reason_code="json_api_error_payload",
            evidence_samples=["error.code", "error.message"],
            evidence_counts={"api_error_payload": 1},
            payload_path=str(payload_path) if payload_path else None,
        )

    missing_required_paths = [".".join(path) for path in required_paths if not _json_path_exists(data, path)]
    if missing_required_paths:
        return ValidationResult(
            source=source,
            ready_for_extraction=False,
            reason_code="json_contract_required_paths_missing",
            evidence_samples=missing_required_paths[:4],
            evidence_counts={"missing_required_path_count": len(missing_required_paths)},
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

    required_path_hits = [".".join(path) for path in required_paths if _json_path_exists(data, path)]
    if required_path_hits:
        return ValidationResult(
            source=source,
            ready_for_extraction=True,
            reason_code="json_required_paths_detected",
            evidence_samples=required_path_hits[:4],
            evidence_counts={"required_path_count": len(required_path_hits)},
            payload_path=str(payload_path) if payload_path else None,
        )

    path_hits = [".".join(path) for path in marker_paths if _json_path_exists(data, path)]
    if allow_structural_marker_fallback and path_hits:
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
            ready_for_extraction=False,
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


def _parse_allowlist(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _is_allowed_source(source: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    return any(fnmatch(source, pattern) for pattern in allowlist)


def run_validations(out_dir: Path, mandatory_sources: set[str], provider_allowlist: list[str]) -> tuple[list[ValidationResult], bool, list[str]]:
    raw_dir = out_dir / "raw"
    validators: dict[str, Callable[[str, str, Path | None], ValidationResult]] = {
        "tennisabstract_leaders": validate_ta_leaders,
        "tennisabstract_leadersource_wta": validate_ta_leadersource_wta,
        "ta_h2h": validate_ta_h2h,
        "itf": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("data",), ("rankings",), ("results",), ("players",)],
            required_paths=[("data", "rankings")],
            allow_structural_marker_fallback=True,
        ),
        "tennisexplorer": validate_tennisexplorer,
        "sofascore_events_live": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        ),
        "sofascore_scheduled_events": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        ),
        "sofascore_player_detail": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("player",)],
            required_paths=[("player",)],
        ),
        "sofascore_player_recent": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        ),
        "sofascore_player_stats_overall": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("statistics",)],
            required_paths=[("statistics",)],
        ),
        "sofascore_player_stats_last52": lambda payload, source, payload_path: validate_json_source(
            payload,
            source,
            payload_path,
            marker_paths=[("statistics",)],
            required_paths=[("statistics",)],
        ),
    }

    results: list[ValidationResult] = []
    skipped_sources: list[str] = []
    all_mandatory_ready = True

    for source, validator in validators.items():
        if not _is_allowed_source(source, provider_allowlist):
            print(f"INFO: skipping provider '{source}' (not in provider allowlist: {','.join(provider_allowlist)})")
            skipped_sources.append(source)
            continue
        payload_path = _find_payload(raw_dir, source)
        if payload_path is None:
            result = _missing_payload_result(source)
        else:
            payload = _read_text(payload_path)
            result = validator(payload, source, payload_path)
        results.append(result)

        if source in mandatory_sources and not result.ready_for_extraction:
            all_mandatory_ready = False

    return results, all_mandatory_ready, skipped_sources



def _has_valid_tennis_player_prerequisite(out_dir: Path) -> tuple[bool, str]:
    payload_path = _find_payload(out_dir / "raw", "sofascore_player_detail")
    payload = _load_json(payload_path) if payload_path else None
    if not isinstance(payload, dict):
        return False, "player_detail_payload_unavailable"

    player = payload.get("player")
    if not isinstance(player, dict):
        return False, "player_detail_missing_player_object"

    sport = player.get("sport")
    sport_slug = ""
    if isinstance(sport, dict):
        sport_slug = str(sport.get("slug", "")).strip().lower()
    if sport_slug != "tennis":
        return False, "player_detail_not_tennis"

    player_id = player.get("id")
    if not (isinstance(player_id, int) and player_id > 0) and not (isinstance(player_id, str) and player_id.isdigit()):
        return False, "player_detail_invalid_player_id"

    return True, "valid_tennis_player_id_detected"


def _has_endpoint_support_prerequisite(out_dir: Path, source: str) -> tuple[bool, str]:
    parsed_path = out_dir / "parsed" / f"{source}.json"
    parsed = _load_json_if_exists(parsed_path)
    if not isinstance(parsed, dict):
        return False, "endpoint_probe_metadata_missing"

    if not parsed.get("transport_pass", False):
        return False, "endpoint_transport_not_ready"

    http_code = str(parsed.get("http_code", "")).strip()
    if not (len(http_code) == 3 and http_code.isdigit() and http_code.startswith("2")):
        return False, f"endpoint_http_not_2xx:{http_code or 'unknown'}"

    return True, "endpoint_support_detected"


def _classify_volatile_source_prerequisite(out_dir: Path, source: str) -> tuple[bool, str]:
    has_player_id, player_reason = _has_valid_tennis_player_prerequisite(out_dir)
    if not has_player_id:
        return False, player_reason

    has_endpoint_support, endpoint_reason = _has_endpoint_support_prerequisite(out_dir, source)
    if not has_endpoint_support:
        return False, endpoint_reason

    return True, "valid_tennis_player_id_and_endpoint_support"


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
        default=os.environ.get(
            "MANDATORY_SOURCES",
            "tennisabstract_leaders,tennisabstract_leadersource_wta,ta_h2h,sofascore_events_live,sofascore_scheduled_events,sofascore_player_detail,sofascore_player_recent",
        ),
        help="Comma-separated source keys that must be extraction-ready for zero exit (before allowlist filtering)",
    )
    parser.add_argument(
        "--conditional-mandatory-sources",
        default=os.environ.get(
            "CONDITIONAL_MANDATORY_SOURCES",
            "sofascore_player_stats_overall,sofascore_player_stats_last52",
        ),
        help="Comma-separated source keys that become mandatory only when prerequisites are met",
    )
    parser.add_argument(
        "--provider-allowlist",
        default=os.environ.get("PROVIDER_ALLOWLIST", "tennisabstract_*,ta_*,mcp_report_*,sofascore_*"),
        help="Comma-separated wildcard patterns for providers to validate",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir or Path.cwd() / "tmp" / "source_probes")
    if not args.out_dir and "OUT_DIR" in os.environ:
        out_dir = Path(os.environ["OUT_DIR"])

    provider_allowlist = _parse_allowlist(args.provider_allowlist)
    mandatory_sources_all = {item.strip() for item in args.mandatory_sources.split(",") if item.strip()}
    conditional_mandatory_sources_all = {
        item.strip() for item in args.conditional_mandatory_sources.split(",") if item.strip()
    }
    mandatory_sources = {source for source in mandatory_sources_all if _is_allowed_source(source, provider_allowlist)}
    conditional_mandatory_sources = {
        source for source in conditional_mandatory_sources_all if _is_allowed_source(source, provider_allowlist)
    }

    excluded_mandatory = sorted(mandatory_sources_all - mandatory_sources)
    excluded_conditional_mandatory = sorted(conditional_mandatory_sources_all - conditional_mandatory_sources)
    for source in excluded_mandatory:
        print(f"INFO: skipping mandatory provider '{source}' (not in provider allowlist: {args.provider_allowlist})")
    for source in excluded_conditional_mandatory:
        print(
            f"INFO: skipping conditional mandatory provider '{source}' "
            f"(not in provider allowlist: {args.provider_allowlist})"
        )

    prerequisite_skips: dict[str, str] = {}
    promoted_conditional_sources: set[str] = set()
    for source in sorted(conditional_mandatory_sources):
        prereq_ok, prereq_reason = _classify_volatile_source_prerequisite(out_dir, source)
        if prereq_ok:
            promoted_conditional_sources.add(source)
        else:
            prerequisite_skips[source] = prereq_reason
            print(f"INFO: skipping '{source}' due to unmet prerequisites ({prereq_reason})")

    effective_mandatory_sources = set(mandatory_sources) | promoted_conditional_sources

    results, all_mandatory_ready, skipped_sources = run_validations(
        out_dir,
        effective_mandatory_sources,
        provider_allowlist,
    )
    result_by_source = {r.source: r for r in results}

    mandatory_failures = [
        {"source": source, "reason": result_by_source[source].reason_code}
        for source in sorted(effective_mandatory_sources)
        if source in result_by_source and not result_by_source[source].ready_for_extraction
    ]
    optional_failures = [
        {"source": result.source, "reason": result.reason_code}
        for result in sorted(results, key=lambda item: item.source)
        if result.source not in effective_mandatory_sources
        and result.source not in prerequisite_skips
        and not result.ready_for_extraction
    ]
    skipped_by_prerequisite = [
        {"source": source, "reason": reason}
        for source, reason in sorted(prerequisite_skips.items())
    ]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_dir": str(out_dir),
        "raw_dir": str(out_dir / "raw"),
        "provider_allowlist": provider_allowlist,
        "mandatory_sources": sorted(mandatory_sources),
        "conditional_mandatory_sources": sorted(conditional_mandatory_sources),
        "conditional_mandatory_promoted": sorted(promoted_conditional_sources),
        "mandatory_sources_excluded_by_allowlist": excluded_mandatory,
        "conditional_mandatory_excluded_by_allowlist": excluded_conditional_mandatory,
        "skipped_sources": skipped_sources,
        "mandatory_failures": mandatory_failures,
        "optional_failures": optional_failures,
        "skipped_by_prerequisite": skipped_by_prerequisite,
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
