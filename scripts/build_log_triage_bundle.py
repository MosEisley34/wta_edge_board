#!/usr/bin/env python3
"""Build a compact triage bundle from raw runtime logs with strict size caps."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from runtime_json_records import iter_json_records

DEFAULT_MAX_CHARS = 30_000
DEFAULT_TOP_CODES = 10
DEFAULT_TOP_FAILURES = 3
DEFAULT_SAMPLES_PER_FAILURE = 3
_STATUS_KEYS = ("status", "gate_status", "outcome")
_CODE_KEYS = ("reason_code", "code", "failure_code", "warning_code")
_TIME_KEYS = ("timestamp", "ts", "time", "created_at", "updated_at", "run_ts")


def _normalize_status(record: dict[str, Any]) -> str:
    for key in _STATUS_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return "unknown"


def _classify_severity(status: str) -> str:
    if "warn" in status:
        return "warning"
    if "insufficient" in status:
        return "failure"
    if "fail" in status or "error" in status:
        return "failure"
    return "other"


def _extract_code(record: dict[str, Any]) -> str:
    for key in _CODE_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown"


def _extract_run_id(record: dict[str, Any]) -> str | None:
    for key in ("run_id", "runId", "run"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_timestamp(record: dict[str, Any]) -> str | None:
    for key in _TIME_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _dominant_failures(failure_counts: Counter[str], limit: int) -> list[str]:
    return [code for code, _ in failure_counts.most_common(limit)]


def _fingerprint_block(records: list[dict[str, Any]]) -> dict[str, Any]:
    digest_counts: Counter[str] = Counter()
    for record in records:
        canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest_counts[hashlib.sha256(canonical.encode("utf-8")).hexdigest()] += 1
    duplicates = [
        {"fingerprint": digest, "count": count}
        for digest, count in digest_counts.most_common()
        if count > 1
    ]
    return {
        "record_fingerprint_count": len(digest_counts),
        "duplicate_fingerprint_count": len(duplicates),
        "top_duplicate_fingerprints": duplicates[:20],
    }


def _encode_bundle(bundle: dict[str, Any]) -> str:
    return json.dumps(bundle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _enforce_size_cap(
    bundle: dict[str, Any],
    max_chars: int,
    dominant_failures: list[str],
) -> tuple[dict[str, Any], bool]:
    trimmed = False

    def over_cap() -> bool:
        return len(_encode_bundle(bundle)) > max_chars

    if not over_cap():
        return bundle, trimmed

    # 1) Drop lowest priority optional diagnostics first.
    for section in ("fingerprint", "transition_points"):
        if section in bundle:
            bundle.pop(section, None)
            trimmed = True
            if not over_cap():
                return bundle, trimmed

    # 2) Trim non-dominant sample groups.
    samples = bundle.get("representative_samples") or {}
    if samples:
        non_dominant = [code for code in list(samples.keys()) if code not in dominant_failures]
        for code in non_dominant:
            samples.pop(code, None)
            trimmed = True
            if not over_cap():
                return bundle, trimmed

    # 3) Reduce retained exemplars while preserving at least one per dominant failure.
    if samples:
        changed = True
        while over_cap() and changed:
            changed = False
            for code in dominant_failures:
                exemplars = samples.get(code)
                if isinstance(exemplars, list) and len(exemplars) > 1:
                    exemplars.pop()
                    changed = True
                    trimmed = True
                    if not over_cap():
                        return bundle, trimmed

    # 4) Last resort: preserve at least one exemplar for the most dominant failure.
    if over_cap() and samples:
        best = dominant_failures[0] if dominant_failures else next(iter(samples.keys()), None)
        preserved: dict[str, Any] = {}
        if best and samples.get(best):
            preserved[best] = [samples[best][0]]
        bundle["representative_samples"] = preserved
        trimmed = True

    return bundle, trimmed


def build_bundle(
    records: list[dict[str, Any]],
    max_chars: int,
    samples_per_failure: int,
    top_codes: int,
    top_failures: int,
    include_fingerprint: bool,
) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    failure_warning_codes: Counter[str] = Counter()
    failure_only_codes: Counter[str] = Counter()
    timestamps: list[str] = []
    run_ids: Counter[str] = Counter()
    representative_pool: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    transition_points: list[dict[str, Any]] = []
    previous_transition_status: str | None = None

    for idx, record in enumerate(records, start=1):
        status = _normalize_status(record)
        code = _extract_code(record)
        status_counts[status] += 1

        severity = _classify_severity(status)
        if severity in {"failure", "warning"}:
            failure_warning_codes[code] += 1
        if severity == "failure":
            failure_only_codes[code] += 1
            if len(representative_pool[code]) < samples_per_failure:
                representative_pool[code].append(record)

        ts = _extract_timestamp(record)
        if ts:
            timestamps.append(ts)
        run_id = _extract_run_id(record)
        if run_id:
            run_ids[run_id] += 1

        if status in {"fail", "insufficient_sample"}:
            if previous_transition_status and previous_transition_status != status:
                transition_points.append(
                    {
                        "index": idx,
                        "from": previous_transition_status,
                        "to": status,
                        "code": code,
                        "timestamp": ts,
                    }
                )
            previous_transition_status = status

    dominant = _dominant_failures(failure_only_codes, top_failures)
    representative_samples = {code: representative_pool[code] for code in dominant if representative_pool.get(code)}

    top_code_rows = [
        {"code": code, "count": count}
        for code, count in failure_warning_codes.most_common(top_codes)
    ]

    timeframe: dict[str, Any] = {"start": None, "end": None}
    if timestamps:
        timeframe = {"start": min(timestamps), "end": max(timestamps)}

    run_id = run_ids.most_common(1)[0][0] if run_ids else None

    bundle: dict[str, Any] = {
        "metadata": {
            "run_id": run_id,
            "timeframe": timeframe,
            "total_records": len(records),
        },
        "status_counts": dict(status_counts),
        "top_failure_warning_codes": top_code_rows,
        "transition_points": transition_points,
        "representative_samples": representative_samples,
    }

    if include_fingerprint:
        bundle["fingerprint"] = _fingerprint_block(records)

    bundle, trimmed = _enforce_size_cap(bundle, max_chars=max_chars, dominant_failures=dominant)
    encoded = _encode_bundle(bundle)
    if len(encoded) > max_chars:
        # Keep required summary plus one top exemplar if available.
        fallback_samples = bundle.get("representative_samples") or {}
        first_code = dominant[0] if dominant else next(iter(fallback_samples.keys()), None)
        preserved_samples = {}
        if first_code and fallback_samples.get(first_code):
            preserved_samples[first_code] = [fallback_samples[first_code][0]]
        bundle = {
            "metadata": bundle.get("metadata", {}),
            "status_counts": bundle.get("status_counts", {}),
            "top_failure_warning_codes": bundle.get("top_failure_warning_codes", []),
            "representative_samples": preserved_samples,
            "truncation_notice": "Output exceeded cap; optional and lower-priority sections were removed.",
        }
        trimmed = True
        encoded = _encode_bundle(bundle)
        if len(encoded) > max_chars:
            # Absolute guard: remove samples only if unavoidable.
            bundle["representative_samples"] = {}
            bundle["truncation_notice"] = "Output exceeded cap; representative samples removed as last resort."

    if trimmed:
        bundle["trimmed_to_fit"] = True

    # Absolute output guard (includes final metadata flags).
    encoded = _encode_bundle(bundle)
    if len(encoded) > max_chars:
        bundle.pop("trimmed_to_fit", None)
        encoded = _encode_bundle(bundle)
    if len(encoded) > max_chars:
        top_codes_section = bundle.get("top_failure_warning_codes")
        if isinstance(top_codes_section, list) and top_codes_section:
            bundle["top_failure_warning_codes"] = top_codes_section[:1]
            encoded = _encode_bundle(bundle)
    if len(encoded) > max_chars:
        samples = bundle.get("representative_samples")
        if isinstance(samples, dict) and samples:
            first_code = next(iter(samples.keys()))
            first_items = samples.get(first_code) or []
            bundle["representative_samples"] = {first_code: first_items[:1]} if first_items else {}
    return bundle


def _load_records(path: Path) -> list[dict[str, Any]]:
    data = path.read_text(encoding="utf-8")
    records: list[dict[str, Any]] = []
    for _, row in iter_json_records(data):
        if isinstance(row, dict):
            records.append(row)
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Path to raw log JSON/NDJSON file")
    parser.add_argument("--out", type=Path, default=None, help="Optional output path (default: stdout)")
    parser.add_argument("--max-chars", type=int, default=DEFAULT_MAX_CHARS, help="Maximum output characters")
    parser.add_argument("--samples-per-failure", type=int, default=DEFAULT_SAMPLES_PER_FAILURE)
    parser.add_argument("--top-codes", type=int, default=DEFAULT_TOP_CODES)
    parser.add_argument("--top-failures", type=int, default=DEFAULT_TOP_FAILURES)
    parser.add_argument("--include-fingerprint", action="store_true")
    args = parser.parse_args()

    records = _load_records(args.input)
    bundle = build_bundle(
        records=records,
        max_chars=args.max_chars,
        samples_per_failure=max(1, args.samples_per_failure),
        top_codes=max(1, args.top_codes),
        top_failures=max(1, args.top_failures),
        include_fingerprint=args.include_fingerprint,
    )
    rendered = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",",":"))
    if args.out:
        args.out.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
