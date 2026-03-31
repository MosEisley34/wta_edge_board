#!/usr/bin/env python3
"""Shared runEdgeBoard summary row normalization and cardinality diagnostics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def is_run_edgeboard_summary_row(row: dict[str, Any]) -> bool:
    return (
        str(row.get("row_type") or "").strip() == "summary"
        and str(row.get("stage") or "").strip() == "runEdgeBoard"
    )


def _summary_identity_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("run_id") or ""),
        str(row.get("row_type") or ""),
        str(row.get("stage") or ""),
        str(row.get("started_at") or ""),
        str(row.get("ended_at") or ""),
    )


def merge_run_summary_rows_for_cardinality(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    deduped_rows: list[dict[str, Any]] = []
    summary_row_indexes_by_identity: dict[tuple[str, str, str, str, str], int] = {}
    summary_identity_counts_by_run_id: dict[str, dict[tuple[str, str, str, str, str], int]] = defaultdict(dict)
    summary_raw_count_by_run_id: dict[str, int] = defaultdict(int)

    for row in rows:
        if not is_run_edgeboard_summary_row(row):
            deduped_rows.append(row)
            continue

        run_id = str(row.get("run_id") or "")
        identity_key = _summary_identity_key(row)
        summary_raw_count_by_run_id[run_id] += 1
        identity_counts = summary_identity_counts_by_run_id[run_id]
        identity_counts[identity_key] = identity_counts.get(identity_key, 0) + 1

        existing_index = summary_row_indexes_by_identity.get(identity_key)
        if existing_index is None:
            deduped_row = dict(row)
            source_kind = str(row.get("_source_kind") or "unknown")
            deduped_row["merged_from_sources"] = [source_kind]
            deduped_rows.append(deduped_row)
            summary_row_indexes_by_identity[identity_key] = len(deduped_rows) - 1
            continue

        existing_row = deduped_rows[existing_index]
        merged_sources = existing_row.get("merged_from_sources")
        if not isinstance(merged_sources, list):
            merged_sources = []
        source_kind = str(row.get("_source_kind") or "unknown")
        if source_kind not in merged_sources:
            merged_sources.append(source_kind)
            merged_sources.sort()
        existing_row["merged_from_sources"] = merged_sources

    duplicate_diagnostics_by_run_id: dict[str, dict[str, Any]] = {}
    for run_id, identity_counts in summary_identity_counts_by_run_id.items():
        duplicate_instances = sum(max(0, count - 1) for count in identity_counts.values())
        identical_duplicate_groups = sum(1 for count in identity_counts.values() if count > 1)
        unique_summary_rows = len(identity_counts)
        raw_summary_rows = int(summary_raw_count_by_run_id.get(run_id, 0))
        has_non_identical_duplicates = raw_summary_rows > 1 and unique_summary_rows > 1
        duplicate_diagnostics_by_run_id[run_id] = {
            "raw_summary_rows": raw_summary_rows,
            "unique_summary_rows": unique_summary_rows,
            "duplicate_instances": duplicate_instances,
            "identical_duplicate_groups": identical_duplicate_groups,
            "has_duplicate_summary_rows": raw_summary_rows > 1,
            "has_non_identical_duplicate_summary_rows": has_non_identical_duplicates,
            "compare_will_fail_due_to_duplicate_summary_rows": has_non_identical_duplicates,
        }

    return deduped_rows, duplicate_diagnostics_by_run_id
