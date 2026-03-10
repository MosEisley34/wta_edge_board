#!/usr/bin/env python3
"""Extract normalized player features into JSONL and CSV artifacts.

Reads source payloads from OUT_DIR/raw (or --out-dir) and writes:
- OUT_DIR/normalized/player_features.jsonl
- OUT_DIR/normalized/player_features.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_COLUMNS = [
    "player_canonical_name",
    "source",
    "as_of",
    "ranking",
    "recent_form",
    "surface_win_rate",
    "hold_pct",
    "break_pct",
    "h2h_wins",
    "h2h_losses",
    "has_stats",
    "reason_code",
    "reason_code_detail",
]


CATALOG_PATH = Path(__file__).resolve().parents[1] / "config" / "probe_sources.tsv"


def _load_selected_sources(catalog_path: Path) -> set[str]:
    if not catalog_path.exists():
        return set()

    selected: set[str] = set()
    for line in catalog_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        row = line.strip()
        if not row or row.startswith("#"):
            continue
        source_key = row.split("\t", 1)[0].strip()
        if source_key:
            selected.add(source_key)
    return selected


def _is_html_payload(path: Path, text: str) -> bool:
    suffix = path.suffix.lower()
    if suffix in {".html", ".htm"}:
        return True
    if suffix == ".body":
        marker = text[:4096].lower()
        if "<html" in marker or "<!doctype html" in marker or "<head" in marker or "<body" in marker:
            return True
    return False


def _detect_payload_mode(source: str, path: Path, text: str) -> str:
    if "matchmx" in text:
        return "matchmx"

    json_sources = {
        "itf",
        "sofascore_events_live",
        "sofascore_scheduled_events",
        "sofascore_player_detail",
        "sofascore_player_recent",
        "sofascore_player_stats_overall",
        "sofascore_player_stats_last52",
    }
    if source in json_sources:
        return "json"

    if source in {"tennisabstract_leaders", "tennisabstract_leadersource_wta"}:
        return "matchmx"

    if _is_html_payload(path, text):
        return "html"

    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"

    if path.suffix.lower() == ".csv":
        return "csv"

    if source in {"tennisexplorer", "ta_h2h"}:
        return "html"

    return "unknown"


@dataclass
class PlayerFeature:
    player_canonical_name: str | None
    source: str
    as_of: str
    ranking: int | None
    recent_form: float | None
    surface_win_rate: float | None
    hold_pct: float | None
    break_pct: float | None
    h2h_wins: int | None
    h2h_losses: int | None
    has_stats: bool
    reason_code: str
    reason_code_detail: str | None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "na", "n/a", "-"}:
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: object) -> int | None:
    number = _to_float(value)
    if number is None:
        return None
    try:
        return int(round(number))
    except (TypeError, ValueError):
        return None


def _pick(obj: dict[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


def _normalize_name(value: object) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", str(value)).strip()
    return normalized or None


def _has_stats(row: PlayerFeature) -> bool:
    return any(
        value is not None
        for value in [
            row.ranking,
            row.recent_form,
            row.surface_win_rate,
            row.hold_pct,
            row.break_pct,
            row.h2h_wins,
            row.h2h_losses,
        ]
    )


def _parse_matchmx_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    rows: list[PlayerFeature] = []
    for match in re.finditer(r"matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;", text):
        tokens = re.findall(r'"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\'|([^,]+)', match.group(1))
        values: list[str] = []
        for quoted_a, quoted_b, unquoted in tokens:
            value = quoted_a or quoted_b or unquoted.strip()
            values.append(value)
        if len(values) < 11:
            continue

        player_name = _normalize_name(values[3] if len(values) > 3 else None)
        feature = PlayerFeature(
            player_canonical_name=player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(values[1] if len(values) > 1 else None),
            recent_form=_to_float(values[7] if len(values) > 7 else None),
            surface_win_rate=_to_float(values[8] if len(values) > 8 else None),
            hold_pct=_to_float(values[9] if len(values) > 9 else None),
            break_pct=_to_float(values[10] if len(values) > 10 else None),
            h2h_wins=None,
            h2h_losses=None,
            has_stats=False,
            reason_code="ok",
            reason_code_detail="normalized_from_matchmx",
        )
        feature.has_stats = _has_stats(feature)
        if feature.player_canonical_name is None:
            feature.reason_code = "missing_player_name"
        elif not feature.has_stats:
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)
    return rows


def _records_from_json(data: object) -> Iterable[dict[str, object]]:
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row
        return
    if isinstance(data, dict):
        if "players" in data and isinstance(data["players"], list):
            for row in data["players"]:
                if isinstance(row, dict):
                    yield row
            return
        if "data" in data and isinstance(data["data"], list):
            for row in data["data"]:
                if isinstance(row, dict):
                    yield row
            return
        if all(not isinstance(v, (dict, list)) for v in data.values()):
            yield data


def _parse_json_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    try:
        data = json.loads(text)
    except Exception:
        return []

    rows: list[PlayerFeature] = []
    for obj in _records_from_json(data):
        player_name = _normalize_name(_pick(obj, "player_canonical_name", "player", "player_name", "name", "athlete"))
        feature = PlayerFeature(
            player_canonical_name=player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(_pick(obj, "ranking", "rank", "wta_rank")),
            recent_form=_to_float(_pick(obj, "recent_form", "form", "form_score")),
            surface_win_rate=_to_float(_pick(obj, "surface_win_rate", "surface_wr", "surface_win_pct")),
            hold_pct=_to_float(_pick(obj, "hold_pct", "hold", "service_hold_pct")),
            break_pct=_to_float(_pick(obj, "break_pct", "break", "return_break_pct")),
            h2h_wins=_to_int(_pick(obj, "h2h_wins", "head_to_head_wins")),
            h2h_losses=_to_int(_pick(obj, "h2h_losses", "head_to_head_losses")),
            has_stats=False,
            reason_code=str(_pick(obj, "reason_code") or "ok"),
            reason_code_detail=str(_pick(obj, "reason_code_detail", "diagnostic") or "normalized_from_json"),
        )
        feature.has_stats = _has_stats(feature)
        if feature.player_canonical_name is None:
            feature.reason_code = "missing_player_name"
        elif not feature.has_stats and feature.reason_code == "ok":
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)
    return rows


def _parse_csv_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    reader = csv.DictReader(text.splitlines())
    rows: list[PlayerFeature] = []
    for obj in reader:
        player_name = _normalize_name(_pick(obj, "player_canonical_name", "player", "player_name", "name"))
        feature = PlayerFeature(
            player_canonical_name=player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(_pick(obj, "ranking", "rank", "wta_rank")),
            recent_form=_to_float(_pick(obj, "recent_form", "form", "form_score")),
            surface_win_rate=_to_float(_pick(obj, "surface_win_rate", "surface_win_pct")),
            hold_pct=_to_float(_pick(obj, "hold_pct", "service_hold_pct")),
            break_pct=_to_float(_pick(obj, "break_pct", "return_break_pct")),
            h2h_wins=_to_int(_pick(obj, "h2h_wins")),
            h2h_losses=_to_int(_pick(obj, "h2h_losses")),
            has_stats=False,
            reason_code=str(_pick(obj, "reason_code") or "ok"),
            reason_code_detail=str(_pick(obj, "reason_code_detail") or "normalized_from_csv"),
        )
        feature.has_stats = _has_stats(feature)
        if feature.player_canonical_name is None:
            feature.reason_code = "missing_player_name"
        elif not feature.has_stats and feature.reason_code == "ok":
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)
    return rows


def _extract_from_file(path: Path, selected_sources: set[str]) -> list[PlayerFeature]:
    source = path.stem.split(".")[0]
    as_of = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()

    if selected_sources and source not in selected_sources:
        return []

    text = path.read_text(encoding="utf-8", errors="ignore")
    payload_mode = _detect_payload_mode(source, path, text)

    if payload_mode == "matchmx":
        rows = _parse_matchmx_rows(source, text, as_of)
        if rows:
            return rows

    if payload_mode == "json":
        json_rows = _parse_json_rows(source, text, as_of)
        if json_rows:
            return json_rows

    if payload_mode == "csv":
        csv_rows = _parse_csv_rows(source, text, as_of)
        if csv_rows:
            return csv_rows

    return [
        PlayerFeature(
            player_canonical_name=None,
            source=source,
            as_of=as_of,
            ranking=None,
            recent_form=None,
            surface_win_rate=None,
            hold_pct=None,
            break_pct=None,
            h2h_wins=None,
            h2h_losses=None,
            has_stats=False,
            reason_code="source_parse_error",
            reason_code_detail=f"unsupported_or_empty_payload:{path.name}",
        )
    ]


def _quality_score(row: PlayerFeature) -> int:
    score = 0
    for value in [
        row.ranking,
        row.recent_form,
        row.surface_win_rate,
        row.hold_pct,
        row.break_pct,
        row.h2h_wins,
        row.h2h_losses,
    ]:
        if value is not None:
            score += 1
    return score


def _dedupe_rows(rows: list[PlayerFeature]) -> list[PlayerFeature]:
    best: dict[tuple[str, str], PlayerFeature] = {}
    passthrough: list[PlayerFeature] = []

    for row in rows:
        if row.player_canonical_name is None:
            passthrough.append(row)
            continue
        key = (row.player_canonical_name.lower(), row.source)
        existing = best.get(key)
        if existing is None or _quality_score(row) > _quality_score(existing):
            best[key] = row

    ordered = sorted(best.values(), key=lambda item: (item.source, item.player_canonical_name or ""))
    return passthrough + ordered


def _write_jsonl(path: Path, rows: list[PlayerFeature]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")


def _write_csv(path: Path, rows: list[PlayerFeature]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SCHEMA_COLUMNS)
        writer.writeheader()
        for row in rows:
            data = asdict(row)
            writer.writerow({key: data.get(key) for key in SCHEMA_COLUMNS})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract normalized player features from source payloads")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory containing raw/ payloads (default: $OUT_DIR or ./tmp/source_probes)",
    )
    parser.add_argument(
        "--source-catalog",
        default=str(CATALOG_PATH),
        help="Path to source catalog used to determine selected providers",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir or Path.cwd() / "tmp" / "source_probes")
    if not args.out_dir and "OUT_DIR" in os.environ:
        out_dir = Path(os.environ["OUT_DIR"])

    raw_dir = out_dir / "raw"
    normalized_dir = out_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        print(f"ERROR: raw payload directory not found: {raw_dir}", file=sys.stderr)
        return 1

    payload_files = sorted(path for path in raw_dir.iterdir() if path.is_file())
    if not payload_files:
        print(f"ERROR: no payload files found in {raw_dir}", file=sys.stderr)
        return 1

    selected_sources = _load_selected_sources(Path(args.source_catalog))

    extracted: list[PlayerFeature] = []
    for path in payload_files:
        extracted.extend(_extract_from_file(path, selected_sources))

    normalized = _dedupe_rows(extracted)
    jsonl_path = normalized_dir / "player_features.jsonl"
    csv_path = normalized_dir / "player_features.csv"
    _write_jsonl(jsonl_path, normalized)
    _write_csv(csv_path, normalized)

    print(f"Wrote {len(normalized)} rows to {jsonl_path}")
    print(f"Wrote {len(normalized)} rows to {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
