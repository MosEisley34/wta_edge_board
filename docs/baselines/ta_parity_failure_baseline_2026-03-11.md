# TA parity failure baseline (captured 2026-03-11)

## Command 1

```bash
python3 scripts/check_ta_parity.py
```

Exit code: `1`

```text
{"status": "fail", "reason_code": "input_missing", "path": "tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body"}
```

## Command 2

```bash
PYTHONPATH=scripts python3 - <<'PY'
from pathlib import Path
from extract_player_features import _parse_matchmx_rows
p = Path("tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body")
payload = p.read_text(encoding="utf-8", errors="ignore")
rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
ok = [r for r in rows if r.reason_code == "ok"]
print("total_rows", len(rows), "ok_rows", len(ok))
print("sample_reason_counts")
from collections import Counter
print(Counter(r.reason_code for r in rows).most_common(5))
PY
```

Exit code: `1`

```text
Traceback (most recent call last):
  File "<stdin>", line 4, in <module>
  File "/root/.pyenv/versions/3.10.19/lib/python3.10/pathlib.py", line 1134, in read_text
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
  File "/root/.pyenv/versions/3.10.19/lib/python3.10/pathlib.py", line 1119, in open
    return self._accessor.open(self, mode, buffering, encoding, errors,
FileNotFoundError: [Errno 2] No such file or directory: 'tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body'
```
