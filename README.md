# wta_edge_board

## Secrets rotation and safe usage

### 1) Rotate/revoke The Odds API key
1. Sign in to The Odds API provider dashboard.
2. Revoke the currently active key used by this project.
3. Generate a replacement key.
4. Record rotation time and owner in your team runbook.

> Note: key rotation must be performed in the provider dashboard; it cannot be automated from this Apps Script repo.

### 2) Update local/runtime and CI/CD secrets
After rotating, update every location that stores `ODDS_API_KEY`:

- Google Sheet `Config` tab (`ODDS_API_KEY` row).
- Google Apps Script project properties (if you mirror secrets there).
- CI/CD provider secret variables (for example GitHub Actions secrets like `ODDS_API_KEY` / `THE_ODDS_API_KEY`).

Recommended process:
- Update secrets first.
- Run one manual pipeline execution.
- Verify credits and fetch success.
- Delete any temporary plaintext notes.

### 3) Logging safety
This project includes log sanitization helpers that:
- redact query-string secrets such as `apiKey=...`,
- redact common token/header patterns,
- redact known webhook URL formats,
- redact object fields with sensitive names (`apiKey`, `secret`, `token`, `password`, etc.).

### 4) CLI safety guidelines
Avoid verbose CLI options that can print full URLs containing secrets.

- **Do not use:** `curl -v "https://.../odds?apiKey=REAL_KEY&..."`
- **Prefer:**
  - environment variable injection without echoing the value,
  - masked/filtered logs,
  - command history controls where appropriate.

Example safer pattern:

```bash
curl -sS "https://api.the-odds-api.com/v4/sports" \
  --get \
  --data-urlencode "apiKey=${ODDS_API_KEY}" \
  --data-urlencode "all=true"
```

If you must debug requests, sanitize output before sharing logs.

## Logging verbosity controls

Use the `Config` tab to tune logging detail:

- `VERBOSE_LOGGING` keeps compatibility with existing on/off behavior.
- `LOG_VERBOSITY_LEVEL` provides granular control (`0` to `3`):
  - `0`: minimal logging
  - `1`: stage summaries only
  - `2`: stage summaries + window/coverage diagnostics (recommended for optimization)
  - `3`: includes sampled payload-level diagnostics for deeper debugging

All logs continue to pass through secret redaction before being persisted.
