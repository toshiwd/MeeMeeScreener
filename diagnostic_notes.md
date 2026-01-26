# TXT Update Status Check

## Current Status

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:28888/api/txt_update/status" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

## Key Findings

Based on the API response:
- **Phase**: done
- **Error**: null (no error reported)
- **Started**: 2026-01-19T19:43:21
- **Finished**: 2026-01-19T19:43:59
- **Processed**: 682 files
- **Duration**: ~38 seconds

## The Mystery

The TXT update shows as "done" with no errors, but the database was NOT updated automatically. This suggests that:

1. **The ingest step was skipped or failed silently**
2. **The error was not captured in the status**

## Looking at the Code

In `main.py` line 4954-4966, after the VBS script completes, it should run:
```python
_set_update_status(phase="ingesting")
ingest_code, ingest_output = _run_ingest_command()
```

But the status shows `phase="done"`, which means it reached line 4974. This means:
- Either the ingest was skipped
- Or the ingest ran but failed to update the database
- Or there's a race condition

## Hypothesis

Looking at the `_run_ingest_command()` function (line 5018-5042), it tries to:
1. Run `ingest_txt.py` as a subprocess if it exists
2. Otherwise, import and run `ingest()` directly

The issue might be:
- **Environment variables not set** - The subprocess might not have `PAN_OUT_TXT_DIR` set correctly
- **Database locked** - The database might have been locked by another process
- **Silent failure** - The ingest might have failed but returned code 0

## Next Steps

We need to add better logging to see what's happening during the ingest phase.
