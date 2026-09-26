# Trace Preparation Runbook

This runbook prepares deterministic trace artifacts for simulation experiments.

## Inputs

- Source trace file in official schema (Azure Functions or Google Borg)
- Expected file SHA-256
- Time window (`--start`, `--end`)
- Sampling scale and deterministic seed

## Command

```bash
uv run aether-sim prepare \
  --source azure-functions \
  --input tests/fixtures/traces/azure_functions_official_schema.csv \
  --expected-sha256 <sha256> \
  --partition eu-small \
  --start 2026-09-25T09:00:00Z \
  --end 2026-09-25T10:00:00Z \
  --scale 1.0 \
  --seed 7 \
  --output ./artifacts/prepared/azure
```

## Validation checklist

- Manifest exists and includes checksum entries
- Parquet artifact is present
- Re-running with same inputs produces equivalent summary outputs
