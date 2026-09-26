# Acceptance Experiment Runbook

Use this flow for one-hour deterministic acceptance comparisons.

## Preconditions

- Prepared Azure artifact directory
- Prepared Borg artifact directory
- Experiment config file (`configs/experiments/eu-small-acceptance-v1.yaml`)

## Compare command

```bash
uv run aether-sim compare \
  --config configs/experiments/eu-small-acceptance-v1.yaml \
  --azure-artifact tests/fixtures/prepared/azure \
  --borg-artifact tests/fixtures/prepared/borg \
  --output ./artifacts/comparisons/eu-small
```

## Expected outputs

- One immutable comparison bundle directory
- Per-run summaries with component metrics and combined score
- Configuration and checksum manifests

## Post-run API validation

Verify the API exposes experiment projection data:

```bash
curl -s http://localhost:8000/api/v1/experiments | jq .
curl -s http://localhost:8000/api/v1/experiments/<run_id> | jq .
curl -s http://localhost:8000/api/v1/experiments/<run_id>/results | jq .
```
