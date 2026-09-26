# PR Summary: Simulation Workspace and Experiment APIs

## Scope

- Added simulation service modules, contracts, and fixtures
- Added simulation projection support in state-builder
- Added read-only experiment API routes and websocket stream
- Added web experiment workspace with live/experiments toggle
- Added multi-run comparison wiring from API results

## Verification

- Web tests: `npm --prefix web test`
- Web typecheck: `npm --prefix web run typecheck`
- Web lint: `npm --prefix web run lint`
- Targeted simulation/api tests run during implementation

## Public-repo safety checks

- Pattern scan for private keys/tokens: no matches found
- `npm --prefix web audit --omit=dev`: 0 vulnerabilities
- `pip_audit` unavailable in current dev environment (not installed)

## Risks to review in PR

1. Large commit scope across backend + frontend + tests
2. New simulation API shape and optional fields in web contracts
3. Runtime fan-out cost from per-run `/results` fetches on snapshot load
4. Need production smoke after manual deploy (api/web/state-builder)

## Post-merge/manual deploy checklist

- Pull target branch on server
- `docker compose up -d --build web api state-builder density-builder ingestor`
- Check readiness and logs
- Validate UI can switch to **Experiments** and shows comparison metrics
