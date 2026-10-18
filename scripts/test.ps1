$ErrorActionPreference = 'Stop'
docker compose exec backend python -m pytest -q
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose exec backend python -m app.verify
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose exec frontend nginx -t
