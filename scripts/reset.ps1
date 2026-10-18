$ErrorActionPreference = 'Stop'
docker compose stop backend worker
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose run --rm migrate python -m app.reset
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose start backend worker
