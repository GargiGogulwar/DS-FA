$ErrorActionPreference = 'Stop'
docker compose stop node-c
try {
    docker compose exec backend python -m app.faults node-down
    if ($LASTEXITCODE -ne 0) { throw 'Node failure verification failed' }
} finally { docker compose start node-c }
docker compose exec backend python -m app.faults node-up
if ($LASTEXITCODE -ne 0) { throw 'Node recovery verification failed' }
docker compose stop rabbitmq
try {
    docker compose exec backend python -m app.faults outage-write
    if ($LASTEXITCODE -ne 0) { throw 'Outbox persistence verification failed' }
} finally { docker compose start rabbitmq }
docker compose exec backend python -m app.faults reconcile
if ($LASTEXITCODE -ne 0) { throw 'Outbox reconciliation verification failed' }
