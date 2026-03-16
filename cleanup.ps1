$ErrorActionPreference = "Stop"
Write-Host "==> Velocity Showdown — Cleanup"
Write-Host "This will stop all containers and DELETE all stored data."
$confirm = Read-Host "Continue? [y/N]"
if ($confirm -ne "y" -and $confirm -ne "Y") {
    Write-Host "Aborted."
    exit 0
}

# Stop containers, remove volumes (ClickHouse data) and networks
docker compose down --volumes --remove-orphans

# Remove locally-built images
docker compose down --rmi local 2>$null

# Optional: remove pulled base images
$pullConfirm = Read-Host "Also remove downloaded images (Kafka, ClickHouse, Kafka-UI)? [y/N]"
if ($pullConfirm -eq "y" -or $pullConfirm -eq "Y") {
    docker rmi apache/kafka:4.1.0 clickhouse/clickhouse-server:26.2 provectuslabs/kafka-ui:latest 2>$null
}

docker image prune -f
Write-Host ""
Write-Host "==> Done. Run .\quickstart.ps1 to start fresh."
