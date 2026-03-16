@echo off
echo ==> Velocity Showdown — Cleanup
echo This will stop all containers and DELETE all stored data.
set /p confirm="Continue? [y/N] "
if /i not "%confirm%"=="y" (
    echo Aborted.
    exit /b 0
)

:: Stop containers, remove volumes (ClickHouse data) and networks
docker compose down --volumes --remove-orphans

:: Remove locally-built images
docker compose down --rmi local 2>nul

:: Optional: remove pulled base images
set /p pull_confirm="Also remove downloaded images (Kafka, ClickHouse, Kafka-UI)? [y/N] "
if /i "%pull_confirm%"=="y" (
    docker rmi apache/kafka:4.1.0 clickhouse/clickhouse-server:26.2 provectuslabs/kafka-ui:latest 2>nul
)

docker image prune -f
echo.
echo ==> Done. Run quickstart.bat to start fresh.
