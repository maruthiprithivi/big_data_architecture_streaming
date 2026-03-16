@echo off
echo ==> Velocity Showdown — Quick Start
docker compose up --build -d
echo.
echo Services starting... (Kafka takes ~30 s to become healthy)
echo.
echo   Dashboard  -^>  http://localhost:3001
echo   Kafka UI   -^>  http://localhost:8080
echo   ClickHouse -^>  http://localhost:8123/play
echo   Collector  -^>  http://localhost:8000/docs
echo.
echo Open the dashboard and click Start Collection when the broker shows connected
