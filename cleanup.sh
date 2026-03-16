#!/usr/bin/env bash
set -e
echo "==> Velocity Showdown — Cleanup"
echo "This will stop all containers and DELETE all stored data."
read -rp "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

# Stop containers, remove volumes (ClickHouse data) and networks
docker compose down --volumes --remove-orphans

# Remove locally-built images
docker compose down --rmi local 2>/dev/null || true

# Optional: remove pulled base images
read -rp "Also remove downloaded images (Kafka, ClickHouse, Kafka-UI)? [y/N] " pull_confirm
if [[ "$pull_confirm" =~ ^[Yy]$ ]]; then
    docker rmi apache/kafka:4.1.0 clickhouse/clickhouse-server:26.2 \
                provectuslabs/kafka-ui:latest 2>/dev/null || true
fi

docker image prune -f
echo ""
echo "==> Done. Run ./quickstart.sh to start fresh."
