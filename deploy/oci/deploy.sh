#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="${APP_DATA_DIR:-/var/lib/tro-case-watch/data}"

cd "$ROOT_DIR"

[[ -f .env ]] || { echo "Missing $ROOT_DIR/.env" >&2; exit 1; }
[[ -f "$DATA_DIR/tro-watch.sqlite" ]] || { echo "Missing $DATA_DIR/tro-watch.sqlite" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Docker is required" >&2; exit 1; }
docker compose version >/dev/null

case "$(uname -m)" in
  aarch64|arm64|x86_64|amd64) ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

mkdir -p "$DATA_DIR"

docker compose -f deploy/oci/compose.yml up -d --build
docker compose -f deploy/oci/compose.yml ps

for attempt in $(seq 1 24); do
  if docker compose -f deploy/oci/compose.yml exec -T app node -e \
    "fetch('http://127.0.0.1:4127/api/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"; then
    echo "Deployment is healthy"
    exit 0
  fi
  sleep 5
done

docker compose -f deploy/oci/compose.yml logs --tail=150 app
echo "Deployment started but health verification failed" >&2
exit 1
