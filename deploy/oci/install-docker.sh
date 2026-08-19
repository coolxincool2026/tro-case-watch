#!/usr/bin/env bash
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This script currently supports Ubuntu or Debian images with apt-get."
  exit 1
fi

sudo apt-get update
sudo apt-get install -y ca-certificates curl xz-utils sqlite3

if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
fi

sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
sudo install -d -m 0750 -o "$USER" -g "$USER" /var/lib/tro-case-watch/data /var/lib/tro-case-watch/recovery

echo "Docker installed. Log out and back in once so the docker group takes effect."
