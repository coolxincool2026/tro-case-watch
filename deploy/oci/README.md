# Ubuntu VPS deployment

Use a clean Ubuntu 24.04 server with at least 4 GB RAM, 2 CPU cores, and an
80 GB SSD. The deployment works on x86_64 and ARM64 hosts.

Network ingress should allow:

- TCP 22 only from the administrator's current IP when possible.
- TCP 80 and 443 from the internet.
- No public access to port 4127 or 4128.

The Compose deployment separates the public web process from the scheduler:

- `app` serves login, search, case detail, and webhook requests without running
  internal schedules.
- `worker` runs recent and bounded backfill schedules with lower CPU priority.
- Docker's init process reaps child processes, and both services have memory,
  CPU, PID, and log-size limits.

Production data is mounted from `/var/lib/tro-case-watch/data`. Run
`install-docker.sh` once on a new Ubuntu instance, then restore the verified
database and `.env` before running `deploy.sh`. Run `harden-host.sh` after
confirming SSH public-key access; it disables password login, enables UFW,
fail2ban, and unattended security upgrades.

Do not reuse credentials or SSH keys from a server that has changed host keys
or rejected previously valid administrator access. Rotate third-party tokens
before enabling their integrations.
