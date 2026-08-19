# Oracle Cloud Always Free deployment

Use an Always Free `VM.Standard.A1.Flex` instance with Ubuntu 24.04, 2 OCPUs,
12 GB RAM, and a 100 GB boot volume. Create it in the account's home region.

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
database and `.env` before running `deploy.sh`.

Do not reuse credentials or SSH keys from a server that has changed host keys
or rejected previously valid administrator access. Rotate third-party tokens
before enabling their integrations.
