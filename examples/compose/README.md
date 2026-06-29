# Docker Compose example

A minimal Headwaters stack — Postgres plus the service — wired with healthchecks
so the service only starts once Postgres is ready.

```sh
docker compose up
curl localhost:8091/health   # -> OK
```

The service image is distroless (no shell), so both the image's built-in
`HEALTHCHECK` and the `healthcheck:` block in `docker-compose.yml` run the
binary's own `healthcheck` subcommand rather than shelling out to `curl`/`wget`:

```yaml
healthcheck:
  test: ["CMD", "/usr/local/bin/app", "healthcheck"]
```

`docker compose ps` shows the `headwaters` service reach `healthy` once `/health`
responds. To build the image from this checkout instead of pulling a tag, replace
the `image:` line with `build: { context: ../.. }`.

Configure the service via environment (see the crate README for the full set):
`DATABASE_URL`, `HEADWATERS__PORT`, `HEADWATERS__HOST`, `RUST_LOG`, …
