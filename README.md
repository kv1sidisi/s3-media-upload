# s3-media-upload

`s3-media-upload` is a small Go foundation for a direct-to-S3 media upload service.
The current slice provides a loopback HTTP process, PostgreSQL schema v1, local Garage
configuration, health probes, structured logs, and process counters. Upload creation
and file processing are planned but are not implemented yet.

## Status

### Implemented

- strict loopback-only configuration;
- forward-only PostgreSQL schema v1;
- `GET /livez`, `GET /readyz`, and `GET /debug/vars`;
- bounded PostgreSQL and signed S3 readiness checks;
- JSON logs and a fixed `media_upload_service` expvar subtree;
- pinned local PostgreSQL, Garage, and Go test containers.

### Planned

- idempotent upload creation and presigned direct PUT;
- durable completion, validation, publication, expiry, and object cleanup.

### Observed

On 2026-08-31, the following passed from a local, uncommitted working tree:

- Go 1.26.7, an empty `gofmt -l` result, `go vet ./...`, and the canonical quick gate;
- fresh pinned PostgreSQL 18.6 and Garage 2.3.0 startup plus transactional schema v1 migration;
- the complete root-package test binary, built under a hard no-swap 2560 MiB limit and
  executed against PostgreSQL and Garage under a hard no-swap 256 MiB limit
  (`GOMEMLIMIT=230MiB`), with shuffle seed `1788177935590793139` and result `PASS`.

This is local ticket-01 evidence, not evidence from an immutable revision. No race gate,
manual upload demo, live AWS run, deployment, or working upload flow has been observed.

## Prerequisites

- Go 1.26.7;
- Docker with Compose 2.17.0 or newer.

The supported runtime is a trusted-host local demo. The HTTP service binds to loopback,
and Compose publishes PostgreSQL and Garage host ports only on `127.0.0.1`.

## Run locally

```sh
cp .env.example .env
docker compose config --quiet
docker compose up --wait --wait-timeout 60 postgres garage
docker compose exec -T postgres \
  psql -X --set=ON_ERROR_STOP=1 --single-transaction \
  --username=media_upload --dbname=media_upload --file=- \
  < migrations/0001_initial.sql
set -a
. ./.env
set +a
GOTOOLCHAIN=local go run .
```

In another terminal:

```sh
curl --fail-with-body http://127.0.0.1:8080/livez
curl --fail-with-body http://127.0.0.1:8080/readyz
curl --fail-with-body http://127.0.0.1:8080/debug/vars
```

`/livez` does not call dependencies. `/readyz` checks PostgreSQL and S3 sequentially
with a two-second timeout per dependency. `/debug/vars` exposes standard Go expvar data
and the service subtree without calling dependencies.

## Configuration

| Variable | Required | Contract |
| --- | --- | --- |
| `HTTP_ADDR` | No | Defaults to `127.0.0.1:8080`; a literal loopback IP and port are required. |
| `DATABASE_URL` | Yes | PostgreSQL DSN; never logged. |
| `S3_BUCKET` | Yes | Dedicated private bucket name. |
| `AWS_REGION` | Yes | AWS signing region. |
| `S3_ENDPOINT` | No | Origin-only loopback HTTP URL for local Garage. |
| `FINALIZE_CLAIM_LEASE` | No | Defaults to `30s` and must exceed the fixed 10-second S3 deadline. |

AWS credentials use the standard AWS SDK chain. `.env.example` contains disposable
local values only; `.env` is ignored.

## Verification

Run the host checks from the module root. Then start the dependencies, migrate schema
v1 as shown above, and run the split integration gate:

```sh
test -z "$(gofmt -l *.go)"
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go test -short -count=1 -shuffle=on -timeout=30s ./...

mkdir .ticket01-bin
TEST_MEMORY_LIMIT=2560m docker compose run --rm -T \
  --volume "$PWD/.ticket01-bin:/out" \
  --env GOMEMLIMIT=2GiB \
  test sh -ec 'GOTOOLCHAIN=local go test -c -o /out/service.test .'
TEST_MEMORY_LIMIT=256m docker compose run --rm -T \
  --volume "$PWD/.ticket01-bin:/out:ro" \
  --env GOMEMLIMIT=230MiB \
  test sh -ec '/out/service.test -test.count=1 -test.shuffle=on -test.timeout=3m'
rm -f .ticket01-bin/service.test
rmdir .ticket01-bin
```

Cold compilation did not fit the 256 MiB execution envelope, including serialized and
reduced-compiler-memory attempts. Compilation therefore has its own 2560 MiB envelope;
the resulting binary still executes the complete integration suite under 256 MiB. The
race gate is reserved for later concurrency slices.

## Security boundary and limitations

This service has no authentication, TLS, rate limiting, quota enforcement, or public
upload controls. Do not expose it to a LAN or the Internet. The bucket must remain
private, and credentials, DSNs, presigned URLs, request data, and object identifiers
must not enter logs. This repository does not claim a working upload flow, deployment,
live AWS verification, broad S3 compatibility, or production readiness.
