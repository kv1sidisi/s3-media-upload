# s3-media-upload

`s3-media-upload` is a small Go foundation for a direct-to-S3 media upload service.
The current slice provides a loopback HTTP process, PostgreSQL schema v1, idempotent
upload creation, 15-minute direct PUT capabilities, authoritative pending reads, local
Garage configuration, health probes, structured logs, and process counters. Completion
and file processing are planned but are not implemented yet.

## Status

### Implemented

- strict loopback-only configuration;
- forward-only PostgreSQL schema v1;
- `POST /uploads` with durable idempotency and presigned direct PUT instructions;
- `GET /uploads/{upload_id}` with an authoritative PostgreSQL representation;
- `GET /livez`, `GET /readyz`, and `GET /debug/vars`;
- bounded PostgreSQL and signed S3 readiness checks;
- JSON logs and a fixed `media_upload_service` expvar subtree;
- pinned local PostgreSQL, Garage, and Go test containers.

### Planned

- durable completion, validation, publication, expiry, and object cleanup.

### Observed

On 2026-08-31, the following passed from a local, uncommitted working tree:

- Go 1.26.7, an empty `gofmt -l` result, `go vet ./...`, and the canonical quick gate;
- fresh pinned PostgreSQL 18.6 and Garage 2.3.0 startup plus transactional schema v1 migration;
- the complete root-package test binary, built under a hard no-swap 2560 MiB limit and
  executed against PostgreSQL and Garage under a hard no-swap 256 MiB limit
  (`GOMEMLIMIT=230MiB`), with shuffle seed `1788187538343055235` and result `PASS`.

This is local ticket-02 evidence, not evidence from an immutable revision. No race gate,
manual demo, live AWS run, deployment, or completed/ready upload flow has been observed.

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

Create an upload resource with declarations only:

```sh
curl --fail-with-body \
  --request POST http://127.0.0.1:8080/uploads \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: ${IDEMPOTENCY_KEY:?set to a canonical lowercase UUIDv4}" \
  --data '{"size_bytes":123456,"content_type":"image/jpeg"}'
```

The response contains an opaque `upload_request`. Send the image directly to its URL
with the returned method and complete headers map, without following redirects. Exact
create replays return the same upload resource and a newly authorized URL while the
write window is open. A successful storage PUT still leaves the resource `pending`;
`GET /uploads/{upload_id}` reads that state from PostgreSQL and never returns a storage
URL, bucket, object key, ETag, digest, claim, or retry metadata. The API never accepts
or proxies image bytes.

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
must not enter logs. This repository does not claim a completed or ready upload flow,
deployment, live AWS verification, broad S3 compatibility, or production readiness.
