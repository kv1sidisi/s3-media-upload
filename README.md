# s3-media-upload

`s3-media-upload` is a small Go foundation for a direct-to-S3 media upload service.
The current slice provides a loopback HTTP process, PostgreSQL schema v1, idempotent
upload creation, 15-minute direct PUT capabilities, authoritative pending reads, local
Garage configuration, durable asynchronous completion intent, authoritative status
reads, verified JPEG/PNG publication, ready-only direct content reads, health probes,
structured logs, and process counters.

## Status

### Implemented

- strict loopback-only configuration;
- forward-only PostgreSQL schema v1;
- `POST /uploads` with durable idempotency and presigned direct PUT instructions;
- `POST /uploads/{upload_id}/complete` with restart-safe `finalizing` intent;
- a sequential fenced finalizer with bounded validation, candidate publication, and
  full-GET verification;
- immutable `ready`/`rejected` outcomes and `GET /uploads/{upload_id}/content`;
- `GET /uploads/{upload_id}` with an authoritative PostgreSQL representation;
- `GET /livez`, `GET /readyz`, and `GET /debug/vars`;
- bounded PostgreSQL and signed S3 readiness checks;
- JSON logs and a fixed `media_upload_service` expvar subtree;
- pinned local PostgreSQL, Garage, and Go test containers.

### Planned

- time-driven missing expiry and physical object cleanup.

### Observed

On 2026-08-31, the following passed from a local, uncommitted working tree:

- pinned Go 1.26.7, an empty `gofmt -l` result, `go vet ./...`, and the canonical quick gate;
- healthy pinned PostgreSQL 18.6 and Garage 2.3.0 with schema v1 present;
- the complete root-package test binary, built under a hard no-swap 2560 MiB limit and
  executed against PostgreSQL and Garage under a hard no-swap 256 MiB limit
  (`GOMEMLIMIT=230MiB`), with shuffle seed `1788200551998977133` and result `PASS`;
- the complete race suite under a hard no-swap 2560 MiB limit, with zero race reports
  and result `ok` in 17.811 seconds.

This is local ticket-04 working-tree evidence, not evidence for an immutable revision.
A manual demo, live AWS, and deployment have not been observed.

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
write window is open. A successful storage PUT still leaves the resource `pending`.

After the PUT completes, durably submit asynchronous completion intent with an empty
body:

```sh
curl --fail-with-body \
  --request POST \
  --header 'Content-Length: 0' \
  "http://127.0.0.1:8080/uploads/${UPLOAD_ID:?set from the create response}/complete"
```

A timely request returns `202` and `finalizing`; an exact replay also returns `202` and
wakes recovery without changing the original completion time. The completion handler
does not inspect S3 or validate bytes. The sequential finalizer later captures one
bounded staging snapshot, validates it, tracks a content-addressed candidate before
writing, and performs a full length-and-SHA-256 GET verification before committing
`ready`. Invalid input becomes an immutable safe `rejected` outcome. A request at or
after the upload deadline atomically expires the upload and records staging cleanup
work. Claims are fenced leases, not exactly-once execution: after a crash or ambiguous
PUT, a fresh worker verifies every tracked candidate before reading staging again.

`GET /uploads/{upload_id}` reads the current state from PostgreSQL and never returns a
storage URL, bucket, object key, ETag, digest, completion timestamp, claim, or retry
metadata. Ready representations contain only decoder-derived image metadata.
`GET /uploads/{upload_id}/content` returns a five-minute `307` capability only for
`ready`; the API never accepts or proxies image bytes.

## Configuration

| Variable | Required | Contract |
| --- | --- | --- |
| `HTTP_ADDR` | No | Defaults to `127.0.0.1:8080`; a literal loopback IP and port are required. |
| `DATABASE_URL` | Yes | PostgreSQL DSN; never logged. |
| `S3_BUCKET` | Yes | Dedicated private bucket name. |
| `AWS_REGION` | Yes | AWS signing region. |
| `S3_ENDPOINT` | No | Origin-only loopback HTTP URL for local Garage. |
| `FINALIZE_CLAIM_LEASE` | No | Defaults to `30s`; minimum `10.000001s` so it exceeds the fixed 10-second S3 deadline at PostgreSQL precision. |

AWS credentials use the standard AWS SDK chain. `.env.example` contains disposable
local values only; `.env` is ignored.

## Verification

Run the host checks from the module root. Then start the dependencies, migrate schema
v1 as shown above, and run the split integration gate:

```sh
test -z "$(gofmt -l *.go)"
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go test -short -count=1 -shuffle=on -timeout=30s ./...

mkdir .test-bin
TEST_MEMORY_LIMIT=2560m docker compose run --rm -T \
  --volume "$PWD/.test-bin:/out" \
  --env GOMEMLIMIT=2GiB \
  test sh -ec 'GOTOOLCHAIN=local go test -c -o /out/service.test .'
TEST_MEMORY_LIMIT=256m docker compose run --rm -T \
  --volume "$PWD/.test-bin:/out:ro" \
  --env GOMEMLIMIT=230MiB \
  test sh -ec '/out/service.test -test.count=1 -test.shuffle=on -test.timeout=3m'
rm -f .test-bin/service.test
rmdir .test-bin

TEST_MEMORY_LIMIT=2560m docker compose run --rm -T \
  --env GOMEMLIMIT=2GiB \
  test sh -ec \
  'GOTOOLCHAIN=local go test -race -count=1 -shuffle=on -timeout=10m ./...'
```

Cold compilation did not fit the 256 MiB execution envelope, including serialized and
reduced-compiler-memory attempts. Compilation therefore has its own 2560 MiB envelope;
the resulting binary still executes the complete integration suite under 256 MiB. The
race suite executes separately under the 2560 MiB envelope.

## Security boundary and limitations

This service has no authentication, TLS, rate limiting, quota enforcement, or public
upload controls. Do not expose it to a LAN or the Internet. The bucket must remain
private, and credentials, DSNs, presigned URLs, request data, and object identifiers
must not enter logs. This repository does not claim deployment, live AWS verification,
broad S3 compatibility, or production readiness. Time-driven missing expiry and
physical tombstone deletion remain separate planned slices.
