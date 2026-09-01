# s3-media-upload

`s3-media-upload` is a small Go service for direct image uploads to S3-compatible
storage. The API stores lifecycle intent in PostgreSQL, sends image bytes directly
between the client and storage, verifies an immutable candidate before publication,
and retains permanent cleanup duties for staging and losing objects.

The supported profile is a trusted-host, loopback-only local demo. It is not a public
upload service or a production deployment.

## Status

| Label | Current truth |
| --- | --- |
| Implemented | Clean revision `6fdc8b7a051144639ff78390c80bb62fe8132131` contains the complete local service, canonical `U1-U5`, `P1-P6`, `S1`, and `E1-E6` tests, Compose environment, and CI workflow. |
| Planned | Push, remote CI, deployment, and live AWS remain separate operations. |
| Observed | On 2026-09-01, the exact local quick/full/race gates and a one-second loopback demo passed against clean revision `6fdc8b7a051144639ff78390c80bb62fe8132131`; the bounded claim is recorded below. |

## Data flow

1. `POST /uploads` commits an upload identity and returns a 15-minute presigned
   single-part `PUT` capability.
2. The client sends the declared JPEG or PNG directly to storage; the API never
   receives image bytes.
3. `POST /uploads/{upload_id}/complete` commits asynchronous finalization intent.
4. One sequential finalizer captures and validates the staging bytes, tracks a
   content-addressed candidate before writing it, then verifies a complete candidate
   `GET` before committing `ready`.
5. `GET /uploads/{upload_id}/content` returns a five-minute direct-read redirect only
   for `ready` uploads.
6. One maintenance loop expires unfinished uploads and repeatedly deletes only
   DB-recorded staging or unselected candidate objects.

PostgreSQL is authoritative for identity, lifecycle, retry intent, the selected key,
and cleanup duties. Storage is authoritative only for bytes. No S3 call runs inside a
PostgreSQL transaction.

## HTTP API

The application surface is unversioned and has exactly four endpoints:

| Method | Path | Result |
| --- | --- | --- |
| `POST` | `/uploads` | `201` for creation, `200` for an exact replay, `422` if the idempotency key is reused with different declarations. |
| `POST` | `/uploads/{upload_id}/complete` | `202` while finalizing; terminal replays return the authoritative terminal representation. |
| `GET` | `/uploads/{upload_id}` | Current `pending`, `finalizing`, `ready`, `rejected`, or `expired` state without a storage capability. |
| `GET` | `/uploads/{upload_id}/content` | Empty `307` direct-read redirect only for `ready`; state-specific errors otherwise. |

Operational endpoints are `GET /livez`, `GET /readyz`, and `GET /debug/vars`.
`/livez` does not call dependencies. `/readyz` checks PostgreSQL and signed S3 access
sequentially with a two-second timeout per dependency. `/debug/vars` exposes the fixed
`media_upload_service` expvar subtree without dependency calls.

Requests and responses use JSON except for the empty `307`. Handler responses include
`Cache-Control: no-store`; content redirects use `private, no-store` and `nosniff`.
The raw request line plus headers is limited to 16,384 bytes, JSON bodies are limited
to 16,384 bytes, and bodyless endpoints require an empty body.

## Version contract and prerequisites

| Component | Supported value |
| --- | --- |
| Go | Exact `1.26.7` with `GOTOOLCHAIN=local`. |
| Docker Compose | Plugin command `docker compose`, version `2.17.0` or newer. |
| PostgreSQL | `postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. |
| Garage | `dxflrs/garage:v2.3.0@sha256:866bd13ed2038ba7e7190e840482bc27234c4afaf77be8cfa439ae088c1e4690`, single-node consistent mode. |
| Full/race Go image | `golang:1.26.7-bookworm@sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514`. |

Compose publishes PostgreSQL on `127.0.0.1:55432` and Garage S3 on
`127.0.0.1:3900`. Garage RPC, web, and admin ports are not published.

## Configuration

| Variable | Required | Contract |
| --- | --- | --- |
| `HTTP_ADDR` | No | Defaults to `127.0.0.1:8080`; a literal loopback IP and port are required. |
| `DATABASE_URL` | Yes | PostgreSQL DSN; never logged. |
| `S3_BUCKET` | Yes | Dedicated private bucket. |
| `AWS_REGION` | Yes | AWS signing region. |
| `S3_ENDPOINT` | No | Origin-only loopback HTTP URL for local Garage; omitted for standard AWS HTTPS resolution. |
| `FINALIZE_CLAIM_LEASE` | No | Defaults to `30s` and must exceed the fixed ten-second S3 operation deadline. |

AWS credentials use the standard AWS SDK chain. `.env.example` contains disposable
loopback values only; `.env` is ignored. Never reuse those values for remote storage.

## Fresh bootstrap

From a fresh checkout:

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

The migration is intentionally external and applies schema ledger version `1`. If old
local named volumes must be discarded before a fresh bootstrap, stop the stack and run
`docker compose down --volumes`; this permanently deletes the local PostgreSQL and
Garage data.

## Live demo

Start the timer only after the dependencies and service are ready. The following flow
must finish within eight minutes. Do not capture raw response bodies or redirect
headers because they contain bearer capabilities.

In a second terminal, record one valid PNG and a fresh canonical lowercase UUIDv4.
The input line is: path without spaces, SHA-256, byte size, width, height, and key.

```sh
API=http://127.0.0.1:8080
read -r DEMO_IMAGE DEMO_SHA256 DEMO_SIZE DEMO_WIDTH DEMO_HEIGHT IDEMPOTENCY_KEY
curl --include "$API/livez"
curl --include "$API/readyz"
curl --silent --show-error "$API/debug/vars"
```

Create the upload. Record the `upload_id` and opaque upload instructions without
copying the URL into evidence:

```sh
curl --include --request POST "$API/uploads" \
  --header "Idempotency-Key: $IDEMPOTENCY_KEY" \
  --header 'Content-Type: application/json' \
  --data "{\"size_bytes\":$DEMO_SIZE,\"content_type\":\"image/png\"}"
```

Repeat that exact command. The replay must return `200` and the same `upload_id`; it
must not create a second `none_to_pending` counter delta.

Enter the replay result through `read` so the bearer URL is not placed in shell
history, then perform the direct storage write:

```sh
read -r UPLOAD_ID PUT_URL
curl --include --request PUT \
  --header 'Content-Type: image/png' \
  --data-binary @"$DEMO_IMAGE" \
  "$PUT_URL"
unset PUT_URL
```

A native storage `2xx` proves only transport success. The service deliberately emits
no `put_succeeded` event because this data path bypasses it.

Commit completion intent, then repeat the status request until it returns `ready`
within the time box:

```sh
curl --include --request POST "$API/uploads/$UPLOAD_ID/complete"
curl --include "$API/uploads/$UPLOAD_ID"
curl --silent --show-error "$API/debug/vars"
```

The completion response must be `202 finalizing`. The final status must contain
decoder-derived size, MIME type, width, and height, but no URL, key, digest, claim, or
retry metadata. Safe logs should show create, capability, completion, finalize-phase,
and terminal events for the same `upload_id`.

Finally, observe the ready-only redirect, download through a fresh redirect, and
compare exact bytes:

```sh
curl --include "$API/uploads/$UPLOAD_ID/content"
DOWNLOADED_IMAGE=/tmp/media-upload-demo-result.png
curl --fail --location --output "$DOWNLOADED_IMAGE" \
  "$API/uploads/$UPLOAD_ID/content"
cmp "$DEMO_IMAGE" "$DOWNLOADED_IMAGE"
```

Success is `/livez=200`, `/readyz=200`, create `201`, exact replay `200` with the same
ID, direct PUT `2xx`, completion `202`, terminal `ready`, content `307`, and `cmp=0`.

## Verification

Run host-only checks from the module root:

```sh
docker compose config --quiet
GOTOOLCHAIN=local go mod tidy -diff
test -z "$(gofmt -l .)"
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go test -short -count=1 -shuffle=on -timeout=30s ./...
```

For full and race gates, start fresh dependencies and apply the migration as shown in
the bootstrap section. The full logical gate separates cold compilation from execution
without weakening the 256 MiB execution boundary:

```sh
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

TEST_MEMORY_LIMIT=2560m docker compose run --rm -T test \
  sh -ec 'GOTOOLCHAIN=local go test -race -count=1 -shuffle=on -timeout=10m ./...'
```

Full and race must execute integration, concurrency, ambiguity, cleanup, and shutdown
paths without a skip, timeout, hang, or race report. In particular, the full suite
contains `TestParentFirstCandidateTerminalRace` and
`TestE2EFinalizeRecovery/delegate_then_fail`; no separate `-run` or `-v` command is
used as substitute evidence. GitHub Actions runs the same quick/full/race gates and
deletes Compose volumes only on its ephemeral runner.

## Evidence

The demo result was recorded at `2026-09-01T10:10:20Z` on clean revision
`6fdc8b7a051144639ff78390c80bb62fe8132131`; the tree was clean before and after the
combined quick/full/race batch and again after demo shutdown, log audit, and cleanup.
The local runner was macOS `26.4.1` on `arm64`, with Go `1.26.7`, Docker client
`29.6.1`, Docker server `29.5.2`, and Docker Compose `5.1.2`. PostgreSQL was `18.6`;
Garage was `2.3.0` in `consistent` mode; the migration ledger was `1|1`. Compose used
the pinned PostgreSQL, Garage, and
`golang:1.26.7-bookworm@sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514`
image digests listed above.

| Evidence | Command or exact input | Result | Supported claim |
| --- | --- | --- | --- |
| Quick/full/race | The verification commands above, unchanged. Full used a 2560 MiB build with `GOMEMLIMIT=2GiB`, then a 256 MiB execution with `GOMEMLIMIT=230MiB`; race used 2560 MiB. | Quick: `ok` in `0.997s`. Full: `PASS`, seed `1788256824094762465`, 31 seconds including cold build. Race: `ok` in `18.203s`, 54 seconds including cold race build. Non-short PostgreSQL/Garage tests ran without skip, timeout, hang, or race report; the suite contains `TestParentFirstCandidateTerminalRace` and `TestE2EFinalizeRecovery/delegate_then_fail`. | The canonical local quick/full/race suite passed under the recorded version and memory boundaries. |
| Live demo | A sanitized non-interactive execution of the documented endpoint sequence used status-only `curl` calls and in-memory `plutil` parsing with `/tmp/media-upload-ticket07.png`, SHA-256 `431ced6916a2a21a156e38701afe55bbd7f88969fbbfc56d7fe099d47f265460`, 68 bytes, `image/png`, `1x1`, and idempotency key `9e7763d1-3cd6-4ee5-a7f9-0701f6a09eaa`. Opaque capabilities were parsed only in shell memory and never printed or stored. | `/livez=200`, `/readyz=200`, create `201`, exact replay `200` with the same upload ID, direct PUT `200`, completion `202`, terminal `ready`, content `307`, download `200`, and `cmp=0` in one second. Status exposed decoder-derived `68,image/png,1x1` and no capability. Transition deltas were `1,1,1`; exact safe log counts were `service.started=1`, `service.stopping=1`, `service.stopped=1`, `capability.issued=4`, `upload.transition=3`, `finalize.phase=16`, and `http.request_finished=12`. The log regex audit found no `put_succeeded`, `X-Amz-`, `location`, `authorization`, `object_key`, `bucket`, `sha256`, `idempotency_key`, `claim_token`, or `reservation_token`. | One loopback Garage flow on the recorded revision reached verified `ready` and returned exact bytes; this is not an AWS, production, performance, or arbitrary-failure claim. |

Never store presigned URLs, redirect `Location`, credentials, DSNs or environment
dumps, raw headers or bodies, image bytes, bucket/object keys, provider errors, or
other secrets in evidence.

## Security boundary and limitations

- The supported runtime is loopback-only on one trusted host. Do not bind it to a LAN,
  public proxy, or the Internet.
- There is no application authentication, TLS termination, rate limiting, quota,
  storage-side 10 MiB enforcement, public media edge, or browser CORS contract.
- The bucket must remain private and runtime credentials must be least-privilege.
- Presigned URLs are replayable bearer capabilities until expiry; a successful direct
  PUT is not publication evidence.
- Cleanup is idempotent and at least once, not exactly once. Permanent tombstones are
  not garbage-collected from PostgreSQL.
- One process and one sequential finalizer are intentional local-demo limits; HA,
  multipart uploads, CDN delivery, performance claims, and capacity claims are absent.
- Garage `v2.3.0` is the tested local target. Live AWS and broad S3 compatibility are
  not verified.
- The project does not claim production readiness, arbitrary-failure recovery, zero
  data loss, public deployment, or open-source licensing.
