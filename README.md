# templogd Workspace

This repository hosts a pair of Rust services that collect ambient sensor readings from a Nature Remo device and expose that data over gRPC. The services share a `common` library and persist data inside Redis Streams so that historical readings can be queried efficiently.

## Components
- **templogd** – asynchronous daemon that polls the Nature Remo REST API every 30 seconds and appends temperature, humidity, and illumination readings to a Redis stream.
- **tempgrpcd** – gRPC server that reads the same Redis stream and returns ambient readings, optionally down-sampled via a Lua helper that is loaded into Redis at startup.
- **common** – shared abstractions for HTTP access, Redis integration, models, and cross-crate messaging.

## Architecture & Data Flow
1. `templogd` authenticates against the Nature Remo API using a bearer token and selects a device by ID.
2. Each reading is pushed to the Redis stream `ambient_condition` with an auto-generated ID (`*`), storing three fields: `temperature`, `humidity`, and `illumination`.
3. `tempgrpcd` exposes a single gRPC method `tempgrpcd.v1.TempgrpcdService/GetAmbientConditions` that queries Redis:
   - When `samples` is unset, the service runs `XRANGE` between the requested timestamps.
   - When `samples` is provided, Redis executes the registered Lua function (`xrange_with_sampling`) to return monotonically spaced samples across the interval.
4. Both binaries rely on Tokio, tracing instrumentation, and a lightweight in-process message bus (`tokio::mpsc`) to isolate I/O from business logic.

## Prerequisites
- Rust toolchain with 2024 edition support (Rust 1.82+). Run `rustup update` if your compiler is older.
- `protobuf-compiler` (a.k.a. `protoc`) so `tempgrpcd` can build gRPC descriptors.
- Redis 7.x for local development, or access to a compatible Redis deployment.
- Nature Remo API token and the target device ID.
- SSH access to the private dependency `tempgrpcd-protos` hosted at `ssh://gitea/ryuichiro/protobuf-rust.git`.

## Running Locally
1. Start Redis (for example: `docker run --rm -p 6379:6379 redis:7`).
2. Export the required environment variables (see below) or pass them as CLI flags.
3. Launch the binaries:
   - `cargo run -p templogd -- --api-token "$TEMPLOGD_NATURE_REMO_API_TOKEN" --device-id "$TEMPLOGD_NATURE_REMO_DEVICE_ID" --redis-host 127.0.0.1 --redis-port 6379`
   - `cargo run -p tempgrpcd -- --server-bind-address 0.0.0.0 --server-port 50051 --bearer-token "$TEMPGRPCD_BEARER_TOKEN" --redis-host 127.0.0.1 --redis-port 6379`

### gRPC quick check
Use `grpcurl` (or similar) to call the server once data exists in Redis:

```bash
grpcurl \
  -plaintext \
  -import-path path/to/protos \
  -proto tempgrpcd/v1/service.proto \
  -H "authorization: Bearer $TEMPGRPCD_BEARER_TOKEN" \
  -d '{
        "startTime": { "seconds": "1715059200" },
        "endTime":   { "seconds": "1715062800" }
      }' \
  localhost:50051 tempgrpcd.v1.TempgrpcdService/GetAmbientConditions
```

Add a `"samples": 24` field when you want the Lua-powered down-sampling that approximates the interval into evenly spaced buckets.

## Environment Variables
### templogd
- `TEMPLOGD_NATURE_REMO_API_TOKEN` – bearer token issued by Nature Remo.
- `TEMPLOGD_NATURE_REMO_DEVICE_ID` – device identifier to filter the API response.
- `TEMPLOGD_REDIS_HOST` (default: `127.0.0.1`).
- `TEMPLOGD_REDIS_PORT` (default: `6379`).

### tempgrpcd
- `TEMPGRPCD_SERVER_BIND_ADDRESS` – address to bind (e.g. `0.0.0.0`).
- `TEMPGRPCD_SERVER_PORT` – listening port.
- `TEMPGRPCD_BEARER_TOKEN` – token expected in the `authorization` metadata header.
- `TEMPGRPCD_REDIS_HOST` / `TEMPGRPCD_REDIS_PORT` – Redis endpoint shared with `templogd`.

## Docker & Compose
The multi-stage `Dockerfile` uses `cargo-chef` to cache dependencies and exposes two stages named `templogd` and `tempgrpcd`. Build and run the entire stack with Redis using Docker Compose:

```bash
docker compose up --build
```

`docker compose` reads `compose.yml`, builds both binaries in release mode, and provisions Redis with a persistent volume (`redis-data`). Replace the placeholders in `compose.yml` with real credentials before running in production.

## Development Workflow
- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets --all-features`
- Tests: `cargo test --workspace`

The workspace uses `tracing` for logging; set `RUST_LOG=debug` (or rely on the defaults) to observe span-oriented logs during development. When modifying gRPC contracts, update the private `tempgrpcd-protos` crate first, then re-run `cargo build` so tonic can regenerate bindings.

## Directory Layout
- `templogd/` – CLI, configuration, controllers, and use cases for the Nature Remo polling daemon.
- `tempgrpcd/` – gRPC server, Redis fetch pipeline, and Askama template used to render the Lua sampling function.
- `common/` – shared gateways (HTTP, Redis), models (`AmbientCondition`, repositories), and async client wrappers.
- `compose.yml` / `Dockerfile` – container orchestration for development or deployment.

## Troubleshooting
- **Missing `tempgrpcd-protos`** – verify your SSH configuration matches the snippet above and that your agent has the correct key.
- **Redis script errors** – the server loads `xrange_with_sampling` under the `mylib` library. Flush scripts with `FT.CALL`? no; if the LUA function signature changes, restart Redis or run `FUNCTION FLUSH` (with caution) before redeploying.
- **Nature Remo authentication** – the API returns HTTP 401 if the token is invalid; the daemon logs the failure and retries after the 30-second interval.
