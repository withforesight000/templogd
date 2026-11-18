# templogd

`templogd` collects temperature, humidity, and illumination from a Nature Remo device. It polls the Nature Remo REST API every 30 seconds, uses a bearer token to authenticate, and selects the device by ID. It writes the three fields (`temperature`, `humidity`, and `illumination`) to the Redis stream `ambient_condition` with an automatically generated entry ID.

`tempgrpcd` serves those readings through the authenticated gRPC method `tempgrpcd.v1.TempgrpcdService/GetAmbientConditions`. For requests without `samples`, it uses Redis `XRANGE` to read the requested time range. With `samples` set, it calls the Lua function `xrange_with_sampling`. If the range contains more readings than requested, the function looks for readings near evenly spaced points in the range.

Both services run on Tokio, use `tracing` for logs, and communicate with their Redis workers through bounded `tokio::mpsc` channels. The `common` crate contains shared models, repository traits, HTTP and Redis adapters, and channel message types.

## Run locally

You need a current stable Rust toolchain with Rust 2024 support, Redis 7, and a Nature Remo API token and device ID. To try the gRPC example, install a version of `grpcurl` that supports v1 server reflection. [grpcurl 1.8.8 release notes](https://github.com/fullstorydev/grpcurl/releases/tag/v1.8.8)

1. Start Redis, for example with `docker run --rm -p 127.0.0.1:6379:6379 redis:7`.
2. Export `TEMPLOGD_NATURE_REMO_API_TOKEN`, `TEMPLOGD_NATURE_REMO_DEVICE_ID`, and `TEMPGRPCD_BEARER_TOKEN`. See [Environment variables](#environment-variables) for the full list of settings. Both services also accept command-line options.
3. Run the services in separate terminals with those variables set.

   ```sh
   cargo run -p templogd -- --redis-host 127.0.0.1 --redis-port 6379
   ```

   ```sh
   cargo run -p tempgrpcd -- --server-bind-address 127.0.0.1 --server-port 50051 --redis-host 127.0.0.1 --redis-port 6379 --allow-direct-clients
   ```

### Query readings with grpcurl

Once Redis contains readings, use the server's gRPC reflection service to call `GetAmbientConditions`. Replace the example timestamps with an RFC 3339 time range that contains your readings. Add `"samples": 24` to the JSON request to ask for up to 24 samples. [Protobuf timestamp format](https://protobuf.dev/programming-guides/json/#well-known-types)

```sh
grpcurl \
  -plaintext \
  -H "authorization: Bearer $TEMPGRPCD_BEARER_TOKEN" \
  -d '{
        "startTime": "2024-05-07T05:20:00Z",
        "endTime":   "2024-05-07T06:20:00Z"
      }' \
  127.0.0.1:50051 tempgrpcd.v1.TempgrpcdService/GetAmbientConditions
```

### Authenticated gRPC health checks

Both `grpc.health.v1.Health/Check` and `grpc.health.v1.Health/Watch` require `authorization: Bearer <token>` metadata and follow the same connection peer rules as data requests. Configure monitoring clients with authentication before deploying this version. Keep tokens out of probe logs.

Check the named service `tempgrpcd.v1.TempgrpcdService`. A `SERVING` response confirms that an authenticated client can reach the gRPC service. To verify that the service can retrieve readings from Redis, query a time range that contains readings.

Deploy the server before clients that rely on authenticated health checks. During rollback, revert those clients before restoring a server with public health checks.

## Environment variables

### templogd

| Variable | Default | Purpose |
| --- | --- | --- |
| `TEMPLOGD_NATURE_REMO_API_TOKEN` | Required | Bearer token issued by Nature Remo. |
| `TEMPLOGD_NATURE_REMO_DEVICE_ID` | Required | Device ID used to select readings from the API response. |
| `TEMPLOGD_REDIS_HOST` | Required | Redis hostname or IP address. |
| `TEMPLOGD_REDIS_PORT` | `6379` | Redis port. |

### tempgrpcd

| Variable | Default | Purpose |
| --- | --- | --- |
| `TEMPGRPCD_SERVER_BIND_ADDRESS` | Required | Address on which the server listens, such as `127.0.0.1`. |
| `TEMPGRPCD_SERVER_PORT` | Required | gRPC listening port. |
| `TEMPGRPCD_BEARER_TOKEN` | Required | Token expected in the `authorization` metadata. |
| `TEMPGRPCD_REDIS_HOST` | Required | Hostname or IP address of the Redis instance used by `templogd`. |
| `TEMPGRPCD_REDIS_PORT` | `6379` | Redis port. |
| `TEMPGRPCD_TRUSTED_PROXY_IPS` | Empty | Comma-separated IP addresses of permitted connection peers. Required unless direct clients are allowed. |
| `TEMPGRPCD_ALLOW_DIRECT_CLIENTS` | `false` | Skips connection peer validation for local development. Keep the listener or published port on loopback when using this option. Leave it disabled in production. |
| `TEMPGRPCD_MAX_RESULT_ENTRIES` | `5000` | Maximum entries in an unsampled response. Queries over the limit fail without returning partial results. |
| `TEMPGRPCD_MAX_SAMPLES` | `1000` | Maximum value a client can request for `samples`. |
| `TEMPGRPCD_MAX_CONCURRENT_REQUESTS` | `4` | Maximum concurrent history RPCs across all connections. |
| `TEMPGRPCD_REQUEST_TIMEOUT_MILLISECONDS` | `5000` | History RPC deadline in milliseconds. |
| `TEMPGRPCD_ACCEPT_LEGACY_MILLISECOND_TIMESTAMPS` | `false` | Temporary compatibility option for old roomtemp clients that put milliseconds in the timestamp's `seconds` field. |

The result limit, sample limit, concurrency limit, and request deadline must be positive. For each setting, the default is also the highest permitted value. Set `TEMPGRPCD_MAX_SAMPLES` at or below `TEMPGRPCD_MAX_RESULT_ENTRIES`.

### Logging

| Variables | Default | Accepted values |
| --- | --- | --- |
| `TEMPLOGD_LOG_FORMAT`, `TEMPGRPCD_LOG_FORMAT` | `json` | `json`, `text` |
| `TEMPLOGD_LOG_LEVEL`, `TEMPGRPCD_LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error`, `off` |

## Run with Docker Compose

Replace the `XXX` placeholders in [compose.yml](compose.yml) locally with the Nature Remo API token, the device ID, and the gRPC bearer token. Keep real credentials out of commits.

```sh
docker compose up --build
```

Compose builds release binaries using the `templogd` and `tempgrpcd` targets in the [Dockerfile](Dockerfile). The build uses `cargo-chef` to cache dependencies. Compose stores Redis data in the `redis-data` volume and publishes gRPC on `127.0.0.1:50051` with direct clients allowed by default.

## Development workflow

Run these local checks before submitting changes. CI also checks the sampling function against Redis 7 and reports test coverage.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --no-fail-fast
```

| Path | Contents |
| --- | --- |
| [templogd/](templogd/) | CLI, configuration, controllers, and use cases for the Nature Remo polling daemon. |
| [tempgrpcd/](tempgrpcd/) | gRPC server, Redis queries, and the Askama template for the Lua sampling function. |
| [common/](common/) | Shared gateways, domain models such as `AmbientCondition`, repository traits, and async client wrappers. |

## Troubleshooting

### Redis rejects the sampling function

At startup, `tempgrpcd` loads the `mylib` library with replacement. If loading fails, check the Redis version and the startup error before changing the Redis function library.

### Nature Remo returns HTTP 401

Check the API token. The daemon logs the failed request and retries after 30 seconds.
