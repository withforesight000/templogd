# AGENTS.md

Check `git status` before editing. Preserve user changes and keep unrelated edits out of your changes.

## Repository overview

This Rust 2024 workspace collects readings from a Nature Remo device and serves them over gRPC. Every 30 seconds, `templogd` polls the Nature Remo API and writes temperature, humidity, and illumination to the Redis stream `ambient_condition`. `tempgrpcd` reads the stream and serves the readings through an authenticated gRPC API, with optional sampling through a Redis Lua function.

The `common` crate contains shared domain models, repository traits, HTTP and Redis adapters, and logging support. Both binaries use bounded Tokio MPSC channels to communicate with their datastore workers and a `CancellationToken` to coordinate shutdown.

## Where to make changes

| Area | Path |
| --- | --- |
| Workspace dependencies and release settings | [Cargo.toml](Cargo.toml) |
| Local and container usage, environment variables | [README.md](README.md), [compose.yml](compose.yml) |
| Shared repository traits | [common/src/model/repository/](common/src/model/repository/) |
| Adapters for external systems | [common/src/gateway/](common/src/gateway/), [common/src/infra/](common/src/infra/) |
| Polling daemon orchestration | [templogd/src/infra/tasks.rs](templogd/src/infra/tasks.rs) |
| gRPC startup and task supervision | [tempgrpcd/src/infra/server.rs](tempgrpcd/src/infra/server.rs) |
| gRPC routing and reflection | [tempgrpcd/src/infra/grpc.rs](tempgrpcd/src/infra/grpc.rs) |
| gRPC authentication | [tempgrpcd/src/infra/auth.rs](tempgrpcd/src/infra/auth.rs) |
| Redis function loading | [tempgrpcd/src/infra/tasks.rs](tempgrpcd/src/infra/tasks.rs) |
| Request validation | [tempgrpcd/src/validator/](tempgrpcd/src/validator/) |
| Lua sampling function | [tempgrpcd/templates/xrange_with_sampling.lua.j2](tempgrpcd/templates/xrange_with_sampling.lua.j2) |
| CI checks | [.github/workflows/ci.yml](.github/workflows/ci.yml) |

Controllers call use cases, and use cases depend on repository and port traits. Infrastructure provides the implementations of those traits. Put code used by both services in `common` and keep behavior specific to one service in that service's crate.

The Git dependency `tempgrpcd-protos` in [tempgrpcd/Cargo.toml](tempgrpcd/Cargo.toml) supplies the Rust protobuf types. The protobuf source files live in the external repository. Leave generated types untouched and coordinate any public gRPC schema changes with the external repository.

## Build and test

Use a current stable Rust toolchain with Rust 2024 support. The services require Redis 7 to run. Unit tests use mocks or stubs and should run without a live Redis instance.

Run these local checks before handing off changes. CI also checks the sampling function against Redis 7 and reports test coverage.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --no-fail-fast
```

Start with the tests for the code you changed.

```sh
cargo test -p common
cargo test -p templogd
cargo test -p tempgrpcd
cargo test -p tempgrpcd validator::get_ambient_conditions_request::tests
```

Apply formatting with `cargo fmt --all` and build release binaries with `cargo build --release`. To run the full stack, replace the `XXX` placeholders locally in `compose.yml` and run `docker compose up --build`. Keep real tokens out of commits.

## Runtime configuration

Clap accepts settings as long options or environment variables.

- `templogd` requires `TEMPLOGD_NATURE_REMO_API_TOKEN`, `TEMPLOGD_NATURE_REMO_DEVICE_ID`, and `TEMPLOGD_REDIS_HOST`. `TEMPLOGD_REDIS_PORT` defaults to `6379`.
- `tempgrpcd` requires `TEMPGRPCD_SERVER_BIND_ADDRESS`, `TEMPGRPCD_SERVER_PORT`, `TEMPGRPCD_BEARER_TOKEN`, and `TEMPGRPCD_REDIS_HOST`. `TEMPGRPCD_REDIS_PORT` defaults to `6379`. Set `TEMPGRPCD_TRUSTED_PROXY_IPS` unless you enable `TEMPGRPCD_ALLOW_DIRECT_CLIENTS` for local development. See [the README](README.md#tempgrpcd) for all settings.

Preserve the masked `Debug` output of configuration structs and keep API and bearer tokens out of logs and other output. The gRPC `authorization` metadata value must have the exact form `Bearer <token>`.

## Code and test conventions

### Formatting, tests, and Rustdoc

- Follow `rustfmt.toml` with four-space indentation and a 120-column limit.
- Use repository and port traits to substitute test implementations. Prefer deterministic in-process stubs or mocks over external API or Redis calls.
- Put tests next to the implementation in `#[cfg(test)] mod tests`. Together, unit tests must exercise every affected path across success and failure cases, including validation, error propagation, and shutdown. Code produced by external commands or code generators is exempt.
- Test private methods through public behavior whenever practical. Direct tests are appropriate when a private method's complexity makes full coverage through the public interface impractical.
- Add Rustdoc (`///` or `//!`) to new methods and other callable APIs. When editing an existing method, add Rustdoc if its purpose is unclear or if a parameter's type does not explain the shape or meaning of the data it accepts. Explain the purpose, inputs, outputs, errors, and important side effects so a Rust beginner can use the documentation when responding to an incident.

### Startup, shutdown, and errors

- Keep async I/O on Tokio and propagate cancellation to new long-running work. Preserve the `Send`, `Sync`, and `'static` bounds that spawned tasks and async repository traits require.
- Treat polling, datastore, and gRPC server tasks as critical. If one returns before cancellation, returns an error, or panics, cancel its siblings and wait for cleanup. Propagate a typed failure to `main` so the process exits unsuccessfully. Propagate any `JoinError` through that error path, even if it has already been logged.
- Handle startup failures such as invalid bind addresses, Redis connection errors, template rendering errors, and Redis function loading errors. Wrap the original cause in a typed startup error and return it in place of calling `unwrap()` or `expect()`.
- Propagate errors with context. Use `thiserror` types at layer boundaries and preserve the original cause. Convert domain and use case errors to the appropriate tonic `Status` in the gRPC layer only.

### gRPC and Redis contracts

- Validate requests before calling a use case. `GetAmbientConditions` requires both timestamps, the start must be at or before the end, and `samples`, if supplied, must be greater than zero.
- Keep the Redis stream key `ambient_condition`, its fields `temperature`, `humidity`, and `illumination`, and the sampling function name `xrange_with_sampling`. Load the function with replacement during `tempgrpcd` startup.
- If the sampling function signature changes, update the Lua template, the Rust call site, and the tests together. Redis Functions require Redis 7 or later.

### Tracing

- Follow the structured `tracing` conventions in the surrounding code. Keep full request metadata, credentials, and other secrets out of logs.
- Tokio channel send errors can contain the unsent value. Record only safe channel and operation details. Map the failure to an error that omits the payload before propagating it. Keep the raw send error out of logs.
- Carry the originating span or trace ID across an MPSC channel when downstream logs must correlate with a request. Instrument the external call and enter that span when logging completion, failure, or unsupported operations. Describe an operation as coming from gRPC only when it has a request span.
- Record values such as `signal`, `operation`, `reason`, and `task` as structured fields so they can be filtered.
- Use stable span names in the form `<layer>.<method>`. Use the clean architecture layer or source directory as the layer name. When the layer contains multiple external systems or implementations, use `<layer>.<component>.<method>`. Examples include `gateway.redis.fetch_ambient_conditions` and `gateway.nature_remo.fetch_ambient_condition`.
- Follow the same naming rules for `info_span!` and other manually created task or request spans. Use the operation name as the final segment for those spans. For constructors, use the actual method name `new` and avoid suffixes such as `_new`.

## Before handing off changes

- Update `Cargo.lock` when dependencies change. Review dependency changes for advisories and unnecessary additions. The weekly `cargo audit` job is non-blocking.
- Update `README.md` and `compose.yml` when CLI options, environment variables, ports, or operational requirements change. If Compose lists a service's environment variables explicitly, add each new runtime setting there as a host pass-through or with a documented default. Verify both default and overridden values with `docker compose config`.
- Keep CI lint settings in place. Fix the cause of each warning or limit an exception to the smallest scope that needs it.
- Start each commit message with a concise English sentence that begins with a capital letter and ends with a period. Leave the second line blank. From the third line on, summarize the changes in bullets. You may omit the list for a very small change.
