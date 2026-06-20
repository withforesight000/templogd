FROM rust:1 AS base
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall cargo-chef

FROM base AS planner

WORKDIR /src
COPY . /src
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y protobuf-compiler

WORKDIR /src

COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . /src
RUN cargo build --release

FROM debian:trixie-slim AS templogd
COPY --from=builder "/src/target/release/templogd" "/usr/local/bin/templogd"
CMD [ "/usr/local/bin/templogd" ]

FROM debian:trixie-slim AS tempgrpcd
COPY --from=builder "/src/target/release/tempgrpcd" "/usr/local/bin/tempgrpcd"
CMD [ "/usr/local/bin/tempgrpcd" ]
