FROM rust:1 AS planner

RUN cargo install cargo-chef

WORKDIR /src
COPY . /src
RUN cargo chef prepare --recipe-path recipe.json

FROM rust:1 AS builder

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y protobuf-compiler
RUN cargo install cargo-chef

WORKDIR /src

COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . /src
RUN cargo build --release

FROM debian:bookworm-slim AS templogd
COPY --from=builder "/src/target/release/templogd" "/usr/local/bin/templogd"
CMD [ "/usr/local/bin/templogd" ]

FROM debian:bookworm-slim AS tempgrpcd
COPY --from=builder "/src/target/release/tempgrpcd" "/usr/local/bin/tempgrpcd"
CMD [ "/usr/local/bin/tempgrpcd" ]
