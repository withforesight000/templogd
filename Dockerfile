FROM dhi.io/rust:1-debian13-dev AS base
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gzip build-essential cmake pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
        https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh \
        -o /tmp/install-binstall.sh \
    && bash /tmp/install-binstall.sh \
    && rm /tmp/install-binstall.sh
RUN cargo binstall cargo-chef --locked --no-confirm

FROM base AS planner

WORKDIR /src
COPY . /src
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder

WORKDIR /src

COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY . /src
RUN cargo build --release --locked

FROM dhi.io/debian-base:trixie-debian13 AS templogd
COPY --from=builder --chmod=755 "/src/target/release/templogd" "/usr/local/bin/templogd"
USER 65532:65532
ENTRYPOINT []
CMD [ "/usr/local/bin/templogd" ]

FROM dhi.io/debian-base:trixie-debian13 AS tempgrpcd
COPY --from=builder --chmod=755 "/src/target/release/tempgrpcd" "/usr/local/bin/tempgrpcd"
USER 65532:65532
ENTRYPOINT []
CMD [ "/usr/local/bin/tempgrpcd" ]
