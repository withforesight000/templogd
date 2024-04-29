ARG BINARY_NAME

FROM rust:1 as builder

WORKDIR /src
COPY . /src
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder "/src/target/release/templogd" "/usr/local/bin/templogd"
CMD [ "/usr/local/bin/${BINARY_NAME}" ]
