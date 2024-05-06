FROM rust:1 as builder

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y protobuf-compiler

WORKDIR /src
COPY . /src
RUN cargo build --release

FROM debian:bookworm-slim as templogd
COPY --from=builder "/src/target/release/templogd" "/usr/local/bin/templogd"
CMD [ "/usr/local/bin/templogd" ]

FROM debian:bookworm-slim as tempgrpcd
COPY --from=builder "/src/target/release/tempgrpcd" "/usr/local/bin/tempgrpcd"
CMD [ "/usr/local/bin/tempgrpcd" ]
