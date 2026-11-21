# Build a static binary, then ship it alone: the runtime image is one file,
# so container start-up costs almost nothing.
FROM --platform=linux/amd64 rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorer CPUs are Cascade Lake or newer: AVX2/BMI2 are safe to target.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" \
    CARGO_NET_RETRY=10 \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp

FROM scratch
COPY --from=build /ssp /ssp
USER 1000:1000
ENTRYPOINT ["/ssp"]
