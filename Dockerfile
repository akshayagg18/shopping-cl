# Build a static binary, then ship it alone: the runtime image is one file,
# so container start-up costs almost nothing.
FROM --platform=linux/amd64 rust:1-slim@sha256:6f14b2b4b278ec0d50aa1d69541c304ce0f95e0f6268487aa9924af1c2e9aebe AS build
COPY . /src
# Scorer CPUs are Cascade Lake or newer: AVX2/BMI2 are safe to target.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes" \
    CARGO_NET_RETRY=10 \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export CARGO_HTTP_CAINFO=/run/secrets/build_ca; fi; \
    cd /src && cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp

FROM scratch
COPY --from=build /ssp /ssp
COPY LICENSE /LICENSE
USER 1000:1000
ENTRYPOINT ["/ssp"]
