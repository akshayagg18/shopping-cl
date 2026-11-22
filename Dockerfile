# Build a static binary, then ship it alone: the runtime image is one file,
# so container start-up costs almost nothing.
# The compiler is pinned: pgo/ssp.profdata is a profile of this exact source built
# by rust 1.97.0 (scorer runs: profile-guided build 6% faster at 20M and 60M).
FROM --platform=linux/amd64 rust:1.97.0-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
# Scorer CPUs are Cascade Lake or newer: AVX2/BMI2 are safe to target.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata" \
    CARGO_NET_RETRY=10 \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp

FROM scratch
COPY --from=build /ssp /ssp
USER 1000:1000
ENTRYPOINT ["/ssp"]
