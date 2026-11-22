# DIAGNOSTIC image: three builds of the v18 source (rust 1.97.0), D25's schedule:
#   /ssp      "pgo" = v18 exactly (x86-64-v3, profile pgo/ssp.profdata)
#   /ssp_alt  "alt" = tuned for Cascade Lake without AVX-512, same profile
#   /ssp_np   "np"  = x86-64-v3 with a profile of a 20M run (pgo/ssp20.profdata)
FROM --platform=linux/amd64 rust:1.97.0-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t1 \
 && cp /t1/x86_64-unknown-linux-gnu/release/ssp /ssp \
 && RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f,+crt-static -Cprofile-use=/src/pgo/ssp.profdata" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t2 \
 && cp /t2/x86_64-unknown-linux-gnu/release/ssp /ssp_alt \
 && RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp20.profdata" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t3 \
 && cp /t3/x86_64-unknown-linux-gnu/release/ssp /ssp_np

FROM scratch
COPY --from=build /ssp /ssp
COPY --from=build /ssp_alt /ssp_alt
COPY --from=build /ssp_np /ssp_np
ENV SS_DIAG=1
USER 1000:1000
ENTRYPOINT ["/ssp"]
