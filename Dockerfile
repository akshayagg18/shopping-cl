# DIAGNOSTIC image (D25's schedule):
#   /ssp      "pgo" = v19 (per-line tokenizer up to 127-byte lines), profile-guided
#   /ssp_alt  "alt" = v18 exactly as submitted (profile-guided)
#   /ssp_np   "np"  = v19 without the profile
FROM --platform=linux/amd64 rust:1.97.0-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t1 \
 && cp /t1/x86_64-unknown-linux-gnu/release/ssp /ssp \
 && RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t2 \
 && cp /t2/x86_64-unknown-linux-gnu/release/ssp /ssp_np

FROM --platform=linux/amd64 rust:1.97.0-slim AS alt
WORKDIR /src
COPY alt/Cargo.toml alt/Cargo.lock ./
COPY alt/src ./src
COPY alt/pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp_alt

FROM scratch
COPY --from=build /ssp /ssp
COPY --from=build /ssp_np /ssp_np
COPY --from=alt /ssp_alt /ssp_alt
ENV SS_DIAG=1
USER 1000:1000
ENTRYPOINT ["/ssp"]
