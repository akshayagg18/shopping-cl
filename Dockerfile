# DIAGNOSTIC image, all profile-guided on rust 1.97.0:
#   /ssp      "new" = v26 (v25 + SS_PR toggle), run with page-mapping variants
#   /ssp_alt  "alt" = v20 exactly as submitted (0.8001 s)
#   /ssp_np   "np"  = v24 (linked names during the parse, dictionary Parquet)
FROM --platform=linux/amd64 rust:1.97.0-slim AS b1
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp

FROM --platform=linux/amd64 rust:1.97.0-slim AS b2
WORKDIR /src
COPY alt/Cargo.toml alt/Cargo.lock ./
COPY alt/src ./src
COPY alt/pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp_alt

FROM --platform=linux/amd64 rust:1.97.0-slim AS b3
WORKDIR /src
COPY alt2/Cargo.toml alt2/Cargo.lock ./
COPY alt2/src ./src
COPY alt2/pgo ./pgo
ENV CARGO_NET_RETRY=10 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes -Cprofile-use=/src/pgo/ssp.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu && cp target/x86_64-unknown-linux-gnu/release/ssp /ssp_np

FROM scratch
COPY --from=b1 /ssp /ssp
COPY --from=b2 /ssp_alt /ssp_alt
COPY --from=b3 /ssp_np /ssp_np
ENV SS_DIAG=1
USER 1000:1000
ENTRYPOINT ["/ssp"]
