# DIAGNOSTIC image, both profile-guided on rust 1.97.0:
#   /ssp      v18b (v18 + person map from zeroed memory), run with partition variants
#   /ssp_alt  "alt" = v18 exactly as submitted
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

FROM scratch
COPY --from=b1 /ssp /ssp
COPY --from=b2 /ssp_alt /ssp_alt
ENV SS_DIAG=1
USER 1000:1000
ENTRYPOINT ["/ssp"]
