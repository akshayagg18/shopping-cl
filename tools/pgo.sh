#!/bin/bash
# usage: pgo.sh SRC_DIR OUT_PROFDATA -> instrumented build of SRC (scorer flags), runs on
# v5-style training data at 8 and 2 threads, merges the profile.
set -e
S=$1; O=$2
F="-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes"
T=/dev/shm/pgo_train
[ -d $T ] || /home/claude/gen5/target/release/generate --seed 7 --events 2000000 --clicks 500000 --output $T > /dev/null
rm -rf /tmp/pgo_raw && mkdir -p /tmp/pgo_raw
(cd $S && RUSTFLAGS="$F -Cprofile-generate=/tmp/pgo_raw" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /tmp/tgt_gen 2>&1 | tail -1)
for n in 8 2; do rm -rf /tmp/pgo_o; DATA_DIR=$T OUTPUT_DIR=/tmp/pgo_o NCPU=$n /tmp/tgt_gen/x86_64-unknown-linux-gnu/release/ssp; done
/usr/lib/llvm-20/bin/llvm-profdata merge -o $O /tmp/pgo_raw/*.profraw
ls -la $O
