#!/usr/bin/env bash
# Formatting plus the unit and executable integration tests. Runs inside the stock-rust image with
# the tree mounted at /src (see SETUP.md).
set -u
cd /src
echo "=== cargo fmt -- --check"
cargo fmt -- --check > /tmp/fmt.out 2>&1; echo "fmt exit: $?"; head -30 /tmp/fmt.out
echo "=== cargo test --locked"
cargo test --locked > /tmp/test.out 2>&1; echo "test exit: $?"
grep -E "^test result|FAILED|panicked|^error|^failures:|^    [a-z_:]+$" /tmp/test.out | head -60
