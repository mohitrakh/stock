# The `stock-rust` image every build, test and measurement script runs in (see SETUP.md).
# The office machine built it from rust:latest in early October 2026: cargo 1.98.1.
FROM rust:latest
RUN apt-get update -qq && apt-get install -y -qq postgresql-client curl > /dev/null && rm -rf /var/lib/apt/lists/* \
 && rustup component add rustfmt
