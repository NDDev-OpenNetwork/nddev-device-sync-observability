set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

check:
    cargo fmt --all -- --check
    cargo test --workspace --all-features --locked --jobs 4
    cargo clippy --workspace --all-targets --all-features --locked --jobs 4 -- -D warnings

integration:
    python3 scripts/check-compose.py

sdk-check:
    python3 scripts/check-sdk.py

protocol-check:
    python3 scripts/check-protocol.py
