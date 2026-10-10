set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

check:
    cargo fmt --all -- --check
    cargo test --workspace --locked
    cargo clippy --workspace --all-targets --locked -- -D warnings

integration:
    python3 scripts/check-compose.py

sdk-check:
    python3 scripts/check-sdk.py

protocol-check:
    python3 scripts/check-protocol.py
