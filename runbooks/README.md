# chia-peer runbooks

## Local development

Prerequisites: a stable Rust toolchain (`rustup toolchain install stable`) with `rustfmt` + `clippy`,
plus `cargo-nextest` and `cargo-llvm-cov` for the coverage gate. On Linux the native TLS stack builds
vendored OpenSSL (no system package needed).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo llvm-cov nextest --fail-under-lines 80 --all-features --workspace   # tests + coverage gate
cargo doc --no-deps --all-features
```

The test suite is fully offline: unit tests plus an **in-process wallet-protocol simulator**
(`chia-wallet-sdk`'s `peer-simulator`, a dev-dependency) exercise connect/subscribe/submit/reorg with
no live network.

### Using the library

```rust,no_run
use chia_peer::{ChiaLightClient, ChiaPeerConfig};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Discover a public mainnet peer (IPv6-first), or point at your own node with
    // `ChiaPeerConfig::mainnet().with_trusted_endpoint(addr)`.
    let client = ChiaLightClient::connect(ChiaPeerConfig::mainnet()).await?;

    // Register the read side as a ChainSource provider (needs a multi-thread runtime handle).
    let provider = client.as_chain_source_provider(tokio::runtime::Handle::current());
    let _ = provider; // hand to a dig-chainsource registry
    Ok(())
}
```

The `ChainSource` provider is a **synchronous** facade; it MUST be built on and driven from a
multi-thread tokio runtime (or wrapped in `tokio::task::spawn_blocking` from async code).

## Release / deploy (crates.io)

chia-peer is a per-merge-tag published crate (CLAUDE.md §3.6, group B).

1. Bump `version` in `Cargo.toml` in the PR (SemVer; the `Check Version Increment` gate enforces it).
2. On merge to `main`, `.github/workflows/release.yml` regenerates `CHANGELOG.md` (git-cliff), commits
   it, and pushes tag `vX.Y.Z` — using `secrets.RELEASE_TOKEN` (a classic PAT; a `GITHUB_TOKEN`-pushed
   tag would not trigger the publish workflow).
3. The pushed tag triggers `.github/workflows/publish.yml`, which builds, packages, and
   `cargo publish`es to crates.io using `secrets.CARGO_REGISTRY_TOKEN`, then cuts a GitHub Release.

### First release

GitHub does not fire a `push: main` workflow on the squash-merge that first adds the workflow file.
Kick the first release manually with `gh workflow run release.yml -R DIG-Network/chia-peer` (a
`workflow_dispatch` trigger is wired for exactly this), or push an empty commit to `main` with the
`RELEASE_TOKEN` identity.

### Secrets required on the repo

- `RELEASE_TOKEN` — classic PAT allowed past branch protection (changelog commit + tag push).
- `CARGO_REGISTRY_TOKEN` — crates.io publish token.
