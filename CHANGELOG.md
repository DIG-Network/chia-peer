# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org) and
[Conventional Commits](https://www.conventionalcommits.org).

## [0.1.2] - 2026-07-20

### Bug Fixes
- Clamp live-fetched `confirmed_height` to the known peak on every provider read path, so an
  above-peak coin reports 0 confirmations instead of underflowing a consumer's `peak - confirmed`
  (u32) count into a spurious hyper-confirmed value (#1326)

## [0.1.1] - 2026-07-20

### Bug Fixes
- Lower peak on authoritative reorg (confirmation accuracy) (#1311) (#2)

## [0.1.0] - 2026-07-20

### Features
- Chia-peer light-client crate (dig-node seam 1) (#1)

### Documentation
- Bootstrap chia-peer repository


