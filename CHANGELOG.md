# Changelog

All notable changes to Ferrite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure and core components
- Buffer pool manager with LRU-K replacement policy
- B+ tree index implementation
- Write-ahead logging (WAL) for crash recovery
- SQL parser, binder, and optimizer
- Vectorized execution engine
- Transaction manager with concurrency control
- Lock manager with deadlock detection
- Network protocol for client-server mode
- CLI interface for interactive queries
- Comprehensive test suite including SQL logic tests

### Changed
- Nothing yet

### Deprecated
- Nothing yet

### Removed
- Nothing yet

### Fixed
- Nothing yet

### Security
- Nothing yet

## [0.1.0] - Unreleased

### Added
- Initial release of Ferrite DBMS
- Core storage engine with page-based layout
- Buffer pool with configurable size and LRU-K eviction
- B+ tree indexes for efficient lookups
- Basic SQL support (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE)
- Transaction support with ACID guarantees
- WAL-based crash recovery
- Embedded and client-server operation modes

---

[Unreleased]: https://github.com/ferritedb/ferrite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ferritedb/ferrite/releases/tag/v0.1.0
