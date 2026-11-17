# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Validate required AMQP references without reflecting over concurrently active channel internals.

- Give each RabbitMQ delivery its own tracing lifetime and safely capture errors on ended spans, including concurrent span completion.
- Apply the five-second subscription callback default consistently, with positive common/backend timeout overrides.

### Changed

- Update compatible dependencies and pin supported development/CI tools (Go 1.27.4; consumer minimum Go 1.26.11).
- Keep unit tests hermetic and runnable through `make ci` without an environment-file loader.

## Project Roadmap

- [ ] Create and Update should allow to pass more than one object per time (Bulk Create and Update)

## [1.0.0] - 2023-02-08
### Added
- First release.
