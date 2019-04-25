# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### None

## [2.2.1]
### Changed
- Use Folly library for map concurrency versus std::mutex.

## [2.2.0]
### Changed
- Changed how threads are shutdown fixing many memory leaks.

[Unreleased]: https://github.corp.ebay.com/SDS/iomgr/compare/testing/v2.x...develop
[2.2.1]: https://github.corp.ebay.com/SDS/iomgr/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.corp.ebay.com/SDS/iomgr/compare/5822b12...v2.2.0
