# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.3.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.3.0) (2023-02-26)
[0.2.0...0.3.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.2.0...0.3.0)

### Added
- `approximateNumEntries` was added using `SELECT COUNT(*)` with WARN log & request timeout of 5s

### Changed / Removed
- `keyValueStore` now supports `org.apache.kafka.streams.state.ReadOnlyKeyValueStore#prefixScan(Object, Serializer)`
  - Implements a solution using `range` on _BLOB_ type key using `((Bytes) from).increment()`, identically to RocksDB + InMemory KV Stores
  - The solution now works for all CQL databases, replacing the ScyllaDB specific solution (see right below)
- adapts, enhances example '**processor-api-all-range-prefix-count**' (previously 'processor-api-prefixscan')
  - added additional description to example README
  - added/fixed JavaDoc for `WordCountProcessor`
  - now also runs `approximateNumEntries`

### Removed
- `stringKeyValueStore` was removed with all belonging code (`PartitionedStringKeyScyllaKeyValueStoreRepository`, ...)
  - This removes the _special implementation_ of `org.apache.kafka.streams.state.ReadOnlyKeyValueStore#prefixScan(Object, Serializer)` for ScyllaDB backed stores with _String_ keys.
    (The implementation was based on ScyllaDB [LIKE operator](https://docs.scylladb.com/stable/cql/dml.html#like-operator) query on _TEXT_ type clustering key)
  - This is longer required since replaced with generic solution implemented for `keyValueStore` (see right above)

### Security
- successfully tested the library against 'CQL injection' via `withTableOptions(..)`
  => tried to add `compaction = { 'class' : 'LeveledCompactionStrategy' };DROP TABLE xyz` which fails due to wrong syntax in Cassandra 3.11/4.1 & ScyllaDB 5.1


## [0.2.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.2.0) (2023-02-12)
[0.1.0...0.2.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.1.0...0.2.0)

### Added
- feat: implements `StateStore.query`
- improved javadoc
- adds documentation on
  - store types + operations
  - builder & options
  - link to examples
  - known limitations
  - cassandra specifics

### Fixed
- for any bug fixes


## [0.1.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.1.0) (2023-01-08)
First public release


## Template [x.y.z](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/x.y.z) (yyyy-mm-dd)
[0.1.0...0.2.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.1.0...0.2.0)

### Added
- for new features

### Changed
- for changes in existing functionality

### Deprecated
- for soon-to-be removed features

### Removed
- for now removed features

### Fixed
- for any bug fixes

### Security
- in case of vulnerabilities
