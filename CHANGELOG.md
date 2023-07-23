# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.7.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.7.0) (2023-07-23)
[0.6.0...0.7.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.6.0...0.7.0)

### Added
- Advanced, optimised, efficient, custom implementation of `ReadOnlyKeyValueStore` for 'Interactive Queries' with for 'partitioned' type CassandraKeyValueStore
  - Offered by `CassandraStateStore` static methods, for an example please check the [README](README.md#interactive-queries)
    - `CassandraStateStore#readOnlyPartitionedKeyValueStore(KafkaStreams, String, CqlSession, String, boolean, String, Serde, Serde)`
    - `CassandraStateStore#readOnlyPartitionedKeyValueStore(KafkaStreams, String, CqlSession, String, boolean, String, Serde, Serde, Function, StreamPartitioner)`
- New example '[processor-api-all-range-prefix-count](examples/processor-api-all-range-prefix-count)' featuring the new `CassandraReadOnlyKeyValueStore`
  - Implements a REST API with endpoints for all available 'Interactive Query' methods to play with hands-on

### Changed
- Integration test is renamed `WordCountGlobalStoreTest` -> `WordCountGlobalKeyValueStoreTest`
  - test + assertions added for `ReadOnlyKeyValueStore.all()`

### Removed
- First impl. of `CassandraStateStore#readOnlyPartitionedKeyValueStore(KafkaStreams, String)` was removed and replaced by the more complex methods that return an instance of `CassandraReadOnlyKeyValueStore` (already mentioned above) 


## [0.6.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.6.0) (2023-07-11)
[0.5.0...0.6.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.5.0...0.6.0)

### Changed
- to have an explicit naming of store types, it was decided to rename `CassandraStores.keyValueStore` to `CassandraStores.partitionedKeyValueStore`
- the renamed `partitionedKeyValueStore` is no longer recommended as the default choice


## [0.5.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.5.0) (2023-07-09)
[0.4.0...0.5.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.4.0...0.5.0)

### Added
- Interface `CassandraStateStore` https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/19 https://github.com/thriving-dev/kafka-streams-cassandra-state-store/pull/20   
  - implemented by all the libraries custom StateStore solutions (only `CassandraKeyValueStore` _yet_)
  - provides static helper methods to get ReadOnly stores for interactive queries (see [docs](README.md#interactive-queries))

### Changed
- dependency updates, incl. kafka-streams 3.5.0 https://github.com/thriving-dev/kafka-streams-cassandra-state-store/pull/22   
  Reminder: the library itself does not have any transitive dependencies. All dependencies are _compileOnly_ or for tests.


## [0.4.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.4.0) (2023-05-30)
[0.3.2...0.4.0](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.3.2...0.4.0)

### Added
- feat: Add builder config option - opt-in to enable count for approximateNumEntries [#10](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/10)
- feat: Add builder config option - allow setting execution profiles (for DDL; DML) [#11](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/11)
- feat: Add builder config option - opt-out to avoid tables to be auto-created [#9](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/9)
- feat: add Quarkus examples app as GraalVM native image [#7](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/7)

### Changed
- store size via `approximateNumEntries` is now by default disabled - opt-in option has been added to builder - ref [#10](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/10)

### Fixed
- bug: cassandra concurrent schema updates (initial concurrent table auto-creation) [#12](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/12)
- small fix: example 'processor-api-all-range-prefix-count' not runnable

### Other
- documentation improved


## [0.3.2](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.3.2) (2023-04-17)
[0.3.1...0.3.2](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.3.1...0.3.2)

### Changed
- fix: small refactoring to allow better reuse of `AbstractCassandraKeyValueStoreRepository`

### Changed (documentation only)
- feat: improve documentation for recommended kafka streams config
- feat: add documentation on 'Cassandra table partitioning' (avoiding large partitions)


## [0.3.1](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/releases/tag/0.3.1) (2023-03-09)
[0.3.0...0.3.1](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/0.3.0...0.3.1)

### (Added)
- Adds a first set of **integration tests** with _testcontainers_
  - Covering
    - **WordCountTest** testing the kafka streams 'Hello world!' app to work with CassandraKeyValueStore
    - **WordCountInteractiveQueriesTest** testing interactive queries incl. methods `all`, `range`, `prefixScan`, `approximateNumEntries` 
    - **WordCountGlobalStoreTest** testing `CassandraStores.globalKeyValueStore()` and store access via interactive queries 
  - Using
    - Cassandra 4.1
    - Redpanda

### Changed / Removed
- Simplified overly complex class name to `GlobalCassandraKeyValueStoreRepository`.  


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
[a.b.c...x.y.z](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/compare/a.b.c...x.y.z)

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
