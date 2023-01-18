# kafka-streams-cassandra-state-store

[![Java CI](https://github.com/hartmut-co-uk/kafka-streams-cassandra-state-store/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/hartmut-co-uk/kafka-streams-cassandra-state-store/actions/workflows/build-gradle-project.yml)

## Overview 
Kafka Streams State Store implementation that persists data to Apache Cassandra.
For now, only KeyValueStore type is supported.

ℹ️ [Kafka Streams](https://kafka.apache.org/documentation/streams/) is a client library for building applications and microservices, where the input and output data are stored in Kafka clusters.   
ℹ️ [Apache Cassandra](https://cassandra.apache.org/) is a free and open-source, distributed, wide-column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.

### Project Status
⚠️Current project status is to be considered **EXPERIMENTAL!!** ⚠️   
Please carefully read documentation provided on [store types](#store-types) and [limitations](#known-limitations).

## Stack

### Implemented/compiled with
* Java 17
* kafka-streams 3.3.1
* datastax java-driver-core 4.15.0

### Supported client-libs
* Kafka Streams 2.7.0+ (maybe even earlier versions, but wasn't tested further back)
* Datastax java client (v4) `'com.datastax.oss:java-driver-core:4.15.0'`
* ScyllaDB shard-aware datastax java client (v4) fork `'com.scylladb:java-driver-core:4.14.1.0'`

### Supported databases
* Apache Cassandra 3.11
* Apache Cassandra 4
* ScyllaDB (should work from 4.3+)

## Get it!

### Maven

Functionality of this package is contained in
Java package `dev.thriving.oss.kafka.streams.cassandra.state.store`.

To use the package, you need to use following Maven dependency:

```xml
<dependency>
    <groupId>dev.thriving.oss</groupId>
    <artifactId>kafka-streams-cassandra-state-store</artifactId>
    <version>${version}</version>
</dependency>
```

### Gradle

#### Groovy DSL
```groovy
implementation 'dev.thriving.oss:kafka-streams-cassandra-state-store:${version}'
```

## Usage
### Quick start

‼️**Important:** Always disable logging + caching (by default kafka streams is buffering writes)  

#### High-level DSL <> StoreSupplier
```java
StreamsBuilder builder = new StreamsBuilder();
KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("queryable-store-name");
KTable<Long,String> table = builder.table(
  "topicName",
  Materialized.<Long,String>as(
                 CassandraStores.builder(session, "store-name")
                         .keyValueStore()
              )
              .withKeySerde(Serdes.Long())
              .withValueSerde(Serdes.String())
              .withLoggingDisabled()
              .withCachingDisabled());
```

#### Processor API <> StoreBuilder
```java
Topology topology = new Topology();
topology.addProcessor("processorName", ...);

Map<String,String> topicConfig = new HashMap<>();
StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                CassandraStores.builder(session, WORD_GROUPED_COUNT_STORE)
                        .stringKeyValueStore(),
                Serdes.String(),
                Serdes.Long())
        .withLoggingDisabled()
        .withCachingDisabled();

topology.addStateStore(storeBuilder, "processorName");
```

### Builder

#### Config Options

### Store Types
TODO: create table with types <> supported operations

#### keyValueStore (recommended default)
#### stringKeyValueStore
#### globalKeyValueStore

### Examples

## Fine Print 

### Known Limitations


## Roadmap

- [x] MVP
  - [x] CQL Schema
  - [x] implementation
- [x] restructure code
  - [x] split implementation & examples
  - [x] Abstract store, introduce Repo, KeySerdes (Byte <> ByteBuffer|String)
  - [x] CassandraStores Builder, configurable
    - [x] table name fn
    - [x] keyspace
    - [x] ~~table default ttl~~
    - [x] ~~compaction strategy~~
    - [x] ~~compression~~
    - [x] fully customizable table options (support both Cassandra & ScyllaDB)
- [x] examples
  - [x] WordCount Cassandra 4
  - [x] WordCount Cassandra 3 (v4 client lib)
  - [x] WordCount ScyllaDB
  - [x] WordCount Processor + all + range + prefixScan
  - [x] GlobalCassandraStore + KStream enrichment 
  - [ ] app as GraalVM native image (Micronaut || Quarkus)
- [x] additional features
  - [x] Prefix scan with `stringKeyValueStore` (ScyllaDB only)
  - [ ] ~~Prefix scan with `stringKeyValueStore` (Cassandra with SASIIndex? https://stackoverflow.com/questions/49247092/order-by-and-like-in-same-cassandra-query/49268543#49268543)~~
  - [x] `globalKeyValueStore`
- [x] OpenSource
  - [x] choose + add license
  - [x] add CHANGELOG.md
  - [x] add CODE_OF_CONDUCT.md
  - [ ] ~~? add CONTRIBUTING.md~~
  - [x] polishing
  - [x] make repo public
  - [x] Publish to maven central (?) https://h4pehl.medium.com/publish-your-gradle-artifacts-to-maven-central-f74a0af085b1
    - [x] request namespace ownership
    - [x] add JavaDocs
    - [x] other -> maven central compliant https://central.sonatype.org/publish/requirements/
    - [x] gradle plugin to publish to maven central https://julien.ponge.org/blog/publishing-from-gradle-to-maven-central-with-github-actions/
    - [x] publish snapshot version 0.1.0-SNAPSHOT
    - [x] add gradle release plugin
    - [x] tag + publish initial version 0.1.0
- [ ] Ops
  - [x] github actions to build (+test)
  - [ ] ? add renovate 
    - https://github.com/renovatebot/github-action
    - https://docs.renovatebot.com/java/
  - [ ] ? add trivy https://github.com/marketplace/actions/trivy-action
  - [ ] ? github actions to publish to maven central https://julien.ponge.org/blog/publishing-from-gradle-to-maven-central-with-github-actions/
- [ ] Write Documentation
  - [x] summary
  - [x] cleanup README
  - [x] install
  - [ ] quick start
  - [ ] overview store types
  - [ ] compatibility cassandra 3.11, 4.x, ScyllaDB
  - [ ] limitations
  - [ ] usage, builder, config options
  - [ ] link to examples
  - [ ] (Caching options)
- [ ] Security
  - [ ] prevent + test against 'CQL injection' via `withTableOptions(..)`
- [ ] tests
  - [ ] unit tests (?)
  - [ ] WordCount integration test using testcontainers
    - Testcontainers for Java
      https://www.testcontainers.org/
    - Cassandra Module - Testcontainers for Java
      https://www.testcontainers.org/modules/databases/cassandra/
    - Cassandra 4 mit Testcontainers in Spring Boot
      https://www.trion.de/news/2022/02/01/cassandra-4-testcontainers.html
    - Kafka Containers - Testcontainers for Java
      https://www.testcontainers.org/modules/kafka/
    - testcontainers-java/settings.gradle at main · testcontainers/testcontainers-java
      https://github.com/testcontainers/testcontainers-java/blob/main/settings.gradle
    - testcontainers-java/examples/kafka-cluster at main · testcontainers/testcontainers-java
      https://github.com/testcontainers/testcontainers-java/tree/main/examples/kafka-cluster
    - testcontainers-java/RedisBackedCacheTest.java at main · testcontainers/testcontainers-java
      https://github.com/testcontainers/testcontainers-java/blob/main/examples/redis-backed-cache/src/test/java/RedisBackedCacheTest.java
- [ ] Features Planned/Considered
  - [ ] (?) simple inMemory read cache -> Caffeine?
  - [ ] add additional store types
    - [ ] WindowedStore functionality, example, ...
    - [ ] ...?
  - [ ] Benchmark

