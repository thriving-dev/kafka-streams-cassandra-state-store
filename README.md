## kafka-streams-cassandra-state-store

[![Java CI](https://github.com/hartmut-co-uk/kafka-streams-cassandra-state-store/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/hartmut-co-uk/kafka-streams-cassandra-state-store/actions/workflows/build-gradle-project.yml)

TODO: describe purpose


### Stack

#### compiled with

* Java 17
* kafka-streams 3.3.1
* datastax java-driver-core 4.15.0

#### supports / tested with

* kafka-streams 2.7.0+ (maybe even earlier versions, but wasn't tested further back)
* datastax java client (v4) `com.datastax.oss:java-driver-core:4.15.0`
* ScyllaDB shard-aware datastax java client (v4) fork `com.scylladb:java-driver-core:4.14.1.0`
* Apache Cassandra 3.11
* Apache Cassandra 4
* ScyllaDB (should work from 4.3+)

### Roadmap

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
- [x] additional features
  - [x] Prefix scan with `stringKeyValueStore` (ScyllaDB only)
  - [ ] ~~Prefix scan with `stringKeyValueStore` (Cassandra with SASIIndex? https://stackoverflow.com/questions/49247092/order-by-and-like-in-same-cassandra-query/49268543#49268543)~~
  - [x] `globalKeyValueStore`
- [ ] OpenSource
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
- [ ] Documentation
  - [ ] summary
  - [ ] cleanup README
  - [ ] quick start
  - [ ] overview store types
  - [ ] compatibility cassandra 3.11, 4.x, ScyllaDB
  - [ ] limitations
  - [ ] usage, builder, config options
  - [ ] link to examples
  - [ ] (Caching options)
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
  - [ ] add WindowedStore functionality, example, ...

### draft cql schema

```sql
CREATE KEYSPACE IF NOT EXISTS "test" WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE xyz_changelog (
    partition int,
    key BLOB,
    time timestamp,
    value BLOB,
    PRIMARY KEY ((partition), key)
);

-- create only for CassandraTimestampedKeyValueStore  
CREATE MATERIALIZED VIEW xyz_changelog_mv AS 
SELECT * FROM xyz_changelog
WHERE time IS NOT NULL AND key IS NOT NULL
PRIMARY KEY ((partition), time, key)
WITH CLUSTERING ORDER BY (time DESC);
```

### commands

    docker exec -it scylla-1 cqlsh
    docker exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --group kafka-streams-101 --describe
    
    kafka-topics --bootstrap-server=localhost:9092 --create --partitions 6 --replication-factor 1 --topic streams-plaintext-input
    kafka-topics --bootstrap-server=localhost:9092 --create --partitions 6 --replication-factor 1 --topic streams-wordcount-output
    kafka-topics --bootstrap-server=localhost:9092 --list


### commands

    echo "431afee2-4347-4cb3-bb4e-f48daf712f57:" | kcat -b localhost:9092 -t random-strings -Z -K:
    kcat -C -b localhost:9092 -t random-strings -Z -K::


### POC cql schema

```bash
[16:26:19] dev/git/poc » docker exec -it scylla-1 cqlsh                                                                                                                                                                                                                              130 ↵
Connected to  at 172.23.0.3:9042.
[cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.
cqlsh> CREATE TABLE xyz_changelog (
...     key BLOB,
...     partition int,
...     time timestamp,
...     value BLOB,
...     PRIMARY KEY ((partition), key)
... );
InvalidRequest: Error from server: code=2200 [Invalid query] message="No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"
cqlsh> CREATE KEYSPACE IF NOT EXISTS "test" WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
cqlsh> use test;
cqlsh:test> CREATE TABLE xyz_changelog (     key BLOB,     partition int,     time timestamp,     value BLOB,     PRIMARY KEY ((partition), key) );
cqlsh:test> SELECT * FROM xyz_changelog ;

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition=0;

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition=0 AND key=intAsBlob(33);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition=0 AND key=intAsBlob(33);
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=0;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid INTEGER constant (0) for "key" of type blob"
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(33);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> CREATE MATERIALIZED VIEW xyz_changelog_mv AS
...     SELECT key, partition, time, value
...     FROM xyz_changelog
...     WHERE time IS NOT NULL
...     PRIMARY KEY ((partition), time, key)
... );
SyntaxException: line 6:0  : syntax error...

cqlsh:test> CREATE MATERIALIZED VIEW xyz_changelog_mv AS
...     SELECT * FROM xyz_changelog
...     WHERE time IS NOT NULL
...     PRIMARY KEY ((partition), time, key)
...     WITH CLUSTERING ORDER BY (time DESC)
... );
SyntaxException: line 6:0  : syntax error...

cqlsh:test> CREATE MATERIALIZED VIEW xyz_changelog_mv AS
...     SELECT * FROM xyz_changelog
...     WHERE time IS NOT NULL
...     PRIMARY KEY ((partition), time, key)
...     WITH CLUSTERING ORDER BY (time DESC)
... ;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Primary key column 'key' is required to be filtered by 'IS NOT NULL'"
cqlsh:test> CREATE MATERIALIZED VIEW xyz_changelog_mv AS
... SELECT * FROM xyz_changelog
... WHERE time IS NOT NULL AND key IS NOT NULL
... PRIMARY KEY ((partition), time, key)
... WITH CLUSTERING ORDER BY (time DESC);
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (0, intAsBlob(1), 1, intAsBlob(33));
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (0, intAsBlob(2), 2, intAsBlob(33));
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (1, intAsBlob(3), 2, intAsBlob(33));
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (4, intAsBlob(4), 1, intAsBlob(33));
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(33);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021
0 | 0x00000002 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(3 rows)
cqlsh:test> SELECT * FROM xyz_changelog;

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021
0 | 0x00000002 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
4 | 0x00000004 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(4 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(1 rows)
cqlsh:test> SELECT *, blobAsInt(value) FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);
SyntaxException: line 1:8  : syntax error...

cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(1 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN ;
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key>intAsBlob(0);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021
0 | 0x00000002 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(3 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key>intAsBlob(1);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000002 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(2 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key>=intAsBlob(1);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 | 0x00000021
0 | 0x00000002 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(3 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key>=intAsBlob(10);

partition | key | time | value
-----------+-----+------+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key>intAsBlob(2);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(1 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE key>intAsBlob(2);

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
4 | 0x00000004 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(2 rows)

Warnings :
This query should use ALLOW FILTERING and will be rejected in future versions.

cqlsh:test> SELECT * FROM xyz_changelog WHERE key>intAsBlob(2) ALLOW FILTERING;

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
4 | 0x00000004 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(2 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE key>intAsBlob(2) AND key<intAsBlob(5) ALLOW FILTERING;

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021
4 | 0x00000004 | 1970-01-01 00:00:00.001000+0000 | 0x00000021

(2 rows)
cqlsh:test> SELECT * FROM xyz_changelog WHERE key>intAsBlob(2) AND key<intAsBlob(4) ALLOW FILTERING;

partition | key        | time                            | value
-----------+------------+---------------------------------+------------
1 | 0x00000003 | 1970-01-01 00:00:00.002000+0000 | 0x00000021

(1 rows)
cqlsh:test> SELECT * FROM xyz_changelog_mv;

partition | time                            | key        | value
-----------+---------------------------------+------------+------------
1 | 1970-01-01 00:00:00.002000+0000 | 0x00000003 | 0x00000021
0 | 1970-01-01 00:00:00.002000+0000 | 0x00000002 | 0x00000021
0 | 1970-01-01 00:00:00.001000+0000 | 0x00000001 | 0x00000021
4 | 1970-01-01 00:00:00.001000+0000 | 0x00000004 | 0x00000021

(4 rows)
cqlsh:test> SELECT * FROM xyz_changelog_mv WHERE partition IN (0,1,2,3,4) AND time <1;;

partition | time | key | value
-----------+------+-----+-------

(0 rows)
SyntaxException: line 1:0 no viable alternative at input ';'
cqlsh:test> SELECT * FROM xyz_changelog_mv WHERE partition IN (0,1,2,3,4) AND time<1;

partition | time | key | value
-----------+------+-----+-------

(0 rows)
cqlsh:test> SELECT * FROM xyz_changelog_mv WHERE partition IN (0,1,2,3,4) AND time<=1;

partition | time                            | key        | value
-----------+---------------------------------+------------+------------
0 | 1970-01-01 00:00:00.001000+0000 | 0x00000001 | 0x00000021
4 | 1970-01-01 00:00:00.001000+0000 | 0x00000004 | 0x00000021

(2 rows)
cqlsh:test> SELECT * FROM xyz_changelog_mv WHERE partition IN (0,1,2,3,4) AND time<2;

partition | time                            | key        | value
-----------+---------------------------------+------------+------------
0 | 1970-01-01 00:00:00.001000+0000 | 0x00000001 | 0x00000021
4 | 1970-01-01 00:00:00.001000+0000 | 0x00000004 | 0x00000021

(2 rows)
cqlsh:test> SELECT * FROM xyz_changelog_mv WHERE partition IN (0,1,2,3,4) AND time>1;

partition | time                            | key        | value
-----------+---------------------------------+------------+------------
0 | 1970-01-01 00:00:00.002000+0000 | 0x00000002 | 0x00000021
1 | 1970-01-01 00:00:00.002000+0000 | 0x00000003 | 0x00000021

(2 rows)
cqlsh:test> SELECT blobAsInt(value) FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);

system.blobasint(value)
-------------------------
                      33

(1 rows)
cqlsh:test> SELECT partition, key, time, blobAsInt(value) AS value_int FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);

partition | key        | time                            | value_int
-----------+------------+---------------------------------+-----------
0 | 0x00000001 | 1970-01-01 00:00:00.001000+0000 |        33

(1 rows)
cqlsh:test> SELECT partition, blobAsInt(key) AS key_int, time, blobAsInt(value) AS value_int FROM xyz_changelog WHERE partition IN (0, 1, 2) AND key=intAsBlob(1);

partition | key_int | time                            | value_int
-----------+---------+---------------------------------+-----------
0 |       1 | 1970-01-01 00:00:00.001000+0000 |        33

(1 rows)
cqlsh:test> SELECT partition, blobAsInt(key) AS key_int, time, blobAsInt(value) AS value_int FROM xyz_changelog;

partition | key_int | time                            | value_int
-----------+---------+---------------------------------+-----------
1 |       3 | 1970-01-01 00:00:00.002000+0000 |        33
0 |       1 | 1970-01-01 00:00:00.001000+0000 |        33
0 |       2 | 1970-01-01 00:00:00.002000+0000 |        33
4 |       4 | 1970-01-01 00:00:00.001000+0000 |        33

(4 rows)
cqlsh:test> SELECT partition, blobAsInt(key) AS key_int, time, blobAsInt(value) AS value_int FROM xyz_changelog_mv;

partition | key_int | time                            | value_int
-----------+---------+---------------------------------+-----------
1 |       3 | 1970-01-01 00:00:00.002000+0000 |        33
0 |       2 | 1970-01-01 00:00:00.002000+0000 |        33
0 |       1 | 1970-01-01 00:00:00.001000+0000 |        33
4 |       4 | 1970-01-01 00:00:00.001000+0000 |        33

(4 rows)
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (0, intAsBlob(5), 5, intAsBlob(33));
cqlsh:test> INSERT INTO xyz_changelog (partition, key, time, value) VALUES (0, intAsBlob(6), 0, intAsBlob(33));
cqlsh:test> SELECT partition, blobAsInt(key) AS key_int, time, blobAsInt(value) AS value_int FROM xyz_changelog_mv;

partition | key_int | time                            | value_int
-----------+---------+---------------------------------+-----------
1 |       3 | 1970-01-01 00:00:00.002000+0000 |        33
0 |       5 | 1970-01-01 00:00:00.005000+0000 |        33
0 |       2 | 1970-01-01 00:00:00.002000+0000 |        33
0 |       1 | 1970-01-01 00:00:00.001000+0000 |        33
0 |       6 | 1970-01-01 00:00:00.000000+0000 |        33
4 |       4 | 1970-01-01 00:00:00.001000+0000 |        33

(6 rows)
cqlsh:test>
```

```sql

CREATE TABLE xyz_global_changelog2 (
    key BLOB,
    time timestamp,
    value BLOB,
    PRIMARY KEY (key)
);
SELECT * FROM xyz_changelog2
WHERE key=intAsBlob(1);
SELECT * FROM xyz_changelog2
WHERE key>=intAsBlob(1);
```



```java
    /**
     * CQL compaction strategy to be used when first creating the state store cassandra table.
     * <p>
     * Please note this config will only apply upon initial table creation. ('ALTER TABLE' is not yet supported).
     * <p>
     * Default compaction strategy used is 'LeveledCompactionStrategy'
     *
     * @param compactionStrategy the compaction strategy for the cassandra table (cannot be {@code null} or blank)
     * @return itself
     */
    public CassandraStores withCompactionStrategy(String compactionStrategy) {
        assert compactionStrategy != null && !compactionStrategy.isBlank() : "compactionStrategy cannot be null or blank";
        this.compactionStrategy = compactionStrategy;
        return this;
    }

    /**
     * The default expiration time ("TTL") in seconds for the state store cassandra table.
     * <p>
     * Please note this config will only apply upon initial table creation. ('ALTER TABLE' is not yet supported).
     * <p>
     * Default TTL: 0 (data will not expire)
     *
     * @param defaultTtlSeconds the default expiration time ("TTL") in seconds for the table (cannot be <0)
     * @return itself
     */
    public CassandraStores withDefaultTtlSeconds(long defaultTtlSeconds) {
        assert defaultTtlSeconds >= 0 : "defaultTtlSeconds cannot be null and must be >= 0";
        this.defaultTtlSeconds = defaultTtlSeconds;
        return this;
    }
```
