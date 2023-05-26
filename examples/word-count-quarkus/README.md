# kafka-streams-cassandra-state-store/examples
## word-count-quarkus

Features the notorious 'word-count example', written as a quarkus application, fully fledged localstack demo with docker-compose.

The docker-compose stack features
- 3 node KRaft Kafka cluster
- 3 node ScyllaDB cluster
- 3 replicas of the kafka-streams quarkus app
- pre-init of 
  - kafka topics (p=12, r=3), via debezium `CREATE_TOPICS` ENV var 
  - cassandra schema (keyspace), via 'init container'

## quick start (native executable)

**Important: run from the directory of this README.md !!**

### build

Instructions for building as a native executable from Mac on Apple Silicon.   
_Note: for other OS / JVM, see [quarkus](#quarkus) section below..._

On Mac with Apple Silicon (M1/M2 chip) use:
```shell script
../../gradlew clean build -Dquarkus.package.type=native -Dquarkus.native.container-build=true  -Dquarkus.native.builder-image=quay.io/quarkus/ubi-quarkus-mandrel-builder-image:22.3-java17 -Dquarkus.native.resources.includes="librocksdbjni-linux-aarch64.so" -Dquarkus.native.resources.excludes="librocksdbjni-linux64.so"
```

### docker-compose stack

Start docker-compose stack
```bash
export QUARKUS_MODE=native
docker-compose up --build -d
```

### Play with the application

2. Produce some messages, e.g. via kcat to the input topic
```bash
echo "Hello world" | kcat -P -b localhost:19092 -t streams-plaintext-input
echo "What a wonderful world" | kcat -P -b localhost:19092 -t streams-plaintext-input
echo "What a day to say hello" | kcat -P -b localhost:19092 -t streams-plaintext-input
```

3. Start a console-consumer on the output topic
```bash
kcat -C -q -b localhost:19092 -t streams-wordcount-output -K:: -s key=s -s value=q
```

4. Query the state from the Scylla cluster
```bash
docker exec -it scylla-1 cqlsh \
    -k test \
    -e "SELECT partition, blobAsText(key) as key, time, value FROM word_grouped_count_kstreams_store;"
```

### (Cleanup)

Remove docker-compose stack
```bash
docker-compose down
```


## Quarkus

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

[//]: # (### Running the application in dev mode <<-- unfortunately not working!!) 

[//]: # ()
[//]: # (You can run your application in dev mode that enables live coding using:)

[//]: # (```shell script)

[//]: # (../../gradlew quarkusDev)

[//]: # (```)

[//]: # ()
[//]: # (> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.)

### Packaging and running the application

The application can be packaged using:
```shell script
../../gradlew clean build
```
It produces the `quarkus-run.jar` file in the `build/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `build/quarkus-app/lib/` directory.

The application is now runnable using `java -jar build/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
../../gradlew build -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar build/*-runner.jar`.

### Creating a native executable

You can create a native executable using: 
```shell script
../../gradlew build -Dquarkus.package.type=native
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:
```shell script
../../gradlew build -Dquarkus.package.type=native -Dquarkus.native.container-build=true
```

On Mac with Apple Silicon (M1/M2 chip) use:
```shell script
../../gradlew clean build -Dquarkus.package.type=native -Dquarkus.native.container-build=true  -Dquarkus.native.builder-image=quay.io/quarkus/ubi-quarkus-mandrel-builder-image:22.3-java17 -Dquarkus.native.resources.includes="librocksdbjni-linux-aarch64.so" -Dquarkus.native.resources.excludes="librocksdbjni-linux64.so"
```

You can then execute your native executable with: `./build/word-count-quarkus-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/gradle-tooling.

### Related Guides

- Apache Kafka Streams ([guide](https://quarkus.io/guides/kafka-streams)): Implement stream processing applications based on Apache Kafka


## Other

### Advanded debugging

Produce/consumer without kcat
```bash
docker exec -it kafka-1  bin/kafka-console-producer.sh --bootstrap-server=localhost:9092 --topic streams-plaintext-input
docker exec -it kafka-1 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic streams-wordcount-output \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

List Kafka topics, directly from container
```bash
docker exec -it kafka-1  bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list
```

cqlsh
```bash
docker exec -it scylla-1 cqlsh -k test
docker exec -it scylla-1 cqlsh -k test -e "DESC TABLES;"
docker exec -it scylla-1 cqlsh -k test -e "SELECT * FROM word_grouped_count_kstreams_store;"
docker exec -it scylla-1 cqlsh -k test -e "SELECT partition, blobAsText(key) as key, time, value FROM word_grouped_count_kstreams_store;"
```
