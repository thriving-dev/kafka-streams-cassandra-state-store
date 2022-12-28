Source: https://docs.confluent.io/5.5.1/streams/quickstart.html

### TODOs

- [ ] configurable via env vars
- [ ] docker-compose creates cql keyspace
- [ ] docker-compose creates kafka topics

### howto

    kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic streams-wordcount-output \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
