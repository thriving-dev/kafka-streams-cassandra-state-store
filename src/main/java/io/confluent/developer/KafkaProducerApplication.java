package io.confluent.developer;


import com.github.javafaker.Faker;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class KafkaProducerApplication {

    private static final String CLIENT_ID = "datagen";
    private final Logger logger = LoggerFactory.getLogger(KafkaProducerApplication.class);

    private final Producer<String, String> producer;
    final String outTopic;

    private boolean closed;

    public KafkaProducerApplication(final Producer<String, String> producer,
                                    final String topic) {
        this.closed = false;
        this.producer = producer;
        this.outTopic = topic;
    }

    public Future<RecordMetadata> produceRandom() {
        Faker faker = new Faker();
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                this.outTopic,
                UUID.randomUUID().toString(),
                faker.chuckNorris().fact());
        return producer.send(producerRecord);
    }

    private void start() {
        logger.info("Kafka Random Data Producer App Started");
        while (!closed) {
            try {
                logger.info("produce new batch of random string messages...");
                for (int i = 0; i < 50; i++) {
                    produceRandom().get();
                }
                Thread.sleep(500);
            } catch (Exception ex) {
                logger.error(ex.toString());
            }
        }
    }

    public void shutdown() {
        closed = true;
        producer.close(Duration.ofSeconds(5));
    }

    public void createTopics(final Properties props, List<String> topics) {
        try (final AdminClient client = AdminClient.create(props)) {
            logger.info("Creating topics: {}", topics);

            final int numPartitions = Integer.parseInt(props.getProperty("topics.num-partitions"));
            final short replicationFactor = Short.parseShort(props.getProperty("topics.replication-factor"));

            final List<NewTopic> newTopics = topics.stream()
                    .map(topic -> new NewTopic(topic, numPartitions, replicationFactor))
                    .collect(Collectors.toList());


            client.createTopics(newTopics).values().forEach((topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    logger.info(ex.toString());
                }
            });

            //logTopics(topics, client);
        }
    }

    private void logTopics(List<String> topics, AdminClient client) throws InterruptedException, ExecutionException, TimeoutException {
        logger.info("Asking cluster for topic descriptions");
        client
                .describeTopics(topics)
                .all() // allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .forEach((name, description) -> logger.info("Topic Description: {}", description.toString()));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        Properties props = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
            props.load(inputStream);
        }
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

        final String topic = props.getProperty("input.topic.name");


        final Producer<String, String> producer = new KafkaProducer<>(props);
        final KafkaProducerApplication producerApp = new KafkaProducerApplication(producer, topic);

        producerApp.createTopics(props, Arrays.asList(topic));

        try {
            producerApp.start();
        }
        finally {
            producerApp.shutdown();
        }
    }
}