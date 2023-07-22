package dev.thriving.oss.kafka.streams.cassandra.state.store.example.processor.api.misc;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class WordCountProcessor implements Processor<String, String, String, Long> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountProcessor.class);

    private ProcessorContext<String, Long> context;
    private KeyValueStore<String, Long> store;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        this.store = context.getStateStore(ProcessorApiAllRangePrefixCountDemo.WORD_GROUPED_COUNT_STORE);

        LOG.info("Processor init for task: {}", context.taskId());

        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::prefixScan);
        context.schedule(Duration.ofSeconds(12), PunctuationType.WALL_CLOCK_TIME, this::rangeFrom);
        context.schedule(Duration.ofSeconds(17), PunctuationType.WALL_CLOCK_TIME, this::approximateNumEntries);
        context.schedule(Duration.ofSeconds(21), PunctuationType.WALL_CLOCK_TIME, this::all);
    }

    @Override
    public void process(Record<String, String> record) {
        // read current
        final Long currentCount = store.get(record.key());

        // increment or init and store
        Long count = currentCount != null ? currentCount + 1 : 1L;

        // persist new value
        store.put(record.key(), count);

        // emit new record
        context.forward(new Record<>(record.key(), count, record.timestamp(), record.headers()));
    }

    private void prefixScan(long timestamp) {
        try (KeyValueIterator<String, Long> iter = store.prefixScan("bel", Serdes.String().serializer())) {
            iter.forEachRemaining(kv -> LOG.info("[{}] prefixScan('bel') -> {}::{}", context.taskId(), kv.key, kv.value));
        }
    }

    private void approximateNumEntries(long timestamp) {
        long count = store.approximateNumEntries();
        LOG.info("[{}] approximateNumEntries() -> {}", context.taskId(), count);
    }

    private void rangeFrom(long timestamp) {
        try (KeyValueIterator<String, Long> iter = store.range("netherlands", "romania")) {
            iter.forEachRemaining(kv -> LOG.info("[{}] range('netherlands', 'romania') -> {}::{}", context.taskId(), kv.key, kv.value));
        }
    }

    private void all(long timestamp) {
        try (KeyValueIterator<String, Long> iter = store.all()) {
            iter.forEachRemaining(kv -> LOG.info("[{}] all() -> {}::{}", context.taskId(), kv.key, kv.value));
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
