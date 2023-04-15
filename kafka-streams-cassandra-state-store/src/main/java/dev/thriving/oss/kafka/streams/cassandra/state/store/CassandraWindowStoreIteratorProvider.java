package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Iterator;

public class CassandraWindowStoreIteratorProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowStoreIteratorProvider.class);

    private final Iterator<Row> iter;
    private final long windowSize;

    public CassandraWindowStoreIteratorProvider(Iterator<Row> iter,
                                                final long windowSize) {
        this.iter = iter;
        this.windowSize = windowSize;
    }

    public WindowStoreIterator<byte[]> valuesIterator() {
        return new WrappedWindowStoreIterator(iter);
    }

    public KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator() {
        return new WrappedKeyValueIterator(iter, windowSize);
    }

    private static class WrappedWindowStoreIterator implements WindowStoreIterator<byte[]> {
        private final Iterator<Row> iter;

        WrappedWindowStoreIterator(final Iterator<Row> iter) {
            this.iter = iter;
        }

        @Override
        public Long peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            Row row = iter.next();

            ByteBuffer byteBuffer = row.getByteBuffer("value");
            byte[] value = byteBuffer == null ? null : byteBuffer.array();

            Instant timestamp = row.getInstant("time");
            final long timestampMs = timestamp.toEpochMilli();
            return KeyValue.pair(timestampMs, value);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static class WrappedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        private final Iterator<Row> iter;
        private final long windowSize;

        WrappedKeyValueIterator(final Iterator<Row> iter,
                                final long windowSize) {
            this.iter = iter;
            this.windowSize = windowSize;
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            Row row = iter.next();

            Bytes key = Bytes.wrap(row.getByteBuffer("key").array());

            ByteBuffer byteBuffer = row.getByteBuffer("value");
            byte[] value = byteBuffer == null ? null : byteBuffer.array();

            long startTime = row.getLong("start_time");
            TimeWindow timeWindow = timeWindowForSize(startTime);

            return KeyValue.pair(new Windowed<>(key, timeWindow), value);
        }

        @Override
        public void close() {
            // do nothing
        }

        /**
         * Safely construct a time window of the given size,
         * taking care of bounding endMs to Long.MAX_VALUE if necessary
         */
        private TimeWindow timeWindowForSize(final long startMs) {
            long endMs = startMs + windowSize;

            if (endMs < 0) {
                LOG.warn("Warning: window end time was truncated to Long.MAX");
                endMs = Long.MAX_VALUE;
            }
            return new TimeWindow(startMs, endMs);
        }
    }
}
