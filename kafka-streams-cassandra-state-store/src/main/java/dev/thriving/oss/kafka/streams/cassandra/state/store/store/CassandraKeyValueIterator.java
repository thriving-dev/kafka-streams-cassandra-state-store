package dev.thriving.oss.kafka.streams.cassandra.state.store.store;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.function.Function;

public class CassandraKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private final Iterator<Row> iter;
    private final Function<Row, Bytes> extractKeyFn;

    public CassandraKeyValueIterator(Iterator<Row> iter, Function<Row, Bytes> extractKeyFn) {
        this.iter = iter;
        this.extractKeyFn = extractKeyFn;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        Row row = iter.next();
        Bytes key = extractKeyFn.apply(row);
        byte[] value = row.getByteBuffer(1).array();
        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Bytes peekNextKey() {
        throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
    }
}
