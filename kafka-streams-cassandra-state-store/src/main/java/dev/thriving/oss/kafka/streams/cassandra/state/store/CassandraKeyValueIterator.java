package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

/**
 * Implements {@link KeyValueIterator} wrapping cassandra java client query
 * {@link com.datastax.oss.driver.api.core.cql.ResultSet} iterator -> {@link Iterator<Row>}.
 */
public class CassandraKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private final Iterator<Row> iter;

    /**
     * Constructor for wrapping a cassandra java client query
     * {@link com.datastax.oss.driver.api.core.cql.ResultSet} iterator {@link Iterator<Row>}.
     *
     * @param iter typically the iterator from a cassandra query {@link com.datastax.oss.driver.api.core.cql.ResultSet}
     */
    public CassandraKeyValueIterator(Iterator<Row> iter) {
        this.iter = iter;
    }

    public static KeyValueIterator<Bytes, byte[]> emptyIterator() {
        return new CassandraKeyValueIterator(Collections.emptyIterator());
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        Row row = iter.next();
        Bytes key = Bytes.wrap(row.getByteBuffer("key").array());
        ByteBuffer byteBuffer = row.getByteBuffer("value");
        byte[] value = byteBuffer == null ? null : byteBuffer.array();
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
