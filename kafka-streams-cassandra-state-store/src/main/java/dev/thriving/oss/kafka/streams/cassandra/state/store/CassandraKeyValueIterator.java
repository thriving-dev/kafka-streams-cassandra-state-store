package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.cql.Row;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.CassandraKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Implements {@link KeyValueIterator} wrapping cassandra java client query
 * {@link com.datastax.oss.driver.api.core.cql.ResultSet} iterator -> {@link Iterator<Row>}.
 */
public class CassandraKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private final Iterator<Row> iter;
    private final Function<Row, Bytes> extractKeyFn;

    /**
     * Constructor for wrapping a cassandra java client query
     * {@link com.datastax.oss.driver.api.core.cql.ResultSet} iterator {@link Iterator<Row>}.
     * <p>
     * Since {@link CassandraKeyValueStore} / {@link CassandraKeyValueStoreRepository}
     *  * implementations exist with different CQL native types for 'key' column (BLOB|TEXT), the iterator must get the
     *  * appropriate type from the {@link Row} iterated.
     *
     * @param iter typically the iterator from a cassandra query {@link com.datastax.oss.driver.api.core.cql.ResultSet}
     * @param extractKeyFn function to get the key as {@link Bytes} from a {@link Row}
     */
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
        ByteBuffer byteBuffer = row.getByteBuffer(1);
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
