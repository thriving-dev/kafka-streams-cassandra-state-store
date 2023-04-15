package dev.thriving.oss.kafka.streams.cassandra.state.store;

import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.CassandraWindowStoreRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

public class CassandraWindowStore implements WindowStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowStore.class);
    public static final int SEQNUM_SIZE = 4;

    private final String name;
    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;
    private final CassandraWindowStoreRepository repo;
    protected final Position position;
    private StateStoreContext stateStoreContext;
    private int partition;
    private long seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private long lastCleanupTime;

    private volatile boolean open = false;

    public CassandraWindowStore(final String name,
                                final long retentionPeriod,
                                final long windowSize,
                                final boolean retainDuplicates,
                                final CassandraWindowStoreRepository repo) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.repo = repo;
        this.position = Position.emptyPosition();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                    "Use CassandraWindowStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.stateStoreContext = context;
        this.partition = context.taskId().partition();
        this.lastCleanupTime = 0;

        if (root != null) {
            // register the store
            context.register(
                    root,
                    (RecordBatchingStateRestoreCallback) records -> { }
            );
        }

        open = true;
    }

    @Override
    public void close() {
        this.open = false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
        // do-nothing
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        LOG.trace("put {}::{}", key, value);
        maybeRemoveExpiredWindows();
        observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            // expiredRecordSensor.record(1.0d, ProcessorContextUtils.currentSystemTime(context));
            LOG.warn("Skipping record for expired segment.");
        } else {
            if (value != null) {
                maybeUpdateSeqnumForDups();
                repo.save(partition, windowStartTimestamp, key, value, seqnum);
                StoreQueryUtils.updatePosition(position, stateStoreContext);
            } else if (!retainDuplicates) {
                // Skip if value is null and duplicates are allowed since this delete is a no-op
                repo.delete(partition, windowStartTimestamp, key, seqnum);
            }
        }

        StoreQueryUtils.updatePosition(position, stateStoreContext);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, true);
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, false);
    }

    private WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo, final boolean forward) {
        Objects.requireNonNull(key, "key cannot be null");

        maybeRemoveExpiredWindows();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return new CassandraWindowStoreIteratorProvider(Collections.emptyIterator(), windowSize).valuesIterator();
        }

        return repo.fetch(partition, key, timeFrom, timeTo, forward, windowSize).valuesIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final long timeFrom, final long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo, final long timeFrom, final long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo, false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final long timeFrom, final long timeTo, final boolean forward) {
        maybeRemoveExpiredWindows();

        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return KeyValueIterators.emptyIterator();
        }

        return repo.fetch(partition, keyFrom, keyTo, timeFrom, timeTo, forward, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo, final boolean forward) {
        maybeRemoveExpiredWindows();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return KeyValueIterators.emptyIterator();
        }

        return repo.fetchAll(partition, timeFrom, timeTo, forward);
    }

    @Override
    public byte[] fetch(final Bytes key, final long windowStartTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");

        maybeRemoveExpiredWindows();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            return null;
        }

        return repo.get(partition, windowStartTimestamp, key, seqnum);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return all(true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return all(false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> all(final boolean forward) {
        maybeRemoveExpiredWindows();

        final long minTime = observedStreamTime - retentionPeriod;

        return repo.getAllWindowsFrom(partition, minTime, forward);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
                query,
                positionBound,
                config,
                this,
                position,
                stateStoreContext
        );
    }

    private void maybeRemoveExpiredWindows() {
        // exec in a buffered fashion, based on currentTimeMillis & retentionPeriod -> keep last time executed % retentionPeriod
        long currentTimeMillis = System.currentTimeMillis();
        long bufferedCleanupTime = currentTimeMillis - (currentTimeMillis % retentionPeriod);

        if (lastCleanupTime < bufferedCleanupTime) {
            long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);
            repo.deleteWindowsOlderThan(partition, minLiveTime);
            lastCleanupTime = bufferedCleanupTime;
        }
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
