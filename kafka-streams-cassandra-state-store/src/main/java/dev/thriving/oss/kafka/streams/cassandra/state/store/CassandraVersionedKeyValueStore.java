package dev.thriving.oss.kafka.streams.cassandra.state.store;

import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.CassandraVersionedKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.VersionedEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class CassandraVersionedKeyValueStore implements CassandraStateStore, VersionedKeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraVersionedKeyValueStore.class);

    protected final String name;
    private final CassandraVersionedKeyValueStoreRepository repo;
    private final long historyRetention;
    private final long gracePeriod;
    protected StateStoreContext context;
    protected int partition;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    protected Position position = Position.emptyPosition();
    private volatile boolean open = false;

    public CassandraVersionedKeyValueStore(String name,
                                           CassandraVersionedKeyValueStoreRepository repo,
                                           long historyRetentionMs) {
        this.name = name;
        this.repo = repo;
        this.historyRetention = historyRetentionMs;
        // history retention doubles as grace period for now. could be nice to allow users to
        // configure the two separately in the future. if/when we do, we should enforce that
        // history retention >= grace period, for sound semantics.
        this.gracePeriod = historyRetentionMs;

    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                    "Use CassandraVersionedKeyValueStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        this.partition = context.taskId().partition();

        if (root != null) {
            // register the store
            context.register(
                    root,
                    (RecordBatchingStateRestoreCallback) records -> {
                    }
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
    public long put(Bytes key, byte[] value, long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        // validate time is within the retention window + gracePeriod
        if (timestamp < observedStreamTime - gracePeriod) {
            LOG.warn("Skipping record for expired put.");
            return PUT_RETURN_CODE_NOT_PUT;
        }
        observedStreamTime = Math.max(observedStreamTime, timestamp);

        VersionedEntry persistedEntry = doPut(key, value, timestamp, false);
        return persistedEntry.validTo().toEpochMilli();
    }

    private VersionedEntry doPut(Bytes key, byte[] value, long timestamp, boolean returnPrevious) {
        // step 1: cleanup expired records
        repo.cleanup(partition, key, Instant.ofEpochMilli(observedStreamTime - gracePeriod));

        // step 2: get current latest value
        final VersionedEntry currentLatestEntry = repo.get(partition, key);

        VersionedEntry newEntry = new VersionedEntry(value,
                Instant.now(),
                Instant.ofEpochMilli(timestamp),
                Instant.ofEpochMilli(PUT_RETURN_CODE_VALID_TO_UNDEFINED));
        VersionedEntry patchedEntry = null;
        // step 3: check if new record is after current latest
        if (currentLatestEntry == null) {
            // nothing to do here...
        } else if (Instant.ofEpochMilli(timestamp).isAfter(currentLatestEntry.validFrom().minusMillis(1))) {
            // persist the new record as the new latest
            // step 4: (logical) update prev. record & insert new record
            //          (actual cql) upsert current (update value+validFrom) - insert the prev. record as new row
            patchedEntry = new VersionedEntry(currentLatestEntry.value(),
                    currentLatestEntry.timestamp(),
                    currentLatestEntry.validFrom(),
                    Instant.ofEpochMilli(timestamp));
        } else {
            // persist the new record as a point-in-time entry
            // step 4: fetch record `asOfTimestamp`
            final VersionedEntry asOfTimestampEntry = repo.get(partition, key, Instant.ofEpochMilli(timestamp));
            if (asOfTimestampEntry == null) {
                // step 5a: no prev. non-latest record, insert new
                newEntry = new VersionedEntry(value,
                        Instant.now(),
                        Instant.ofEpochMilli(timestamp),
                        currentLatestEntry.validFrom());
            } else if (asOfTimestampEntry.validFrom().isAfter(Instant.ofEpochMilli(timestamp - 1))) {
                // step 5b: prev. non-latest record, validFrom after new timestamp
                newEntry = new VersionedEntry(value,
                        Instant.now(),
                        Instant.ofEpochMilli(timestamp),
                        asOfTimestampEntry.validFrom());
            } else {
                // step 5c: update prev. record & insert new record
                newEntry = new VersionedEntry(value,
                        Instant.now(),
                        Instant.ofEpochMilli(timestamp),
                        asOfTimestampEntry.validTo());
                patchedEntry = new VersionedEntry(asOfTimestampEntry.value(),
                        asOfTimestampEntry.timestamp(),
                        asOfTimestampEntry.validFrom(),
                        Instant.ofEpochMilli(timestamp));
            }
        }

        // save
        List<VersionedEntry> entries = Stream.of(
                newEntry,
                patchedEntry
        ).filter(Objects::nonNull).toList();
        repo.saveInBatch(partition,
            key,
            entries
        );
        return returnPrevious ? patchedEntry : newEntry;
    }

    @Override
    public VersionedRecord<byte[]> delete(Bytes key, long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        // step 1: validate time is within the retention window + gracePeriod
        if (timestamp < observedStreamTime - gracePeriod) {
            LOG.warn("Skipping record for expired delete.");
            return null;
        }

        observedStreamTime = Math.max(observedStreamTime, timestamp);
        VersionedEntry previousEntry = doPut(
                key,
                null,
                timestamp,
                true
        );

        return toVersionedRecord(previousEntry);
    }

    @Override
    public VersionedRecord<byte[]> get(Bytes key) {
        LOG.trace("get {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        // query latest record (if present)
        final VersionedEntry entry = repo.get(partition, key);
        if (entry != null) {
            return toVersionedRecord(entry);
        } else {
            return null;
        }
    }

    @Override
    public VersionedRecord<byte[]> get(Bytes key, long asOfTimestamp) {
        LOG.trace("get {} as of {}", key, asOfTimestamp);
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        if (asOfTimestamp < observedStreamTime - historyRetention) {
            // history retention exceeded. we still check the latest value store in case the
            // latest record version satisfies the timestamp bound, in which case it should
            // still be returned (i.e., the latest record version per key never expires).
            final VersionedEntry entry = repo.get(partition, key);
            if (entry != null) {
                final long latestTimestamp = entry.validFrom().toEpochMilli();
                if (latestTimestamp <= asOfTimestamp) {
                    return toVersionedRecord(entry);
                }
            }

            // history retention has elapsed and the latest record version (if present) does
            // not satisfy the timestamp bound. return null for predictability, even if data
            // is still present in segments.
            LOG.warn("Returning null for expired get.");
            return null;
        }

        // fetch latest value and check if it's a valid choice
        final VersionedEntry currentLatestEntry = repo.get(partition, key);
        if (currentLatestEntry == null) {
            return null;
        } else if (currentLatestEntry.validFrom().isBefore(Instant.ofEpochMilli(asOfTimestamp + 1))) {
            return toVersionedRecord(currentLatestEntry);
        }

        // query point-in-time record (if present)
        final VersionedEntry entry = repo.get(partition, key, Instant.ofEpochMilli(asOfTimestamp));
        if (entry != null && entry.validFrom().toEpochMilli() <= asOfTimestamp) {
            return toVersionedRecord(entry);
        } else {
            return null;
        }
    }

    private static VersionedRecord<byte[]> toVersionedRecord(VersionedEntry entry) {
        if (entry == null || entry.value() == null) {
            return null;
        }
        return new VersionedRecord<>(
                entry.value(),
                entry.validFrom().toEpochMilli()
        );
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }
}
