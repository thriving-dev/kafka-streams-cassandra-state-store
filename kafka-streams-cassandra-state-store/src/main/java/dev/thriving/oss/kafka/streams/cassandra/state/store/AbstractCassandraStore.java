package dev.thriving.oss.kafka.streams.cassandra.state.store;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;

public abstract class AbstractCassandraStore implements CassandraStateStore, StateStore {

    protected final String name;
    protected StateStoreContext context;
    protected int partition;
    protected Position position = Position.emptyPosition();
    protected volatile boolean open = false;

    public AbstractCassandraStore(String name) {
        this.name = name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                    "Use StateStore#init(StateStoreContext, StateStore) instead."
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
}
