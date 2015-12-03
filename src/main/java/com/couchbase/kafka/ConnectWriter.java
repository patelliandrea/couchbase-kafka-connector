package com.couchbase.kafka;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventHandler;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.filter.Filter;
import javafx.util.Pair;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@link ConnectWriter} is in charge of filtering and routing events to the Kafka cluster.
 *
 * @author Andrea Patelli
 */
public class ConnectWriter implements EventHandler<DCPEvent> {
    private final Filter filter;

    public final static Object semaphore = new Object();

    private final static ConcurrentLinkedQueue<Pair<String, Short>> queue = new ConcurrentLinkedQueue<>();

    /**
     * Creates a new {@link ConnectWriter}.
     *
     * @param filter the filter to select events to publish.
     */
    public ConnectWriter(final Filter filter) {
        this.filter = filter;
    }

    /**
     * Handles {@link DCPEvent}s that come into the response RingBuffer.
     */
    @Override
    public void onEvent(final DCPEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        synchronized (queue) {
            if (filter.pass(event)) {
                if (event.message() instanceof MutationMessage) {
                    MutationMessage mutation = (MutationMessage) event.message();
                    String message = mutation.content().toString(CharsetUtil.UTF_8);
                    queue.add(new Pair<>(message, ((MutationMessage) event.message()).partition()));
                    mutation.content().release();
                    semaphore.notifyAll();
                }
            }
        }
    }

    /**
     * Method used to get a copy of the current queue of messages read from Couchbase.
     *
     * @return a copy of the queue
     */
    public static Queue<Pair<String, Short>> getQueue() {
        Queue<Pair<String, Short>> tmpQueue;
        synchronized (queue) {
            tmpQueue = new LinkedList<>(queue);
            queue.clear();
        }
        return new LinkedList<>(tmpQueue);
    }
}
