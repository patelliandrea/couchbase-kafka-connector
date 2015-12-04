package com.couchbase.kafka;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventHandler;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.filter.Filter;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@link ConnectWriter} is in charge of filtering and routing events to the Kafka cluster.
 *
 * @author Andrea Patelli
 */
public class ConnectWriter implements EventHandler<DCPEvent> {
    private final static Logger log = LoggerFactory.getLogger(ConnectWriter.class);
    private final Filter filter;

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
        // if the event passes the filter, the message is added to a queue
        if (filter.pass(event)) {
            if (event.message() instanceof MutationMessage) {
                synchronized (ConnectWriter.class) {
                    MutationMessage mutation = (MutationMessage) event.message();
                    String message = mutation.content().toString(CharsetUtil.UTF_8);
                    queue.add(new Pair<>(message, ((MutationMessage) event.message()).partition()));
                    mutation.content().release();
                    ConnectWriter.class.notifyAll();
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
        synchronized (ConnectWriter.class) {
            tmpQueue = new LinkedList<>();
            for(int i = 0; i < 100 && !queue.isEmpty(); i++)
                tmpQueue.add(queue.poll());
//            tmpQueue = new LinkedList<>(queue);
//            queue.clear();
        }
        return new LinkedList<>(tmpQueue);
    }

    public static int queueSize() {
        synchronized (ConnectWriter.class) {
            return queue.size();
        }
    }
}
