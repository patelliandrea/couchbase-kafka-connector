package com.couchbase.kafka;

import com.couchbase.client.core.message.dcp.MutationMessage;
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
public class ConnectWriter {
    private final static Logger log = LoggerFactory.getLogger(ConnectWriter.class);
    private final Filter filter;

    private final static ConcurrentLinkedQueue<MutationMessage> queue = new ConcurrentLinkedQueue<>();

    public final static Object sync = new Object();

    /**
     * Creates a new {@link ConnectWriter}.
     *
     * @param filter the filter to select events to publish.
     */
    public ConnectWriter(final Filter filter) {
        this.filter = filter;
    }

    public void addToQueue(final DCPEvent event) {
        synchronized (sync) {
            // if the event passes the filter, the message is added to a queue
            if (filter.pass(event)) {
                MutationMessage mutation = (MutationMessage) event.message();
                queue.add(mutation);
            }
        }
    }

    /**
     * Method used to get a copy of the current queue of messages read from Couchbase.
     *
     * @return a copy of the queue
     */
    public static Queue<MutationMessage> getQueue() {
        synchronized (sync) {
            Queue<MutationMessage> tmpQueue;
            tmpQueue = new LinkedList<>();
            while(!queue.isEmpty()) {
                tmpQueue.add(queue.poll());
            }
            return new LinkedList<>(tmpQueue);
        }
    }

    public static int queueSize() {
        return queue.size();
    }
}
