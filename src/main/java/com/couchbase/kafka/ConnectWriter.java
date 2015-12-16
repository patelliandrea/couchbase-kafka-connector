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
    private static Integer batchSize;

    private final static ConcurrentLinkedQueue<Pair<String, Short>> queue = new ConcurrentLinkedQueue<>();

    public final static Object sync = new Object();

    /**
     * Creates a new {@link ConnectWriter}.
     *
     * @param filter the filter to select events to publish.
     */
    public ConnectWriter(final Filter filter, Integer batchSize) {
        this.filter = filter;
        this.batchSize = batchSize;
    }

    public synchronized void addToQueue(final DCPEvent event) {
        // if the event passes the filter, the message is added to a queue
        if (filter.pass(event)) {
                MutationMessage mutation = (MutationMessage) event.message();
                String message = new String(mutation.content().toString(CharsetUtil.UTF_8));
                queue.add(new Pair<>(message, ((MutationMessage) event.message()).partition()));
                mutation.content().release();
        } else if(event.message() instanceof MutationMessage) {
            MutationMessage mutation = (MutationMessage) event.message();
            mutation.content().release();
        }
    }

    /**
     * Method used to get a copy of the current queue of messages read from Couchbase.
     *
     * @return a copy of the queue
     */
    public synchronized static Queue<Pair<String, Short>> getQueue() {
        Queue<Pair<String, Short>> tmpQueue;
            tmpQueue = new LinkedList<>();
            for (int i = 0; i < batchSize && !queue.isEmpty(); i++)
                tmpQueue.add(queue.poll());
        return new LinkedList<>(tmpQueue);
    }
}
