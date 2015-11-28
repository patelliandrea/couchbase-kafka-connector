/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.kafka;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventHandler;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@link ConnectWriter} is in charge of filtering and routing events to the Kafka cluster.
 *
 * @author Sergey Avseyev
 */
public class ConnectWriter implements EventHandler<DCPEvent> {
    private static final Logger logger = LoggerFactory.getLogger(ConnectWriter.class);
    private final Filter filter;

    private final static ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

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
        if (filter.pass(event)) {

            //do stuff

            if (event.message() instanceof MutationMessage) {
                MutationMessage mutation = (MutationMessage) event.message();
                String message = mutation.content().toString(CharsetUtil.UTF_8);
                logger.trace("message: {}", message);
                queue.add(message);
                mutation.content().release();
            }
        }
    }

    public static Queue<String> getQueue() {
        logger.trace("Static size: {}", queue.size());
        LinkedList<String> tmpQueue = new LinkedList<>(ConnectWriter.queue);
        ConnectWriter.queue.clear();
        logger.trace("Returning size: {}", tmpQueue.size());
        return new LinkedList<>(tmpQueue);
    }
}
