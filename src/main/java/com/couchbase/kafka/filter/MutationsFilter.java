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

package com.couchbase.kafka.filter;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.kafka.DCPEvent;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link MutationsFilter} allows only mutations to be sent to Kafka.
 *
 * @author Sergey Avseyev
 */
public class MutationsFilter implements Filter {
    private final static Map<Short, Long> toCommit = new HashMap<>(0);
    private SourceTaskContext context;
    private static Boolean ready = Boolean.FALSE;

    /**
     * Returns true if event is mutation.
     *
     * @param dcpEvent event object from Couchbase.
     * @return true if event is mutation.
     */
    public boolean pass(final DCPEvent dcpEvent) {
        if(dcpEvent.message() instanceof MutationMessage) {
            // partition of the message
            Short partition = ((MutationMessage)dcpEvent.message()).partition();
            // count of messages already been read from the partition
            Long count = MutationsFilter.toCommit.get(partition);
            // if the count is null, it's the first message to commit
            count = count == null ? 1 : count;
            // counter of the messages for this partition already written to kafka
            Long position = new Long(0);
            Map<String, Object> offsets = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));
            // if the map is null, no offsets have been committed for the current partition
            position = offsets == null ? position : (Long) offsets.get(partition.toString());

            // if we have read more messages than the ones already sent to kafka, it's a newer message
            ready = count > partition;
            // update the counter of consumed messages from couchbase
            MutationsFilter.toCommit.put(partition, ++count);
        }


        return ready && dcpEvent.message() instanceof MutationMessage;
    }

    public void setContext(final SourceTaskContext context) {
        this.context = context;
    }
}


