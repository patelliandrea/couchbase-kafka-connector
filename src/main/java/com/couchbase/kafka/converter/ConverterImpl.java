package com.couchbase.kafka.converter;

import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.kafka.DCPEvent;

/**
 * Created by a.patelli on 14/12/2015.
 */
public class ConverterImpl implements Converter {
    @Override
    public DCPEvent toEvent(DCPRequest dcpRequest) {
        DCPEvent event = new DCPEvent();
        event.setMessage(dcpRequest);
        return event;
    }
}
