package com.couchbase.kafka.converter;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.kafka.DCPEvent;

/**
 * Created by a.patelli on 14/12/2015.
 */
public interface Converter {
    DCPEvent toEvent(DCPRequest dcpRequest);
}
