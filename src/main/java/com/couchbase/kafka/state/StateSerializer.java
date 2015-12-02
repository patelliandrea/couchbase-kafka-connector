package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;

/**
 * @author Sergey Avseyev
 */
public interface StateSerializer {
    void dump(BucketStreamAggregatorState aggregatorState);

    void dump(BucketStreamAggregatorState aggregatorState, Short partition);

    BucketStreamAggregatorState load(BucketStreamAggregatorState aggregatorState);

    BucketStreamState load(BucketStreamAggregatorState aggregatorState, Short partition);
}
