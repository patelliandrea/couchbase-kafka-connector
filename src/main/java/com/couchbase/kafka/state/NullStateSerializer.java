package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.kafka.CouchbaseEnvironment;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class NullStateSerializer implements StateSerializer {

    public NullStateSerializer(final CouchbaseEnvironment environment) {
    }


    @Override
    public void dump(BucketStreamAggregatorState aggregatorState) {

    }

    @Override
    public void dump(BucketStreamAggregatorState aggregatorState, short partition) {

    }

    @Override
    public BucketStreamAggregatorState load(BucketStreamAggregatorState aggregatorState) {
        return new BucketStreamAggregatorState(aggregatorState.name());
    }

    @Override
    public BucketStreamState load(BucketStreamAggregatorState aggregatorState, short partition) {
        return new BucketStreamState(partition, 0, 0, 0xffffffff, 0, 0xffffffff);
    }
}