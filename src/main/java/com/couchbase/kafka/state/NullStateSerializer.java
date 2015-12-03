package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.kafka.CouchbaseEnvironment;
import org.apache.kafka.connect.source.SourceTaskContext;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class NullStateSerializer implements StateSerializer {
    SourceTaskContext context;

    public NullStateSerializer(final CouchbaseEnvironment environment) {
        this.context = environment.getSourceTaskContext();
    }


    @Override
    public void dump(BucketStreamAggregatorState aggregatorState) {
//        log.warn("dumping: {}", aggregatorState.toString());
//        for(BucketStreamState streamState : aggregatorState) {
//            dump(aggregatorState, streamState.partition());
//        }
        // do nothing, dumped by committing offsets
    }

    @Override
    public void dump(BucketStreamAggregatorState aggregatorState, Short partition) {
//        TestClass.positions.put(partition, aggregatorState.get(partition).startSequenceNumber());
//        context.offsetStorageReader().offset(Collections.singletonMap(partition.toString(), aggregatorState.get(partition).startSequenceNumber())).put(partition.toString(), aggregatorState.get(partition).startSequenceNumber());
        // do nothing, dumped by committing offsets
    }

    @Override
    public BucketStreamAggregatorState load(BucketStreamAggregatorState aggregatorState) {
//        return new BucketStreamAggregatorState(aggregatorState.name());
        for (BucketStreamState streamState : aggregatorState) {
            BucketStreamState newState = load(aggregatorState, streamState.partition());
            if (newState != null) {
                aggregatorState.put(newState, true);
            }
        }
        return aggregatorState;
    }

    @Override
    public BucketStreamState load(BucketStreamAggregatorState aggregatorState, Short partition) {
////        TestClass.positions.put(partition, new Long(0));
//        return new BucketStreamState(partition, 1, 0, 0xffffffff, 0, 0xffffffff);
////        return new BucketStreamState(partition, 0, TestClass.positions.get(partition), 0xffffffff, TestClass.positions.get(partition), 0xffffffff);
        // makes no sense returning something, not working
        return null;
    }
}