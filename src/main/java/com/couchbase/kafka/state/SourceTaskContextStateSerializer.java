package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.kafka.CouchbaseEnvironment;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.Map;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class SourceTaskContextStateSerializer implements StateSerializer {
    SourceTaskContext context;

    public SourceTaskContextStateSerializer(final CouchbaseEnvironment environment) {
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
        BucketStreamState partitionState = aggregatorState.get(partition);
        Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));
        int currentOffset = 0;
        if(offsetMap != null) {
            try {
                currentOffset = Integer.parseInt((String) offsetMap.get(Integer.toString(partition)));
            } catch(Exception e) {
                currentOffset = 0;
            }
        }
        return new BucketStreamState(partitionState.partition(), partitionState.vbucketUUID(), currentOffset, 0xffffffff, currentOffset, 0xffffffff);
    }
}