package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.kafka.CouchbaseEnvironment;
import org.apache.kafka.connect.couchbase.CouchbaseSourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class SourceTaskContextStateSerializer implements StateSerializer {
    private final static Logger log = LoggerFactory.getLogger(SourceTaskContextStateSerializer.class);
    private SourceTaskContext context;
    private String couchbaseBucket;

    public SourceTaskContextStateSerializer(final CouchbaseEnvironment environment) {
        this.context = environment.getSourceTaskContext();
        this.couchbaseBucket = environment.couchbaseBucket();
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
                aggregatorState.put(newState, false);
            }
        }
        return aggregatorState;
    }

    @Override
    public BucketStreamState load(BucketStreamAggregatorState aggregatorState, Short partition) {
        BucketStreamState partitionState = aggregatorState.get(partition);
//        Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));
        Map<String, Object> offsetsForPartition = CouchbaseSourceTask.offsets.get(Collections.singletonMap(couchbaseBucket, partition));
        if (offsetsForPartition != null) {
            Long currentOffset = (Long) offsetsForPartition.get(partition.toString());
            if (currentOffset != null)
                return new BucketStreamState(partitionState.partition(), partitionState.vbucketUUID(), currentOffset, 0xffffffff, currentOffset, 0xffffffff);
        }
        return null;
    }
}