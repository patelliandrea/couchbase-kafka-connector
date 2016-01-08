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


    /**
     * Save the current state
     *
     * @param aggregatorState the state to save
     */
    @Override
    public void dump(BucketStreamAggregatorState aggregatorState) {
        // do nothing, dumped by committing offsets
    }

    /**
     * For every partition, save the current state
     *
     * @param aggregatorState the state to be saved
     * @param partition       to partition for which save the state
     */
    @Override
    public void dump(BucketStreamAggregatorState aggregatorState, Short partition) {
        // do nothing, dumped by committing offsets
    }

    /**
     * Load the saved state
     *
     * @param aggregatorState the state to load
     * @return the loaded state
     */
    @Override
    public BucketStreamAggregatorState load(BucketStreamAggregatorState aggregatorState) {
        for (BucketStreamState streamState : aggregatorState) {
            BucketStreamState newState = load(aggregatorState, streamState.partition());
            if (newState != null) {
                aggregatorState.put(newState, false);
            }
        }
        return aggregatorState;
    }

    /**
     * For every partition, load the saved state
     *
     * @param aggregatorState the state to load
     * @param partition       the partition for which load the state
     * @return the loaded state
     */
    @Override
    public BucketStreamState load(BucketStreamAggregatorState aggregatorState, Short partition) {
        BucketStreamState partitionState = aggregatorState.get(partition);
        Map<String, Object> offsetsForPartition = CouchbaseSourceTask.offsets.get(Collections.singletonMap(couchbaseBucket, partition));
        if (offsetsForPartition != null) {
            Long currentOffset = (Long) offsetsForPartition.get(partition.toString());
            if (currentOffset != null)
                return new BucketStreamState(partitionState.partition(), partitionState.vbucketUUID(), currentOffset, 0xffffffff, currentOffset, 0xffffffff);
        }
        return null;
    }
}