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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.dcp.BucketStreamAggregator;
import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.core.dcp.BucketStreamStateUpdatedEvent;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventTranslatorOneArg;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.kafka.state.StateSerializer;
import org.apache.kafka.connect.source.SourceTaskContext;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private final static Map<Short, Long> toCommit = new HashMap<>(0);

    private static final EventTranslatorOneArg<DCPEvent, CouchbaseMessage> TRANSLATOR =
            new EventTranslatorOneArg<DCPEvent, CouchbaseMessage>() {
                @Override
                public void translateTo(final DCPEvent event, final long sequence, final CouchbaseMessage message) {
                    event.setMessage(message);
                }
            };
    private final ClusterFacade core;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final List<String> nodes;
    private final String bucket;
    private final String streamName;
    private final String password;
    private final BucketStreamAggregator streamAggregator;
    private final StateSerializer stateSerializer;
    private int numberOfPartitions;
    private SourceTaskContext context;

    /**
     * Creates a new {@link CouchbaseReader}.
     *
     * @param couchbaseNodes    list of the Couchbase nodes to override {@link CouchbaseEnvironment#couchbaseNodes()}
     * @param couchbaseBucket   bucket name to override {@link CouchbaseEnvironment#couchbaseBucket()}
     * @param couchbasePassword password to override {@link CouchbaseEnvironment#couchbasePassword()}
     * @param core              the core reference.
     * @param dcpRingBuffer     the buffer where to publish new events.
     * @param stateSerializer   the object to serialize the state of DCP streams.
     */
    public CouchbaseReader(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                           final ClusterFacade core, final RingBuffer<DCPEvent> dcpRingBuffer, final StateSerializer stateSerializer, final SourceTaskContext context) {
        this.core = core;
        this.dcpRingBuffer = dcpRingBuffer;
        this.nodes = couchbaseNodes;
        this.bucket = couchbaseBucket;
        this.password = couchbasePassword;
        this.streamAggregator = new BucketStreamAggregator(core, bucket);
        this.stateSerializer = stateSerializer;
        this.context = context;
        this.streamName = "CouchbaseKafka(" + this.hashCode() + ")";
    }

    /**
     * Performs connection with 2 seconds timeout.
     */
    public void connect() {
        connect(3, TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout  the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    public void connect(final long timeout, final TimeUnit timeUnit) {
        core.send(new SeedNodesRequest(nodes))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        numberOfPartitions = core.<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                })
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     */
    public void run() {
        final BucketStreamAggregatorState state = new BucketStreamAggregatorState(streamName);
        for (int i = 0; i < numberOfPartitions; i++) {
            state.put(new BucketStreamState((short) i, 1, 0, 0xffffffff, 0, 0xffffffff));
        }
        stateSerializer.load(state);

        state.updates().subscribe(
                new Action1<BucketStreamStateUpdatedEvent>() {
                    @Override
                    public void call(BucketStreamStateUpdatedEvent event) {
                        if (event.partialUpdate()) {
                            stateSerializer.dump(event.aggregatorState());
                        } else {
                            stateSerializer.dump(event.aggregatorState(), event.partitionState().partition());
                        }
                    }
                });
        streamAggregator.feed(state)
//                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    private long sequence = 0;

                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage snapshotMarker = (SnapshotMarkerMessage) dcpRequest;
                            final BucketStreamState oldState = state.get(snapshotMarker.partition());
                            BucketStreamState newState = new BucketStreamState(
                                    snapshotMarker.partition(),
                                    oldState.vbucketUUID(),
                                    snapshotMarker.endSequenceNumber(),
                                    oldState.endSequenceNumber(),
                                    snapshotMarker.endSequenceNumber(),
                                    oldState.snapshotEndSequenceNumber());
                            state.put(newState);
                        } else {
                            // partition of the message
                            Short partition = dcpRequest.partition();
                            // count of messages already been read from the partition
                            Long count = CouchbaseReader.toCommit.get(partition);
                            // if the count is null, it's the first message to commit
                            count = count == null ? 1 : count;
                            // counter of the messages for this partition already written to kafka
                            Long position = new Long(0);
                            Map<String, Object> offsets = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));
                            // if the map is null, no offsets have been committed for the current partition
                            position = offsets == null ? position : (Long) offsets.get(partition.toString());

                            // if we have read more messages than the ones already sent to kafka, it's a newer message
                            if (count > position) {
                                dcpRingBuffer.tryPublishEvent(TRANSLATOR, dcpRequest);
                            }
                            // update the counter of consumed messages from couchbase
                            CouchbaseReader.toCommit.put(partition, ++count);
                        }
                    }
                });
    }
}
