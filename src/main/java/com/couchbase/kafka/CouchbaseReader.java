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
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.kafka.converter.Converter;
import com.couchbase.kafka.converter.ConverterImpl;
import com.couchbase.kafka.state.RunMode;
import com.couchbase.kafka.state.StateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private final static Logger log = LoggerFactory.getLogger(CouchbaseReader.class);

    private final ClusterFacade core;
    private final List<String> nodes;
    private final String bucket;
    private final String streamName;
    private final String password;
    private final BucketStreamAggregator aggregator;
    private final StateSerializer stateSerializer;
    private int numberOfPartitions;
    private final Converter converter;
    private final ConnectWriter writer;
    private final Integer maxDrainRate;


    /**
     * Creates a new {@link CouchbaseReader}.
     *
     * @param couchbaseNodes    list of the Couchbase nodes to override {@link CouchbaseEnvironment#couchbaseNodes()}
     * @param couchbaseBucket   bucket name to override {@link CouchbaseEnvironment#couchbaseBucket()}
     * @param couchbasePassword password to override {@link CouchbaseEnvironment#couchbasePassword()}
     * @param core              the core reference.
     *                          //     * @param dcpRingBuffer     the buffer where to publish new events.
     * @param stateSerializer   the object to serialize the state of DCP streams.
     */
    public CouchbaseReader(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                           final ClusterFacade core, final StateSerializer stateSerializer, final ConnectWriter writer, final Integer maxDrainRate) {
        this.core = core;
        this.nodes = couchbaseNodes;
        this.bucket = couchbaseBucket;
        this.password = couchbasePassword;
        this.aggregator = new BucketStreamAggregator(core, bucket);
        this.stateSerializer = stateSerializer;
        this.streamName = "CouchbaseKafka(" + this.hashCode() + ")";
        this.converter = new ConverterImpl();
        this.writer = writer;
        this.maxDrainRate = maxDrainRate;
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

    public MutationToken[] currentSequenceNumbers() {
        return aggregator.getCurrentState().map(new Func1<BucketStreamAggregatorState, MutationToken[]>() {
            @Override
            public MutationToken[] call(BucketStreamAggregatorState aggregatorState) {
                List<MutationToken> tokens = new ArrayList<MutationToken>(aggregatorState.size());
                for (BucketStreamState streamState : aggregatorState) {
                    tokens.add(new MutationToken(streamState.partition(),
                            streamState.vbucketUUID(), streamState.startSequenceNumber()));
                }
                return tokens.toArray(new MutationToken[tokens.size()]);
            }
        }).toBlocking().first();
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     */
    public void run(final BucketStreamAggregatorState state, final RunMode mode) {
        if (mode == RunMode.LOAD_AND_RESUME) {
            stateSerializer.load(state);
        }

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
        Timer timer = new Timer();
        timer.schedule(new java.util.TimerTask() {
            @Override
            public void run() {
                log.trace("count {}", count);
                count = 0;
            }
        }, 0, 1000);

        aggregator
                .feed(state)
                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        synchronized (ConnectWriter.sync) {
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
                                while (count >= maxDrainRate) {
                                    try {
                                        ConnectWriter.sync.wait();
                                    } catch (Exception e) {
                                    }
                                }
                                writer.addToQueue(converter.toEvent(dcpRequest));
                                count++;
                            }
                        }
                    }
                });
    }

    private static long count = 0;
}
