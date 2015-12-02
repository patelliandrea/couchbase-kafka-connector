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
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.com.lmax.disruptor.ExceptionHandler;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.client.deps.com.lmax.disruptor.dsl.Disruptor;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.kafka.filter.Filter;
import com.couchbase.kafka.state.RunMode;
import com.couchbase.kafka.state.StateSerializer;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link CouchbaseConnector} is an entry point of the library. It sets up connections with both Couchbase and
 * Kafka clusters. And carries all events from Couchbase to Kafka.
 * <p/>
 * The example below will transfer all mutations from Couchbase bucket "my-bucket" as JSON to Kafka topic "my-topic".
 * <pre>
 * {@code
 *  DefaultCouchbaseEnvironment.Builder builder =
 *        (DefaultCouchbaseEnvironment.Builder) DefaultCouchbaseEnvironment.builder()
 *           .kafkaFilterClass("kafka.serializer.StringEncoder")
 *           .kafkaValueSerializerClass("com.couchbase.kafka.coder.JsonEncoder")
 *           .dcpEnabled(true);
 *  CouchbaseEnvironment env = builder.build();
 *  CouchbaseConnector connector = CouchbaseConnector.create(env,
 *                 "couchbase.example.com", "my-bucket", "pass",
 *                 "kafka.example.com", "my-topic");
 *  connector.run();
 * }
 * </pre>
 *
 * @author Sergey Avseyev
 */
public class CouchbaseConnector implements Runnable {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseConnector.class);

    private static final DCPEventFactory DCP_EVENT_FACTORY = new DCPEventFactory();

    private final ClusterFacade core;
    private final ExecutorService disruptorExecutor;
    private final Disruptor<DCPEvent> disruptor;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final CouchbaseReader couchbaseReader;
    ConnectWriter writer;
    private final Filter filter;
    private final StateSerializer stateSerializer;
    private final CouchbaseEnvironment environment;

    /**
     * Create {@link CouchbaseConnector} with specified settings (list of Couchbase nodes)
     * and custom {@link CouchbaseEnvironment}.
     *
     * @param couchbaseNodes    address of Couchbase node to override {@link CouchbaseEnvironment#couchbaseNodes()}.
     * @param couchbaseBucket   name of Couchbase bucket to override {@link CouchbaseEnvironment#couchbaseBucket()}.
     * @param couchbasePassword password for Couchbase bucket to override {@link CouchbaseEnvironment#couchbasePassword()}.
     * @param environment       custom environment object.
     */
    private CouchbaseConnector(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword, final CouchbaseEnvironment environment) {
        try {
            filter = (Filter) Class.forName(environment.kafkaFilterClass()).newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot initialize filter class:" +
                    environment.kafkaFilterClass(), e);
        }
        try {
            stateSerializer = (StateSerializer) Class.forName(environment.couchbaseStateSerializerClass())
                    .getDeclaredConstructor(CouchbaseEnvironment.class)
                    .newInstance(environment);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot initialize state serializer class: " +
                    environment.couchbaseStateSerializerClass(), e);
        }
        this.environment = environment;
        core = new CouchbaseCore(environment);
        disruptorExecutor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cb-kafka", true));
        disruptor = new Disruptor<DCPEvent>(
                DCP_EVENT_FACTORY,
                16384,
                disruptorExecutor
        );
        disruptor.handleExceptionsWith(new ExceptionHandler() {
            @Override
            public void handleEventException(final Throwable ex, final long sequence, final Object event) {
                LOGGER.warn("Exception while Handling DCP Events {}", event, ex);
            }

            @Override
            public void handleOnStartException(final Throwable ex) {
                LOGGER.warn("Exception while Starting DCP RingBuffer", ex);
            }

            @Override
            public void handleOnShutdownException(final Throwable ex) {
                LOGGER.info("Exception while shutting down DCP RingBuffer", ex);
            }
        });

        final Properties props = new Properties();
        writer = new ConnectWriter(filter);
        disruptor.handleEventsWith(writer);
        disruptor.start();
        dcpRingBuffer = disruptor.getRingBuffer();
        couchbaseReader = new CouchbaseReader(couchbaseNodes, couchbaseBucket, couchbasePassword, core, dcpRingBuffer, stateSerializer, environment.getSourceTaskContext());
        couchbaseReader.connect();
    }
    /**
     * Create {@link CouchbaseConnector} with specified settings.
     *
     * @param environment custom environment object
     * @return configured {@link CouchbaseConnector}
     */
    public static CouchbaseConnector create(final CouchbaseEnvironment environment) {
        return create(environment.couchbaseNodes(), environment.couchbaseBucket(), environment.couchbasePassword(), environment);
    }

    /**
     * Create {@link CouchbaseConnector} with specified settings.
     *
     * @param couchbaseNodes    address of Couchbase node to override {@link CouchbaseEnvironment#couchbaseNodes()}.
     * @param couchbaseBucket   name of Couchbase bucket to override {@link CouchbaseEnvironment#couchbaseBucket()}.
     * @param couchbasePassword password for Couchbase bucket to override {@link CouchbaseEnvironment#couchbasePassword()}.
     * @param environment       environment object
     * @return configured {@link CouchbaseConnector}
     */
    public static CouchbaseConnector create(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword, final CouchbaseEnvironment environment) {
        return new CouchbaseConnector(couchbaseNodes, couchbaseBucket, couchbasePassword, environment);
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     */
    @Override
    public void run() {
        couchbaseReader.run();
    }
}
