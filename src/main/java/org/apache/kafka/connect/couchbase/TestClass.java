package org.apache.kafka.connect.couchbase;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.kafka.CouchbaseConnector;
import com.couchbase.kafka.DefaultCouchbaseEnvironment;
import com.couchbase.kafka.state.RunMode;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by a.patelli on 29/11/2015.
 */
public class TestClass {
    public final static Map<Short, Long> positions = new HashMap<>(0);
    public final static Map<Short, Long> counters = new HashMap<>(0);
    public static void main(String[] args) {
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder()
                .kafkaFilterClass("com.couchbase.kafka.filter.MutationsFilter");
        builder
                .couchbaseNodes("192.168.56.1");
        builder
                .couchbaseBucket("beer-sample");
        builder
                .couchbaseStateSerializerClass("com.couchbase.kafka.state.NullStateSerializer");
        builder
                .dcpEnabled(true);
        builder
                .autoreleaseAfter(TimeUnit.SECONDS.toMillis(10L));
        CouchbaseConnector connector = CouchbaseConnector.create(builder.build());
        connector.run();
        while(true);
    }
}
