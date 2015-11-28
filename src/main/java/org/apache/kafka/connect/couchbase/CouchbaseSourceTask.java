package org.apache.kafka.connect.couchbase;

import com.couchbase.kafka.ConnectWriter;
import com.couchbase.kafka.CouchbaseConnector;
import com.couchbase.kafka.DefaultCouchbaseEnvironment;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class CouchbaseSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(CouchbaseSourceTask.class);

    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;

    private static CouchbaseConnector connector;

    @Override
    public String version() {
        return new CouchbaseSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(CouchbaseSourceConnector.TOPIC_CONFIG);
        if (topic == null)
            throw new ConnectException("CouchbaseSourceTask config missing topic setting");
        schemaName = props.get(CouchbaseSourceConnector.SCHEMA_NAME);
        if (schemaName == null)
            throw new ConnectException("CouchbaseSourceTask config missing schemaName setting");
        couchbaseNodes = props.get(CouchbaseSourceConnector.COUCHBASE_NODES);
        if (couchbaseNodes == null)
            throw new ConnectException("CouchbaseSourceTask config missing couchbaseNodes setting");
        couchbaseBucket = props.get(CouchbaseSourceConnector.COUCHBASE_BUCKET);
        if (couchbaseBucket == null)
            throw new ConnectException("CouchbaseSourceTask config missing couchbaseBucket setting");

        DefaultCouchbaseEnvironment.Builder builder =
                (DefaultCouchbaseEnvironment.Builder) DefaultCouchbaseEnvironment.builder()
                        .kafkaFilterClass("")
                        .couchbaseNodes("")
                        .couchbaseBucket("")
                        .couchbaseStateSerializerClass("")
                        .dcpEnabled(true);
        connector = CouchbaseConnector.create(builder.build());
        connector.run();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Queue<String> queue = new LinkedList<>(ConnectWriter.getQueue());
        while (!queue.isEmpty()) {
            log.warn("received: {}", queue.poll());
        }
        return new ArrayList<>(0);
    }

    @Override
    public void stop() {

    }
}
