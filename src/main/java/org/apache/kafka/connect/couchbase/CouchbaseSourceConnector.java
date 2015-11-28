package org.apache.kafka.connect.couchbase;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by a.patelli on 28/11/2015.
 */
public class CouchbaseSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String COUCHBASE_NODES = "couchbase.nodes";
    public static final String COUCHBASE_BUCKET = "couchbase.bucket";

    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
        schemaName = props.get(SCHEMA_NAME);
        couchbaseNodes = props.get(COUCHBASE_NODES);
        couchbaseBucket = props.get(COUCHBASE_BUCKET);

        if (topic == null || topic.isEmpty())
            throw new ConnectException("Configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("Configuration should have a single topic when used as a source");
        if (schemaName == null || schemaName.isEmpty())
            throw new ConnectException("Configuration must include 'schema.name' setting");
        if (couchbaseNodes == null || couchbaseNodes.isEmpty())
            throw new ConnectException("Configuration must include 'couchbase.nodes' setting");
        if (couchbaseBucket == null || couchbaseBucket.isEmpty())
            throw new ConnectException("Configuration must include 'couchbase.bucket' setting");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CouchbaseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(SCHEMA_NAME, schemaName);
        config.put(COUCHBASE_NODES, couchbaseNodes);
        config.put(COUCHBASE_BUCKET, couchbaseBucket);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }
}
