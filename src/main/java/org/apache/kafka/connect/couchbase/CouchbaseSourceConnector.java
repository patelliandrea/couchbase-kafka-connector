package org.apache.kafka.connect.couchbase;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CouchbaseSourceConnector implement the connector interface to pull data from Couchbase
 * and send it to Kafka.
 *
 * @author Andrea Patelli
 */
public class CouchbaseSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String COUCHBASE_NODES = "couchbase.nodes";
    public static final String COUCHBASE_BUCKET = "couchbase.bucket";
    public static final String MAX_DRAIN_RATE = "dcp.maximum.drainrate";
    public static final String DCP_BUFFER_SIZE = "couchbase.dcpConnectionBufferSize";

    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;
    private String maxDrainRate;
    private String dcpConnectionBufferSize;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
        schemaName = props.get(SCHEMA_NAME);
        couchbaseNodes = props.get(COUCHBASE_NODES);
        couchbaseBucket = props.get(COUCHBASE_BUCKET);
        maxDrainRate = props.get(MAX_DRAIN_RATE);
        dcpConnectionBufferSize = props.get(DCP_BUFFER_SIZE);

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
        if (maxDrainRate == null || maxDrainRate.isEmpty()) {
            throw new ConnectException("Configuration must include 'dcp.maximum.drainrate' setting");
        } else if (maxDrainRate != null) {
            try {
                Integer.parseInt(maxDrainRate);
            } catch (Exception e) {
                throw new ConnectException("'dcp.maximum.drainrate' setting should be an integer");
            }
        }
        if (dcpConnectionBufferSize == null || dcpConnectionBufferSize.isEmpty()) {
            dcpConnectionBufferSize = Integer.toString(DefaultCoreEnvironment.DCP_CONNECTION_BUFFER_SIZE);
        } else {
            try {
                Integer.parseInt(dcpConnectionBufferSize);
            } catch (Exception e) {
                throw new ConnectException("'couchbase.dcpConnectionBufferSize' setting should be an integer");
            }
        }
    }

    /**
     * Returns the Task implementation for this Connector
     */
    @Override
    public Class<? extends Task> taskClass() {
        return CouchbaseSourceTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(SCHEMA_NAME, schemaName);
        config.put(COUCHBASE_NODES, couchbaseNodes);
        config.put(COUCHBASE_BUCKET, couchbaseBucket);
        config.put(MAX_DRAIN_RATE, maxDrainRate);
        config.put(DCP_BUFFER_SIZE, dcpConnectionBufferSize);
        configs.add(config);
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }
}
