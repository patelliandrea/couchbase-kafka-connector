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
    public static final String TASK_BULK_SIZE = "task.bulk.size";
    public static final String TASK_POLL_FREQUENCY = "task.poll.frequency";

    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;
    private String taskBulkSize;
    private String taskPollFrequency;

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
        taskBulkSize = props.get(TASK_BULK_SIZE);
        taskPollFrequency = props.get(TASK_POLL_FREQUENCY);

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
        if(taskPollFrequency != null) {
            try {
                Integer.parseInt(taskPollFrequency);
            } catch(Exception e) {
                throw new ConnectException("'task.poll.frequency' setting should be an integer");
            }
        }
        if(taskBulkSize != null) {
            try {
                Integer.parseInt(taskBulkSize);
            } catch(Exception e) {
                throw new ConnectException("'task.bulk.size' setting should be an integer");
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
        config.put(TASK_BULK_SIZE, taskBulkSize);
        config.put(TASK_POLL_FREQUENCY, taskPollFrequency);
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
