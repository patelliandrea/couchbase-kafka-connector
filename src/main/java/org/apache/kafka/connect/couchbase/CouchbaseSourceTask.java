package org.apache.kafka.connect.couchbase;

import com.couchbase.kafka.ConnectWriter;
import com.couchbase.kafka.CouchbaseConnector;
import com.couchbase.kafka.DefaultCouchbaseEnvironment;
import javafx.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * CouchbaseSourceTask is a Task that pulls records from Couchbase for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class CouchbaseSourceTask extends SourceTask {
    private static Schema schema = null;
    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;

    private static CouchbaseConnector connector;

    private final static Map<Short, Long> committed = new HashMap<>(0);

    @Override
    public String version() {
        return new CouchbaseSourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsinng and one-time setup of the task.
     *
     * @param props initial configuration.
     */
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

        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("bucket", Schema.OPTIONAL_STRING_SCHEMA)
                .field("document", Schema.OPTIONAL_STRING_SCHEMA)
                .field("body", Schema.OPTIONAL_STRING_SCHEMA)
                .build();


        DefaultCouchbaseEnvironment.Builder builder =
                (DefaultCouchbaseEnvironment.Builder) DefaultCouchbaseEnvironment.builder()
                        .kafkaFilterClass("com.couchbase.kafka.filter.MutationsFilter");
        builder
                .couchbaseNodes(couchbaseNodes);
        builder
                .couchbaseBucket(couchbaseBucket);
        builder
                .couchbaseStateSerializerClass("com.couchbase.kafka.state.NullStateSerializer");
        builder
                .dcpEnabled(true);
        builder
                .setSourceTaskContext(context);
        builder
                .autoreleaseAfter(TimeUnit.SECONDS.toMillis(10L));
        connector = CouchbaseConnector.create(builder.build());
        connector.run();
    }

    /**
     * Poll this CouchbaseSourceTask for new records. Block if no data is currently available.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Queue<Pair<String, Short>> queue = new LinkedList<>(ConnectWriter.getQueue());
        while (queue.isEmpty())
            ConnectWriter.semaphore.wait();
        while (!queue.isEmpty()) {
            Pair<String, Short> value = queue.poll();
            String message = value.getKey();
            Short partition = value.getValue();
            Struct struct = new Struct(schema);
            struct.put("bucket", couchbaseBucket);
            struct.put("document", "doc");
            struct.put("body", message);

            Long count = null;
            Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));

            if (offsetMap != null)
                if (offsetMap.get(partition.toString()) != null) {
                    count = (Long) offsetMap.get(partition.toString());
                }

            if (count == null) {
                if (committed.get(partition) == null)
                    committed.put(partition, new Long(0));
                count = committed.get(partition);
            }

            count += 1;
            records.add(new SourceRecord(Collections.singletonMap("couchbase", partition), Collections.singletonMap(partition.toString(), count), topic, struct.schema(), struct));
            committed.put(partition, count);
        }
        return records;
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    }
}
