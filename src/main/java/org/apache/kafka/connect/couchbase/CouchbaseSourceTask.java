package org.apache.kafka.connect.couchbase;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.ConnectWriter;
import com.couchbase.kafka.CouchbaseConnector;
import com.couchbase.kafka.DefaultCouchbaseEnvironment;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * CouchbaseSourceTask is a Task that pulls records from Couchbase for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class CouchbaseSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(CouchbaseSourceTask.class);
    private static Schema schema = null;
    private String topic;
    private String schemaName;
    private String couchbaseNodes;
    private String couchbaseBucket;
    private Integer maxDrainRate;
    private Integer dcpConnectionBufferSize;

    public static Map<Map<String, Short>, Map<String, Object>> offsets;

    private static CouchbaseConnector connector;

    private final static Map<Short, Long> committed = new HashMap<>(0);

    private final Queue<MutationMessage> toRelease = new LinkedList<>();

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
        maxDrainRate = Integer.parseInt(props.get(CouchbaseSourceConnector.MAX_DRAIN_RATE));
        if (maxDrainRate == null)
            throw new ConnectException("CoucbaseSourceTask config missing maxDrainRate setting");

        dcpConnectionBufferSize = Integer.parseInt(props.get(CouchbaseSourceConnector.DCP_BUFFER_SIZE));
        loadOffsets(couchbaseBucket);

        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("bucket", Schema.OPTIONAL_STRING_SCHEMA)
                .field("document", Schema.OPTIONAL_STRING_SCHEMA)
                .field("body", Schema.OPTIONAL_STRING_SCHEMA)
                .build();


        DefaultCouchbaseEnvironment.Builder builder =
                (DefaultCouchbaseEnvironment.Builder) DefaultCouchbaseEnvironment.builder()
                        .maxDrainRate(maxDrainRate)
                        .setSourceTaskContext(context)
                        .kafkaFilterClass("com.couchbase.kafka.filter.MutationsFilter")
                        .couchbaseNodes(couchbaseNodes)
                        .couchbaseBucket(couchbaseBucket)
                        .couchbaseStateSerializerClass("com.couchbase.kafka.state.SourceTaskContextStateSerializer")
                        .dcpEnabled(true)
                        .autoreleaseAfter(TimeUnit.SECONDS.toMillis(10L))
                        .dcpConnectionBufferSize(dcpConnectionBufferSize);

        connector = CouchbaseConnector.create(builder.build());
        new Thread(connector).start();
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
        // get the queue from couchbase
        Queue<MutationMessage> queue;
        queue = ConnectWriter.getQueue();
        synchronized (ConnectWriter.sync) {
            ConnectWriter.sync.notifyAll();
            while (!queue.isEmpty()) {
                MutationMessage value = queue.poll();
                String message = value.content().toString(CharsetUtil.UTF_8);
                Short partition = value.partition();
                Struct struct = new Struct(schema);
                struct.put("bucket", couchbaseBucket);
                struct.put("document", "doc");
                struct.put("body", message);

                Long count = null;
                // read the offset map for the current partition of couchbase
                Map<String, Object> offsetsForPartition = offsets.get(Collections.singletonMap(couchbaseBucket, partition));
                // if the map is not null, the offset are already been committed in the past
                if (offsetsForPartition != null)
                    // check if for the current partition there are saved offsets
                    if (offsetsForPartition.get(partition.toString()) != null) {
                        count = (Long) offsetsForPartition.get(partition.toString());
                    }

                // if it's the first commit, get the count for a local map
                if (count == null) {
                    if (committed.get(partition) == null)
                        committed.put(partition, new Long(0));
                    count = committed.get(partition);
                } else {
                    // else get the bigger of the saved offsets
                    if (committed.get(partition) != null)
                        count = count > committed.get(partition) ? count : committed.get(partition);
                }

                count += 1;
                // add the record to the list to write to kafka
                log.trace("adding record to partition {} with count {}", partition, count);
                // set the count of committed messages for the current partition
                records.add(new SourceRecord(Collections.singletonMap(couchbaseBucket, partition), Collections.singletonMap(partition.toString(), count), topic, struct.schema(), struct));
                committed.put(partition, count);
                value.content().release();
            }
            return records;
        }
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    }

    /**
     * Loads the saved offsets for the current bucket
     *
     * @param source the bucket for which load the offsets
     */
    private void loadOffsets(String source) {
        List<Map<String, Short>> partitions = new ArrayList<>();
        for (Short i = 0; i < 1024; i++) {
            Map<String, Short> partition = Collections.singletonMap(source, i);
            partitions.add(partition);
        }
        offsets = context.offsetStorageReader().offsets(partitions);
    }
}
