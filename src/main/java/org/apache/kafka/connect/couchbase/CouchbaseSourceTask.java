package org.apache.kafka.connect.couchbase;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.kafka.ConnectWriter;
import com.couchbase.kafka.CouchbaseConnector;
import com.couchbase.kafka.DefaultCouchbaseEnvironment;
import com.couchbase.kafka.Direction;
import com.couchbase.kafka.state.RunMode;
import javafx.util.Pair;
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
    private Integer taskBatchSize;
    private Integer taskPollFrequency;

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
        try {
            taskBatchSize = Integer.parseInt(props.get(CouchbaseSourceConnector.TASK_BATCH_SIZE));
        } catch (Exception e) {
            taskBatchSize = 200;
        }
        try {
            taskPollFrequency = Integer.parseInt(props.get(CouchbaseSourceConnector.TASK_POLL_FREQUENCY));
        } catch (Exception e) {
            taskPollFrequency = 1000;
        }

        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("bucket", Schema.OPTIONAL_STRING_SCHEMA)
                .field("document", Schema.OPTIONAL_STRING_SCHEMA)
                .field("body", Schema.OPTIONAL_STRING_SCHEMA)
                .build();


        DefaultCouchbaseEnvironment.Builder builder =
                (DefaultCouchbaseEnvironment.Builder) DefaultCouchbaseEnvironment.builder()
                        .setSourceTaskContext(context)
                        .kafkaFilterClass("com.couchbase.kafka.filter.MutationsFilter")
                        .couchbaseNodes(couchbaseNodes)
                        .couchbaseBucket(couchbaseBucket)
                        .couchbaseStateSerializerClass("com.couchbase.kafka.state.SourceTaskContextStateSerializer")
                        .batchSize(taskBatchSize)
                        .dcpEnabled(true)
                        .autoreleaseAfter(TimeUnit.SECONDS.toMillis(10L));
        connector = CouchbaseConnector.create(builder.build());


        BucketStreamAggregatorState state =  new BucketStreamAggregatorState();
        BucketStreamAggregatorState currentState =  connector.buildState(Direction.FROM_CURRENT);

//        for(short i = 0; i < 1023; i++) {
//            BucketStreamState partitionState = currentState.get(i);
//            Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", i));
//            int currentOffset = 0;
//            if(offsetMap != null) {
//                try {
//                    currentOffset = Integer.parseInt((String) offsetMap.get(Integer.toString(i)));
//                } catch(Exception e) {
//                    currentOffset = 0;
//                }
//            }
//            state.put(new BucketStreamState(partitionState.partition(), partitionState.vbucketUUID(), currentOffset, 0xffffffff, currentOffset, 0xffffffff));
//        }
//        connector.run(state, RunMode.RESUME);
        connector.run(RunMode.LOAD_AND_RESUME);
    }
    private static long consumed = 0;

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
        Queue<Pair<String, Short>> queue;

//        synchronized (ConnectWriter.sync) {
//            ConnectWriter.sync.wait(taskPollFrequency);
            queue = new LinkedList<>(ConnectWriter.getQueue());
//        }
if(!queue.isEmpty()) {
    consumed = consumed + queue.size();
    log.warn("consumed {}", consumed);
    Thread.sleep(30);
}
//        while (!queue.isEmpty()) {
//            Pair<String, Short> value = queue.poll();
//            String message = value.getKey();
//            Short partition = value.getValue();
//            Struct struct = new Struct(schema);
//            struct.put("bucket", couchbaseBucket);
//            struct.put("document", "doc");
//            struct.put("body", message);
//
//            Long count = null;
//            // read the offset map for the current partition of couchbase
//            Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap("couchbase", partition));
//
//            // if the map is not null, the offset are already been committed in the past
//            if (offsetMap != null)
//                // check if for the current partition there are saved offsets
//                if (offsetMap.get(partition.toString()) != null) {
//                    count = (Long) offsetMap.get(partition.toString());
//                }
//
//            // if it's the first commit, get the count for a local map
//            if (count == null) {
//                if (committed.get(partition) == null)
//                    committed.put(partition, new Long(0));
//                count = committed.get(partition);
//            } else {
//                // else get the bigger of the saved offsets
//                if (committed.get(partition) != null)
//                    count = count > committed.get(partition) ? count : committed.get(partition);
//            }
//
//            count += 1;
//            // add the record to the list to write to kafka
//            log.trace("adding record to partition {} with count {}", partition, count);
//            records.add(new SourceRecord(Collections.singletonMap("couchbase", partition), Collections.singletonMap(partition.toString(), count), topic, struct.schema(), struct));
//            // set the count of committed messages for the current partition
//            committed.put(partition, count);
//        }
        return records;
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    }
}
