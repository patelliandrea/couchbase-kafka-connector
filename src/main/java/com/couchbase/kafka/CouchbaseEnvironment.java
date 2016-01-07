package com.couchbase.kafka;

import com.couchbase.client.core.env.CoreEnvironment;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;

/**
 * A {@link CouchbaseEnvironment} settings related to Kafka connection, in addition to all the core building blocks
 * like environment settings and thread pools inherited from {@link CoreEnvironment} so
 * that the application can work with it properly.
 * <p/>
 * This interface defines the contract. How properties are loaded is chosen by the implementation. See the
 * {@link DefaultCouchbaseEnvironment} class for the default implementation.
 * <p/>
 * Note that the {@link CouchbaseEnvironment} is stateful, so be sure to call {@link CoreEnvironment#shutdown()}
 * properly.
 *
 * @author Sergey Avseyev
 */
public interface CouchbaseEnvironment extends CoreEnvironment {
    /**
     * Full name of class used to serialize state of the Couchbase streams. It have to
     * implement {@link com.couchbase.kafka.state.StateSerializer}.
     *
     * @return class name of the serializer
     */
    String couchbaseStateSerializerClass();

    /**
     * List of Couchbase nodes used to connect.
     *
     * @return list of node addresses
     */
    List<String> couchbaseNodes();

    /**
     * Name of the bucket in Couchbase.
     *
     * @return name of the bucket
     */
    String couchbaseBucket();

    /**
     * Password if the bucket is protected.
     *
     * @return couchbase password.
     */
    String couchbasePassword();

    /**
     * Full name of class used to filter data stream from Couchbase
     *
     * @return class name of filter
     */
    String kafkaFilterClass();

    /**
     * Context of the Source Task, used for storing offsets.
     *
     * @return the context of the Source Task
     */
    SourceTaskContext getSourceTaskContext();

    Integer maxDrainRate();
}
