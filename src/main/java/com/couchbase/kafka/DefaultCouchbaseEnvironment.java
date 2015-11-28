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

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author Sergey Avseyev
 */
public class DefaultCouchbaseEnvironment extends DefaultCoreEnvironment implements CouchbaseEnvironment {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseEnvironment.class);

    private static final String COUCHBASE_STATE_SERIALIZER_CLASS = "com.couchbase.kafka.state.ZookeeperStateSerializer";
    private static final String COUCHBASE_BUCKET = "default";
    private static final String COUCHBASE_PASSWORD = "";
    private static final String COUCHBASE_NODE = "127.0.0.1";
    private static final String KAFKA_FILTER_CLASS = "com.couchbase.kafka.filter.MutationsFilter";

    private String couchbaseStateSerializerClass;
    private String couchbasePassword;
    private String couchbaseBucket;
    private List<String> couchbaseNodes;
    private String kafkaFilterClass;


    public static String SDK_PACKAGE_NAME_AND_VERSION = "couchbase-kafka-connector";
    private static final String VERSION_PROPERTIES = "com.couchbase.kafka.properties";

    /**
     * Sets up the package version and user agent.
     *
     * Note that because the class loader loads classes on demand, one class from the package
     * is loaded upfront.
     */
    static {
        try {
            Class<CouchbaseConnector> connectorClass = CouchbaseConnector.class;
            if (connectorClass == null) {
                throw new IllegalStateException("Could not locate CouchbaseConnector");
            }

            String version = null;
            String gitVersion = null;
            try {
                Properties versionProp = new Properties();
                versionProp.load(DefaultCoreEnvironment.class.getClassLoader().getResourceAsStream(VERSION_PROPERTIES));
                version = versionProp.getProperty("specificationVersion");
                gitVersion = versionProp.getProperty("implementationVersion");
            } catch (Exception e) {
                LOGGER.info("Could not retrieve version properties, defaulting.", e);
            }
            SDK_PACKAGE_NAME_AND_VERSION = String.format("couchbase-kafka-connector/%s (git: %s)",
                    version == null ? "unknown" : version, gitVersion == null ? "unknown" : gitVersion);

            // this will overwrite the USER_AGENT in Core
            // making core send user_agent with kafka connector version information
            USER_AGENT = String.format("%s (%s/%s %s; %s %s)",
                    SDK_PACKAGE_NAME_AND_VERSION,
                    System.getProperty("os.name"),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vm.name"),
                    System.getProperty("java.runtime.version")
            );
        } catch (Exception ex) {
            LOGGER.info("Could not set up user agent and packages, defaulting.", ex);
        }
    }

    /**
     * Creates a {@link CouchbaseEnvironment} with default settings applied.
     *
     * @return a {@link DefaultCouchbaseEnvironment} with default settings.
     */
    public static DefaultCouchbaseEnvironment create() {
        return new DefaultCouchbaseEnvironment(builder());
    }

    /**
     * Returns the {@link Builder} to customize environment settings.
     *
     * @return the {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    protected DefaultCouchbaseEnvironment(final Builder builder) {
        super(builder);

        if (!dcpEnabled()) {
            throw new IllegalStateException("Kafka integration cannot work without DCP enabled.");
        }
        couchbaseStateSerializerClass = stringPropertyOr("couchbaseStateSerializerClass", builder.couchbaseStateSerializerClass);
        couchbaseNodes = stringListPropertyOr("couchbase.nodes", builder.couchbaseNodes);
        couchbaseBucket = stringPropertyOr("couchbase.bucket", builder.couchbaseBucket);
        couchbasePassword = stringPropertyOr("couchbase.password", builder.couchbasePassword);
        kafkaFilterClass = stringPropertyOr("kafka.filter.class", builder.kafkaFilterClass);
    }

    @Override
    public String couchbaseStateSerializerClass() {
        return couchbaseStateSerializerClass;
    }

    @Override
    public List<String> couchbaseNodes() {
        return couchbaseNodes;
    }

    @Override
    public String couchbaseBucket() {
        return couchbaseBucket;
    }

    @Override
    public String couchbasePassword() {
        return couchbasePassword;
    }

    @Override
    public String kafkaFilterClass() {
        return kafkaFilterClass;
    }

    private List<String> stringListPropertyOr(String path, List<String> def) {
        String found = stringPropertyOr(path, null);
        if (found == null) {
            return def;
        } else {
            return Arrays.asList(found.split(";"));
        }
    }

    public static class Builder extends DefaultCoreEnvironment.Builder {
        public String couchbaseStateSerializerClass = COUCHBASE_STATE_SERIALIZER_CLASS;
        public List<String> couchbaseNodes;
        public String couchbaseBucket = COUCHBASE_BUCKET;
        public String couchbasePassword = COUCHBASE_PASSWORD;
        public String kafkaFilterClass = KAFKA_FILTER_CLASS;

        public Builder() {
            couchbaseNodes = Collections.singletonList(COUCHBASE_NODE);
        }

        public Builder couchbaseStateSerializerClass(final String couchbaseStateSerializerClass) {
            this.couchbaseStateSerializerClass = couchbaseStateSerializerClass;
            return this;
        }

        public Builder couchbaseNodes(final List<String> couchbaseNodes) {
            this.couchbaseNodes = couchbaseNodes;
            return this;
        }

        public Builder couchbaseNodes(final String couchbaseNode) {
            this.couchbaseNodes(Collections.singletonList(couchbaseNode));
            return this;
        }

        public Builder couchbaseBucket(final String couchbaseBucket) {
            this.couchbaseBucket = couchbaseBucket;
            return this;
        }

        public Builder couchbasePassword(final String couchbasePassword) {
            this.couchbasePassword = couchbasePassword;
            return this;
        }

        public Builder kafkaFilterClass(final String kafkaFilterClass) {
            this.kafkaFilterClass = kafkaFilterClass;
            return this;
        }

        @Override
        public DefaultCouchbaseEnvironment build() {
            return new DefaultCouchbaseEnvironment(this);
        }
    }
}
