# Couchbase Kafka Connector

Porting of the official Couchbase Kafka connector.
It is a [Kafka Connector](http://kafka.apache.org/090/documentation.html#connect)
for loading data into Kafka from a Couchbase Bucket.

# Development

To build a development version, you'll need the latest version of
[couchbase-jvm-core](https://github.com/couchbase/couchbase-jvm-core).
You can build couchbase-kafka-connector with Maven using the
standard lifecycle phases.

# Configuration

Example of the configuration file:

``` ini
# the name of the connector
name=couchbase-source
# the class of the Connector
connector.class=org.apache.kafka.connect.couchbase.CouchbaseSourceConnector
# topic on which produce the messages
topic=testcouchbase
# name of the schema to use for the produced messages
schema.name=testcouchbase
# addresses of the couchbase nodes, separated by ';'
couchbase.nodes=localhost
# bucket from which read documents to write on kafka
couchbase.bucket=beer-sample
# max number of tasks to run
tasks.max=1
# maximum number of documents to fetch from couchbase every second
dcp.maximum.drainrate=600
```