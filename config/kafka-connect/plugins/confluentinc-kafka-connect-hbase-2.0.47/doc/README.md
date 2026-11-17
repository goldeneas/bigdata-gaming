# Introduction

This project provides a base for connectors that to write data to BigTable implementations from Kafka Connect.

Includes:
* Google BigTable Sink Connector
* Apache HBase Sink Connector


# Kafka Connect Suite of BigTable Connectors

*kafka-connect-bigtable* is a suite of [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to copy messages from Kafka Connect and write them to BigTable implementations, such as [Apache HBase](https://hbase.apache.org/) and [Google Cloud BigTable](https://cloud.google.com/bigtable/).
This suite contains three modules:

* `kafka-connect-bigtable-common` - The base BigTable sink connector.
* `kafka-connect-gcp-bigtable` - A specialization of the sink connector that works with [Google Cloud BigTable](https://cloud.google.com/bigtable/).
* `kafka-connect-hbase` - A specialization of the sink connector that works with [Apache HBase](https://hbase.apache.org/).

In the future we may choose to add other modules for specific BigTable implementations.

# Development

To build a development version you'll need a recent version of Kafka
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.

You can build *kafka-connect-bigtable* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-bigtable
- Issue Tracker: https://confluentinc.atlassian.net/projects/CC/issues