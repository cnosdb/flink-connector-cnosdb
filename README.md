# Flink CnosDB Connector

This connector provides a sink that can send data to [CnosDB](https://www.cnosdb.com/). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>com.cnosdb</groupId>
      <artifactId>flink-connector-cnosdb</artifactId>
      <version>1.0</version>
    </dependency>

*Version Compatibility*: This module is compatible with CnosDB 2.1.0+   
*Requirements*: Java 1.8+

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/linking.html).

## Installing CnosDB
Follow the instructions from the [CnosDB](https://github.com/cnosdb/cnosdb).

## Examples

### JAVA API

    DataStream<CnosDBPoint> dataStream = ...
    CnosDBConfig cnosDBConfig = CnosDBConfig.builder()
    dataStream.addSink(new CnosDBSink(cnosDBConfig));

See end-to-end examples at [CnosDB Examples](https://github.com/apache/bahir-flink/tree/master/flink-connector-cnosdb/examples)