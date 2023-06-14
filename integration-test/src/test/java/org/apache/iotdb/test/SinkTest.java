package org.apache.iotdb.test;

import org.apache.flink.table.api.*;

public class SinkTest {
    public static void main(String[] args) {
        // setup environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // create source table
        Schema schema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("amperage", DataTypes.FLOAT())
                .column("voltage", DataTypes.FLOAT())
                .build();

        TableDescriptor sourceDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(schema)
                .option("nodeUrls", "127.0.0.1:6667")
                .option("device", "root.test.flink.sink")
                .build();
        tableEnv.createTemporaryTable("sourceTable", sourceDescriptor);
        Table sourceTable = tableEnv.from("sourceTable");

        // create sink table
        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(schema)
                .option("device", "root.test.flink.sink")
                .build();
        tableEnv.createTemporaryTable("iotdbSinkTable", iotdbDescriptor);

        // insert data
        sourceTable.insertInto("iotdbSinkTable").execute();
    }
}
