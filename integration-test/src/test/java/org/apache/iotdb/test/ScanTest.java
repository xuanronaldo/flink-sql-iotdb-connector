package org.apache.iotdb.test;

import org.apache.flink.table.api.*;

public class ScanTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // scan table
        Schema iotdbTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("amperage", DataTypes.FLOAT())
                .column("voltage", DataTypes.FLOAT())
                .build();

        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(iotdbTableSchema)
                .option("nodeUrls", "127.0.0.1:6667")
                .option("device", "root.test.flink.sink")
                .build();
        tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

        tableEnv.from("iotdbTable").execute().print();
    }
}
