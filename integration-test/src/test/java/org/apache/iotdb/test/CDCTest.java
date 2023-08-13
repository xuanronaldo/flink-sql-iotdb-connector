package org.apache.iotdb.test;

import org.apache.flink.table.api.*;

public class CDCTest {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // scan table
        Schema iotdbTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("s1", DataTypes.INT())
                .column("s2", DataTypes.BIGINT())
                .column("s3", DataTypes.FLOAT())
                .column("s4", DataTypes.DOUBLE())
                .build();

        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(iotdbTableSchema)
                .option("nodeUrls", "127.0.0.1:80")
                .option("device", "root.test.flink.cdc")
                .build();
        tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

        tableEnv.from("iotdbTable").execute().print();
    }
}
