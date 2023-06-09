package org.apache.iotdb.test;

import org.apache.flink.table.api.*;

public class SinkTest {
    public static void main(String[] args) {
        // setup environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // create data source table
        Schema dataGenTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("voltage", DataTypes.FLOAT())
                .build();
        TableDescriptor descriptor = TableDescriptor
                .forConnector("datagen")
                .schema(dataGenTableSchema)
                .option("rows-per-second", "1")
                .option("fields.Time_.kind", "sequence")
                .option("fields.Time_.start", "1")
                .option("fields.Time_.end", "5")
                .option("fields.voltage.min", "1")
                .option("fields.voltage.max", "5")
                .build();
        tableEnv.createTemporaryTable("dataGenTable", descriptor);
        Table dataGenTable = tableEnv.from("dataGenTable");

        // create iotdb sink table
        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(dataGenTableSchema)
                .option("device", "root.test.flink.sink")
                .build();
        tableEnv.createTemporaryTable("iotdbSinkTable", iotdbDescriptor);

        // insert data
        dataGenTable.insertInto("iotdbSinkTable").execute();
    }
}
