package org.apache.iotdb.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ScanTest {
    public static void main(String[] args) {
        // setup environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // scan table
        Schema iotdbTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("amperage", DataTypes.FLOAT())
                .build();

        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(iotdbTableSchema)
                .option("nodeUrls", "127.0.0.1:6667")
                .option("device", "root.test.flink.lookup")
                .build();
        tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

        // output
        TableDescriptor printDescriptor = TableDescriptor
                .forConnector("print")
                .schema(iotdbTableSchema)
                .build();
        tableEnv.createTemporaryTable("printTable", printDescriptor);
        tableEnv.from("iotdbTable").insertInto("printTable").execute().print();
    }
}
