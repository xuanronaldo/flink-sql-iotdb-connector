package org.apache.iotdb.flink.sql.test;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LookupTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Schema dataGenTableSchema = Schema
                .newBuilder()
                .column("Time", DataTypes.BIGINT())
                .column("voltage", DataTypes.FLOAT())
                .build();
        TableDescriptor descriptor = TableDescriptor
                .forConnector("datagen")
                .schema(dataGenTableSchema)
                .option("rows-per-second", "1")
                .option("fields.Time.kind", "sequence")
                .option("fields.Time.start", "1")
                .option("fields.Time.end", "10000")
                .option("fields.voltage.min", "1")
                .option("fields.voltage.max", "5")
                .build();
        tableEnv.createTemporaryTable("dataGenTable", descriptor);

        tableEnv.createTemporaryTable("printTable", TableDescriptor
                .forConnector("print")
                .schema(dataGenTableSchema)
                .build());

        Table dataGenTable = tableEnv.from("dataGenTable");

        TablePipeline pipeline = dataGenTable.insertInto("printTable");
//        pipeline.printExplain();
        pipeline.execute();
    }
}
