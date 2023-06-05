package org.apache.iotdb.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LookupSinkTest {
    public static void main(String[] args) {
        // setup environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // create left table (data source)
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
                .option("fields.Time_.start", "9995")
                .option("fields.Time_.end", "10005")
                .option("fields.voltage.min", "1")
                .option("fields.voltage.max", "5")
                .build();
        tableEnv.createTemporaryTable("leftTable", descriptor);

        // create right table (lookup table)
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

        tableEnv.createTemporaryTable("rightTable", iotdbDescriptor);

        // join
        String sql = "SELECT l.Time_, l.voltage, r.amperage " +
                "FROM (select *,PROCTIME() as proc_time from leftTable) AS l " +
                "JOIN rightTable FOR SYSTEM_TIME AS OF l.proc_time AS r " +
                "ON l.Time_ = r.Time_";
        Table joinedTable = tableEnv.sqlQuery(sql);

        // create sink table
        Schema sinkTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("voltage", DataTypes.FLOAT())
                .column("amperage", DataTypes.FLOAT())
                .build();
        TableDescriptor sinkDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(sinkTableSchema)
                .option("device", "root.test.flink.sink")
                .build();
        tableEnv.createTemporaryTable("sinkTable", sinkDescriptor);

        // insert
        joinedTable.insertInto("sinkTable").execute().print();
    }
}
