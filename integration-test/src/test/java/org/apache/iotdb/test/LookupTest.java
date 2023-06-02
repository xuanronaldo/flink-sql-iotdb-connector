package org.apache.iotdb.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;

public class LookupTest {
    public static void main(String[] args) {
        // setup environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // register left table
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
                .option("fields.Time_.end", "10000")
                .option("fields.voltage.min", "1")
                .option("fields.voltage.max", "5")
                .build();
        tableEnv.createTemporaryTable("dataGenTable", descriptor);

        // register right table
        Schema iotdbTableSchema = Schema
                .newBuilder()
                .column("Time_", DataTypes.BIGINT())
                .column("amperage", DataTypes.FLOAT())
                .column("amperage1", DataTypes.FLOAT())
                .build();

        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")
                .schema(iotdbTableSchema)
                .option("nodeUrls", "127.0.0.1:6667")
                .option("device", "root.test.flink.lookup")
                .build();

        tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

        // join
        String sql = "SELECT l.Time_, l.voltage, r.amperage, r.amperage1 " +
                "FROM (select *,PROCTIME() as proc_time from dataGenTable) AS l " +
                "JOIN iotdbTable FOR SYSTEM_TIME AS OF l.proc_time AS r " +
                "ON l.Time_ = r.Time_";
        tableEnv.sqlQuery(sql)
                .execute().
                print();
    }
}
