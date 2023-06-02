package org.apache.iotdb.flink.sql.provider;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.*;
import java.util.stream.Collectors;

public class IoTDBSinkFunction implements SinkFunction<RowData> {
    private final List<Tuple2<String, DataType>> SCHEMA;
    private final List<String> NODE_URLS;
    private final String USER;
    private final String PASSWORD;
    private final String DEVICE;
    private final List<String> MEASUREMENTS;
    private final List<TSDataType> DATA_TYPES;
    private final Map<DataType, TSDataType> TYPE_MAP = new HashMap<>() {{
        put(DataTypes.INT(), TSDataType.INT32);
        put(DataTypes.BIGINT(), TSDataType.INT64);
        put(DataTypes.FLOAT(), TSDataType.FLOAT);
        put(DataTypes.DOUBLE(), TSDataType.DOUBLE);
        put(DataTypes.BOOLEAN(), TSDataType.BOOLEAN);
        put(DataTypes.STRING(), TSDataType.TEXT);
    }};

    private final Session SESSION;

    public IoTDBSinkFunction(ReadableConfig options, SchemaWrapper schemaWrapper) throws IoTDBConnectionException {
        this.SCHEMA = schemaWrapper.getSchema();

        NODE_URLS = Arrays.asList(options.get(Options.NODE_URLS).split(","));

        USER = options.get(Options.USER);

        PASSWORD = options.get(Options.PASSWORD);

        DEVICE = options.get(Options.DEVICE);

        SESSION = new Session
                .Builder()
                .nodeUrls(NODE_URLS)
                .username(USER)
                .password(PASSWORD)
                .build();

        // get measurements and data types from schema
        MEASUREMENTS = SCHEMA.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
        DATA_TYPES = SCHEMA.stream().map(field -> TYPE_MAP.get(field.f1)).collect(Collectors.toList());
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // open the session if the session has not been opened
        try {
            SESSION.getTimeZone();
        } catch (Exception e) {
            SESSION.open(false);
        }
        long timestamp = value.getLong(0);
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 1; i < MEASUREMENTS.size(); i++) {
            values.add(Utils.getValue(value, SCHEMA.get(i).f1, i));
        }
        // String var1, long var2, List<String> var4, List<TSDataType> var5, List<Object> var6
//        session.insertRecord();

    }

    @Override
    public void finish() throws Exception {
        if (SESSION != null) {
            SESSION.close();
        }
    }
}
