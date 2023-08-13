package org.apache.iotdb.flink.sql.function;

import javafx.util.Pair;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

public class IoTDBCDCSourceFunction<RowData> extends RichSourceFunction<RowData> {
    private ArrayBlockingQueue<Tablet> tablets = new ArrayBlockingQueue<Tablet>(5);
    private WebSocketClient socketClient;
    private Thread consumerThread;

    private final List<Tuple2<String, DataType>> SCHEMA;
    private final List<String> NODE_URLS;
    private final String DEVICE;
    private final List<String> MEASUREMENTS;

    public IoTDBCDCSourceFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
        SCHEMA = schemaWrapper.getSchema();
        NODE_URLS = Arrays.asList(options.get(Options.NODE_URLS).split(","));
        DEVICE = options.get(Options.DEVICE);
        MEASUREMENTS =
                SCHEMA.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        for (String nodeUrl : NODE_URLS) {
            socketClient = new WebSocketClient(new URI(String.format("ws://%s", nodeUrl))) {
                @Override
                public void onOpen(ServerHandshake serverHandshake) {

                }

                @Override
                public void onMessage(String s) {

                }

                @Override
                public void onMessage(ByteBuffer bytes) {
                    super.onMessage(bytes);
                    Tablet tablet = Tablet.deserialize(bytes);
                    try {
                        tablets.put(tablet);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onClose(int i, String s, boolean b) {

                }

                @Override
                public void onError(Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            };
        }

    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        socketClient.connect();
        consumerThread = new Thread() {
            @Override
            public void run() {
                try {
                    collectTablet(tablets.take(), ctx);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        consumerThread.start();
    }

    @Override
    public void cancel() {
        if (socketClient != null) {
            socketClient.close();
        }
        consumerThread.stop();
        tablets.clear();
    }

    public void collectTablet(Tablet tablet, SourceContext<RowData> ctx) {
        List<MeasurementSchema> schemas = tablet.getSchemas();
        int rowSize = tablet.rowSize;
        HashMap<String, Pair<BitMap, Object[]>> values = new HashMap<>();
        for (MeasurementSchema schema : schemas) {
            String measurement = schema.getMeasurementId();
            values.put(measurement, new Pair<>(tablet.bitMaps[schemas.indexOf(schema)], (Object[]) tablet.values[schemas.indexOf(schema)]));
        }
        for (int i = 0; i < rowSize; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(tablet.timestamps[i]);
            for (String measurement : MEASUREMENTS) {
                if (values.get(measurement).getKey().isMarked(i)) {
                    row.add(values.get(measurement).getValue()[i]);
                } else {
                    row.add(null);
                }
            }
            RowData rowData = (RowData) GenericRowData.of(row.toArray());
            ctx.collect(rowData);
        }
    }
}
