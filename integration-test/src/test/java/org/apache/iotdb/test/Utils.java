package org.apache.iotdb.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {
    public static Session session;

    static {
        session = new Session.Builder().build();
        try {
            session.open();
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }
    public static void insertTestData() throws IoTDBConnectionException, StatementExecutionException {
        ArrayList<Long> timestamps = new ArrayList<>();
        List<List<String>> measurements = new ArrayList<>();
        List<List<TSDataType>> dataTypes = new ArrayList<>();
        List<List<Object>> values = new ArrayList<>();

        ArrayList<String> measurement = new ArrayList<String>() {{add("amperage");}};
        ArrayList<TSDataType> dataType = new ArrayList<TSDataType>() {{add(TSDataType.FLOAT);}};

        Random random = new Random();
        for (long i = 1; i <= 10000l; i++) {
            timestamps.add(i);
            measurements.add(measurement);
            dataTypes.add(dataType);
            ArrayList<Object> value = new ArrayList<Object>() {{add(random.nextFloat());}};
            values.add(value);
        }

        // String var1, List<Long> var2, List<List<String>> var3, List<List<TSDataType>> var4, List<List<Object>> var5
        session.insertRecordsOfOneDevice("root.test.flink.lookup", timestamps, measurements, dataTypes, values);

        session.close();
    }

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        insertTestData();
    }
}
