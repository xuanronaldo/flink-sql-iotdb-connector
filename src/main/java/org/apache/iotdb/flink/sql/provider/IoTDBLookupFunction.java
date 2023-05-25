package org.apache.iotdb.flink.sql.provider;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.curator5.com.google.common.cache.Cache;
import org.apache.flink.shaded.curator5.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class IoTDBLookupFunction extends TableFunction<RowData> {
    private final TableSchema schema;
    private final int cacheMaxRows;
    private final int cacheTtlSec;
    private final List<String> nodeUrls;
    private final String user;
    private final String password;
    private final String device;

    private Session session;

    private transient Cache<RowData, RowData> cache;

    public IoTDBLookupFunction(ReadableConfig options, TableSchema schema) {
        this.schema = schema;

        cacheMaxRows = options.get(ConfigOptions
                .key("lookup.cache.max-rows")
                .intType()
                .noDefaultValue());

        cacheTtlSec = options.get(ConfigOptions
                .key("lookup.cache.ttl-sec")
                .intType()
                .noDefaultValue());

        nodeUrls = Arrays.asList(options.get(ConfigOptions
                .key("nodeUrls")
                .stringType()
                .noDefaultValue()).split(","));

        user = options.get(ConfigOptions
                .key("user")
                .stringType()
                .noDefaultValue());

        password = options.get(ConfigOptions
                .key("password")
                .stringType()
                .noDefaultValue());

        device = options.get(ConfigOptions
                .key("device")
                .stringType()
                .noDefaultValue());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        session = new Session.Builder().nodeUrls(nodeUrls).username(user).password(password).build();
        session.open(false);

        if (cacheMaxRows > 0 && cacheTtlSec > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterAccess(cacheTtlSec, TimeUnit.SECONDS)
                    .maximumSize(cacheMaxRows)
                    .build();
        }
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            cache.invalidateAll();
        }
        if (session != null) {
            session.close();
        }
        super.close();
    }

    public void eval(Object obj) throws IoTDBConnectionException, StatementExecutionException {
        RowData lookupKey = GenericRowData.of(obj);
        if (cache != null) {
            RowData cacheRow = cache.getIfPresent(lookupKey);
            if (cacheRow != null) {
                collect(cacheRow);
                return;
            }
        }

        long timestamp = lookupKey.getLong(0);

        List<String> fieldNames = Arrays.asList(schema.getFieldNames());
        fieldNames.remove("Time");
        String measurements = String.join(",", fieldNames);

        String sql = String.format("select %s from %s where time=%d", measurements, device, timestamp);
        SessionDataSet dataSet = session.executeQueryStatement(sql);
        List<String> columnNames = dataSet.getColumnNames();
        RowRecord record = dataSet.next();
        List<Field> fields = record.getFields();

        ArrayList<Object> values = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            if ("Time".equals(fieldName)) {
                continue;
            }
            DataType dataType = schema.getFieldDataType(fieldName).get();
            Field field = fields.get(columnNames.indexOf(fieldName));
        }
    }

    private Object getValue(Field value, DataType dataType) {

        return null;
    }

}
