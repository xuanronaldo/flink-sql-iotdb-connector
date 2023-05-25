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
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.concurrent.TimeUnit;

public class IoTDBLookupFunction extends TableFunction<RowData> {
    private final Session session;
    private final TableSchema schema;
    private final int cacheMaxRows;
    private final int cacheTtlSec;


    private transient Cache<RowData, RowData> cache;

    public IoTDBLookupFunction(ReadableConfig options, Session session, TableSchema schema) {
        this.session = session;
        this.schema = schema;

        cacheMaxRows = options.get(ConfigOptions
                .key("lookup.cache.max-rows")
                .intType()
                .noDefaultValue());

        cacheTtlSec = options.get(ConfigOptions
                .key("lookup.cache.ttl-sec")
                .intType()
                .noDefaultValue());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

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

        String[] fieldNames = schema.getFieldNames();
        long timestamp = lookupKey.getLong(0);
//        String sql = String.format("select %s from %s where time=%s", measurements, devices, key);
//        SessionDataSet dataSet = session.executeQueryStatement(sql);
    }


}
