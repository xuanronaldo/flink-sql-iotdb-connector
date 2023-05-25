package org.apache.iotdb.flink.sql.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.iotdb.session.Session;

public class IoTDBDynamicTableSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;
    private final Session session;

    public IoTDBDynamicTableSource(ReadableConfig options, TableSchema schema, Session session) {
        this.options = options;
        this.schema = schema;
        this.session = session;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
