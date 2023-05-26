package org.apache.iotdb.flink.sql.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.iotdb.flink.sql.provider.IoTDBLookupFunction;

public class IoTDBDynamicTableSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;

    public IoTDBDynamicTableSource(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return TableFunctionProvider.of(new IoTDBLookupFunction(options, schema));
    }

    @Override
    public DynamicTableSource copy() {
        return new IoTDBDynamicTableSource(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "IoTDB Dynamic Table Source";
    }
}
