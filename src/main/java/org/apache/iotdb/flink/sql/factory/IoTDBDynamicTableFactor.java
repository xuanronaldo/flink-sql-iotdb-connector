package org.apache.iotdb.flink.sql.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.source.IoTDBDynamicTableSink;
import org.apache.iotdb.flink.sql.source.IoTDBDynamicTableSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IoTDBDynamicTableFactor implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();
        return new IoTDBDynamicTableSource(options, schema);
    }

    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(Options.NODE_URLS);
        requiredOptions.add(Options.DEVICE);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(Options.USER);
        optionalOptions.add(Options.PASSWORD);
        optionalOptions.add(Options.LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(Options.LOOKUP_CACHE_TTL_SEC);

        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();
        return new IoTDBDynamicTableSink(options, schema);
    }

    protected void validateOptions(ReadableConfig options) {
        List<String> nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).toString().split(","));
        String user = options.get(Options.USER).toString();
        String password = options.get(Options.PASSWORD).toString();
    }
}
