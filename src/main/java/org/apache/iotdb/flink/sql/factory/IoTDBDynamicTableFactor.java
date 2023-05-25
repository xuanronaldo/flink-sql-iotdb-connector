package org.apache.iotdb.flink.sql.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IoTDBDynamicTableFactor implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final ConfigOption<String> NODE_URLS = ConfigOptions
            .key("nodeUrls")
            .stringType()
            .defaultValue("127.0.0.1:6667");
    public static final ConfigOption<String> USER = ConfigOptions
            .key("user")
            .stringType()
            .defaultValue("root");
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .defaultValue("root");
    public static final ConfigOption<Integer> DEVICE = ConfigOptions
            .key("device")
            .intType()
            .defaultValue(-1);
    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .intType()
            .defaultValue(-1);
    public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = ConfigOptions
            .key("lookup.cache.ttl-sec")
            .intType()
            .defaultValue(-1);
    protected static Session session;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(NODE_URLS);
        requiredOptions.add(DEVICE);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USER);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL_SEC);

        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }

    protected void validateOptions(ReadableConfig options) {
        List<String> nodeUrls = Arrays.asList(options.get(NODE_URLS).toString().split(","));
        String user = options.get(USER).toString();
        String password = options.get(PASSWORD).toString();

        try {
            session = new Session.Builder().nodeUrls(nodeUrls).username(user).password(password).build();
            session.open(false);
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }
}
