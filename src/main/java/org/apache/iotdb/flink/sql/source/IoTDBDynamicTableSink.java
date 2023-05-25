package org.apache.iotdb.flink.sql.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class IoTDBDynamicTableSink  implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return null;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
