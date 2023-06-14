package org.apache.iotdb.flink.sql.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;

public class IoTDBUnboundedScanFunction extends RichSourceFunction<RowData> {
    public IoTDBUnboundedScanFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
