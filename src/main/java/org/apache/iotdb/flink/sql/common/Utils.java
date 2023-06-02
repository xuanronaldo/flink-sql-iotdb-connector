package org.apache.iotdb.flink.sql.common;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.iotdb.tsfile.read.common.Field;

import javax.activation.UnsupportedDataTypeException;

public class Utils {
    public static Object getValue(Field value, DataType dataType) throws UnsupportedDataTypeException {
        if (dataType.equals(DataTypes.INT())) {
            return value.getIntV();
        } else if (dataType.equals(DataTypes.BIGINT())) {
            return value.getLongV();
        } else if (dataType.equals(DataTypes.FLOAT())) {
            return value.getFloatV();
        } else if (dataType.equals(DataTypes.DOUBLE())) {
            return value.getDoubleV();
        } else if (dataType.equals(DataTypes.BOOLEAN())) {
            return value.getBoolV();
        } else if (dataType.equals(DataTypes.STRING())) {
            return value.getStringValue();
        } else {
            throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + dataType);
        }
    }

    public static Object getValue(RowData value, DataType dataType, int index) throws UnsupportedDataTypeException {
        if (dataType.equals(DataTypes.INT())) {
            return value.getInt(index);
        } else if (dataType.equals(DataTypes.BIGINT())) {
            return value.getLong(index);
        } else if (dataType.equals(DataTypes.FLOAT())) {
            return value.getFloat(index);
        } else if (dataType.equals(DataTypes.DOUBLE())) {
            return value.getDouble(index);
        } else if (dataType.equals(DataTypes.BOOLEAN())) {
            return value.getBoolean(index);
        } else if (dataType.equals(DataTypes.STRING())) {
            return value.getString(index);
        } else {
            throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + dataType);
        }
    }
}