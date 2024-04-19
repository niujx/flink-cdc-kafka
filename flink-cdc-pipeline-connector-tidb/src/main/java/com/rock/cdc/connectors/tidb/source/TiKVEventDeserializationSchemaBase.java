package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.tidb.table.TiKVDeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

import com.ververica.cdc.common.data.TimestampData;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TiKVEventDeserializationSchemaBase implements Serializable {

    protected final TiTableInfo tableInfo;
    protected final TiKVDeserializationRuntimeConverter physicalConverter;
    protected final Schema schema;
    protected final TableId tableId;

    public TiKVEventDeserializationSchemaBase(TiTableInfo tableInfo,TableId tableId) {
        this.tableInfo = tableInfo;
        this.schema = TiDBSchemaUtils.createSchema(tableInfo);
        this.physicalConverter = createConverter(getRowType());
        this.tableId = tableId;
    }

    private RowType getRowType() {
        List<TiColumnInfo> columns = tableInfo.getColumns();
        String[] columnNames = columns.stream().map(TiColumnInfo::getName).toArray(String[]::new);
        DataType[] logicalTypes = columns.stream().map(TiDBTypesUtils::toDateType).toArray(DataType[]::new);
        return RowType.of(logicalTypes, columnNames);
    }

    protected TiKVDeserializationRuntimeConverter createConverter(DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    public TiKVDeserializationRuntimeConverter createNotNullConverter(DataType type) {

        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return new TiKVDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo schema,
                            org.tikv.common.types.DataType dataType) {

                        return Byte.parseByte(object.toString());
                    }
                };
            case SMALLINT:
                return new TiKVDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo schema,
                            org.tikv.common.types.DataType dataType) {
                        return Short.parseShort(object.toString());
                    }
                };
            case INTEGER:
                return convertToInt();
            case BIGINT:
                return convertToLong();
            case DATE:
                return convertToDate();
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp();
            case FLOAT:
                return convertToFloat();
            case DOUBLE:
                return convertToDouble();
            case CHAR:
            case VARCHAR:
            case ARRAY:
                return convertToString();
            case BINARY:
            case VARBINARY:
                return convertToBinary();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private TiKVDeserializationRuntimeConverter convertToBoolean() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Boolean) {
                    return object;
                } else if (object instanceof Long) {
                    return (Long) object == 1;
                } else if (object instanceof Byte) {
                    return (byte) object == 1;
                } else if (object instanceof Short) {
                    return (short) object == 1;
                } else {
                    return Boolean.parseBoolean(object.toString());
                }
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToInt() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Integer) {
                    return object;
                } else if (object instanceof Long) {
                    return dataType.isUnsigned()
                            ? Integer.valueOf(Short.toUnsignedInt(((Long) object).shortValue()))
                            : ((Long) object).intValue();
                } else {
                    return Integer.parseInt(object.toString());
                }
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToLong() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Integer) {
                    return ((Integer) object).longValue();
                } else if (object instanceof Long) {
                    return object;
                } else {
                    return Long.parseLong(object.toString());
                }
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToDouble() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Float) {
                    return ((Float) object).doubleValue();
                } else if (object instanceof Double) {
                    return object;
                } else {
                    return Double.parseDouble(object.toString());
                }
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToFloat() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Float) {
                    return object;
                } else if (object instanceof Double) {
                    return ((Double) object).floatValue();
                } else {
                    return Float.parseFloat(object.toString());
                }
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToDate() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                return (int) TemporalConversions.toLocalDate(object).toEpochDay();
            }
        };
    }

    private static TiKVDeserializationRuntimeConverter convertToTime() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Long) {
                    return (int) ((Long) object / 1000_000);
                }
                return TimestampData.fromMillis(TemporalConversions.toLocalTime(object).toSecondOfDay() * 1000);
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToTimestamp() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {

                switch (dataType.getType()) {
                    case TypeTimestamp:
                        if (object instanceof Timestamp) {
                            return TimestampData.fromTimestamp((Timestamp) object);
                        }
                        break;
                    case TypeDatetime:
                        if (object instanceof Timestamp) {
                            return TimestampData.fromLocalDateTime(
                                    ((Timestamp) object).toLocalDateTime());
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unable to convert to TimestampData from unexpected value '"
                                        + object
                                        + "' of type "
                                        + object.getClass().getName());
                }
                return object;
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToLocalTimeZoneTimestamp() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Timestamp) {
                    return TimestampData.fromTimestamp(((Timestamp) object));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + object
                                + "' of type "
                                + object.getClass().getName());
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToString() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof byte[]) {
                    return BinaryStringData.fromBytes((byte[]) object);
                }
                return BinaryStringData.fromString(object.toString());
            }
        };
    }

    private TiKVDeserializationRuntimeConverter convertToBinary() {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof byte[]) {
                    return object;
                } else if (object instanceof String) {
                    return ((String) object).getBytes();
                } else if (object instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + object.getClass().getSimpleName());
                }
            }
        };
    }

    /**
     * Deal with unsigned column's value.
     */
    public Object dealUnsignedColumnValue(
            org.tikv.common.types.DataType dataType, Object object) {
        // For more information about numeric columns with unsigned, please refer link
        // https://docs.pingcap.com/tidb/stable/data-type-numeric.
        switch (dataType.getType()) {
            case TypeTiny:
                return (short) Byte.toUnsignedInt(((Long) object).byteValue());
            case TypeShort:
                return Short.toUnsignedInt(((Long) object).shortValue());
            case TypeInt24:
                return (((Long) object).intValue()) & 0xffffff;
            case TypeLong:
                return Integer.toUnsignedLong(((Long) object).intValue());
            case TypeLonglong:
                return new BigDecimal(Long.toUnsignedString(((Long) object)));
            default:
                return object;
        }
    }

    private TiKVDeserializationRuntimeConverter createDecimalConverter(
            DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                BigDecimal bigDecimal;
                if (object instanceof String) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Long) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Double) {
                    bigDecimal = BigDecimal.valueOf((Double) object);
                } else if (object instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) object;
                } else {
                    throw new IllegalArgumentException(
                            "Unable to convert to decimal from unexpected value '"
                                    + object
                                    + "' of type "
                                    + object.getClass());
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }

    private TiKVDeserializationRuntimeConverter createRowConverter(RowType rowType) {
        TiKVDeserializationRuntimeConverter[] fidelConverters = schema.getColumns().stream()
                .map(column -> this.createConverter(column.getType()))
                .toArray(TiKVDeserializationRuntimeConverter[]::new);

        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo tableInfo, org.tikv.common.types.DataType dataType)
                    throws Exception {
                final DataType[] dataTypes = rowType.getFieldTypes().toArray(new DataType[0]);
                BinaryRecordDataGenerator dataGenerator = new BinaryRecordDataGenerator(dataTypes);
                final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

                int arity = fieldNames.length;
                Object[] fields = new Object[schema.getColumnCount()];
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    TiColumnInfo columnInfo = tableInfo.getColumn(fieldName);
                    if (columnInfo == null) {
                        fields[i] = null;
                    } else {
                        int offset = columnInfo.getOffset();
                        org.tikv.common.types.DataType type = columnInfo.getType();
                        Object convertedField =
                                convertField(
                                        fidelConverters[i],
                                        tableInfo,
                                        type,
                                        ((Object[]) object)[offset]);
                        fields[i] = convertedField;
                    }
                }

                return dataGenerator.generate(fields);
            }
        };
    }

    private Object convertField(
            TiKVDeserializationRuntimeConverter fieldConverter,
            TiTableInfo tableInfo,
            org.tikv.common.types.DataType dataType,
            Object fieldValue)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            if (dataType.isUnsigned()) {
                fieldValue = dealUnsignedColumnValue(dataType, fieldValue);
            }
            return fieldConverter.convert(fieldValue, tableInfo, dataType);
        }
    }

    private TiKVDeserializationRuntimeConverter wrapIntoNullableConverter(
            TiKVDeserializationRuntimeConverter converter) {
        return new TiKVDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType)
                    throws Exception {
                if (object == null) {
                    return null;
                }
                return converter.convert(object, schema, dataType);
            }
        };
    }
}
