package com.rock.cdc.connectors.mongodb.source;

import com.google.common.collect.Lists;
import com.mongodb.client.model.changestream.OperationType;
import com.rock.cdc.connectors.mongodb.utils.MongodbTypeUtils;
import com.rock.cdc.connectors.mongodb.utils.MongodbValueUtils;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.*;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongodbEventDeserializer implements EventDeserializer<SourceRecord>, DebeziumDeserializationSchema<Event> {

    private static final Map<DataType, DeserializationRuntimeConverter> CONVERTERS =
            new ConcurrentHashMap<>();

    private MongodbValueUtils mongodbValueUtils;


    public MongodbEventDeserializer(MongodbValueUtils mongodbValueUtils) {
        this.mongodbValueUtils = mongodbValueUtils;
    }

    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        deserialize(record).forEach(out::collect);
    }

    @Override
    public List<? extends Event> deserialize(SourceRecord record) throws Exception {
        return deserializeChangeRecord(record);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }


    public List<ChangeEvent> deserializeChangeRecord(SourceRecord record) throws Exception {
        Struct value = (Struct) record.value();
        org.apache.kafka.connect.data.Schema valueSchema = record.valueSchema();
        OperationType op = operationTypeFor(record);
        TableId tableId = getTableId(record);

        BsonDocument documentKey =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.DOCUMENT_KEY_FIELD);
        BsonDocument fullDocument =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_FIELD);

        //6.0以下不支持 获取before的数据
        BsonDocument fullDocumentBeforeChange =
                extractBsonDocument(
                        value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_BEFORE_CHANGE_FIELD);
        if (op == OperationType.DELETE) {
            fullDocument = documentKey;
        }

        Schema schema = MongodbTypeUtils.createSchema(fullDocument);
        List<ChangeEvent> changeEvents = Lists.newArrayList(new CreateTableEvent(tableId, schema));

        RecordData after;
        if (op != OperationType.UPDATE && op != OperationType.REPLACE) {
            if (op == OperationType.DELETE) {
                after = extractAfterDataRecord(fullDocument, schema);
                changeEvents.add(DataChangeEvent.deleteEvent(tableId, after, Collections.emptyMap()));
            } else if (op == OperationType.INSERT) {
                after = extractAfterDataRecord(fullDocument, schema);
                changeEvents.add(DataChangeEvent.insertEvent(tableId, after, Collections.emptyMap()));
            }
        } else {
            after = extractAfterDataRecord(fullDocument, schema);
            changeEvents.add(DataChangeEvent.updateEvent(tableId, after, after, Collections.emptyMap()));
        }

        return changeEvents;
    }


    protected RecordData extractAfterDataRecord(BsonValue bsonValue, Schema schema) throws Exception {
        return extractDataRecord(bsonValue, schema);
    }

    private RecordData extractDataRecord(BsonValue value, Schema valueSchema) throws Exception {
        RowType rowType = RowType.of(valueSchema.getColumnDataTypes().toArray(new DataType[]{}),
                valueSchema.getColumnNames().toArray(new String[]{}));
        return (RecordData) getOrCreateConverter(rowType)
                .convert(value, valueSchema);
    }


    private DeserializationRuntimeConverter getOrCreateConverter(DataType type) {
        return CONVERTERS.computeIfAbsent(type, this::createConverter);
    }

    private DeserializationRuntimeConverter createConverter(DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    protected OperationType operationTypeFor(SourceRecord record) {
        Struct value = (Struct) record.value();
        return OperationType.fromString(value.getString(MongoDBEnvelope.OPERATION_TYPE_FIELD));
    }

    protected TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        return TableId.tableId(extractDatabase(value), extractTableName(value));
    }

    protected String extractTableName(Struct value) {
        final Struct ns = (Struct) value.get(MongoDBEnvelope.NAMESPACE_FIELD);
        return (String) ns.get(MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD);
    }

    protected String extractDatabase(Struct value) {
        final Struct ns = (Struct) value.get(MongoDBEnvelope.NAMESPACE_FIELD);
        return (String) ns.get(MongoDBEnvelope.NAMESPACE_DATABASE_FIELD);
    }

    protected BsonDocument extractBsonDocument(Struct value, org.apache.kafka.connect.data.Schema valueSchema, String fieldName) {
        if (valueSchema.field(fieldName) != null) {
            String docString = value.getString(fieldName);
            if (docString != null) {
                return BsonDocument.parse(docString);
            }
        }
        return null;
    }


    protected DeserializationRuntimeConverter createNotNullConverter(DataType type) {
        switch (type.getTypeRoot()) {
            case DOUBLE:
                return this::convertToDouble;
            case VARCHAR:
                return this::convertToString;
            case ROW:
                return this::convertToRecord;
            case BINARY:
                return this::convertToBinary;
            case BOOLEAN:
                return this::convertToBoolean;
            case INTEGER:
                return this::convertToInt;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case BIGINT:
                return this::convertToLong;
            case DECIMAL:
                return this::convertToDecimal;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }


    protected Object convertToBoolean(BsonValue dbzObj, Schema schema) {
        return dbzObj.asBoolean().getValue();
    }

    protected Object convertToInt(BsonValue dbzObj, Schema schema) {
        return dbzObj.asInt32().intValue();
    }

    protected Object convertToLong(BsonValue dbzObj, Schema schema) {
        return dbzObj.asInt64().getValue();
    }

    protected Object convertToDouble(BsonValue dbzObj, Schema schema) {
        return dbzObj.asDouble().getValue();
    }

    protected Object convertToTimestamp(BsonValue dbzObj, Schema schema) {
        if (dbzObj.isDateTime()) {
            return TimestampData.fromMillis(dbzObj.asDateTime().getValue());
        }
        long value = dbzObj.asTimestamp().getValue();
        return TimestampData.fromMillis(value * 1000L);
    }

    @SneakyThrows
    protected Object convertToString(BsonValue dbzObj, Schema schema) {
        String stringValue;
        if (dbzObj.isObjectId()) {
            stringValue = dbzObj.asObjectId().getValue().toString();
        }else if(dbzObj.isDocument()){
            stringValue =  dbzObj.asDocument().toJson();
        } else if (dbzObj.isArray()) {
            //flink cdc pipline 现在不支持array对象处理，只能转换成字符串
            List<Object> arrays = Lists.newArrayList();
            for (BsonValue bsonValue : dbzObj.asArray()) {
                Object value;
                if(bsonValue instanceof BsonDocument){
                    value= mongodbValueUtils.extractColumns(bsonValue.asDocument());
                }else{
                     value = mongodbValueUtils.toValue(bsonValue);
                }
                arrays.add(value);
            }
            stringValue  = new ObjectMapper().writeValueAsString(arrays);
            } else {
            stringValue = dbzObj.asString().getValue();
        }
        return BinaryStringData.fromString(stringValue);
    }

    protected Object convertToBinary(BsonValue dbzObj, Schema schema) {
        return dbzObj.asBinary().getData();
    }

    protected Object convertToDecimal(BsonValue dbzObj, Schema schema) {
        BigDecimal bigDecimal = dbzObj.asDecimal128().getValue().bigDecimalValue();
        return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
    }

    protected Object convertToRecord(BsonValue dbzObj, Schema schema)
            throws Exception {

        BsonDocument document = dbzObj.asDocument();
        DataType[] dataTypes = schema.getColumnDataTypes().toArray(new DataType[0]);
        BinaryRecordDataGenerator dataGenerator = new BinaryRecordDataGenerator(dataTypes);

        Object[] fields = new Object[schema.getColumnCount()];
        String[] fieldNames = schema.getColumns().stream().map(Column::getName).toArray(String[]::new);
        DeserializationRuntimeConverter[] fidelConverters = schema.getColumns().stream()
                .map(column -> this.getOrCreateConverter(column.getType()))
                .toArray(DeserializationRuntimeConverter[]::new);

        int arity = fieldNames.length;

        for (int i = 0; i < arity; i++) {
            String fieldName = fieldNames[i];
            BsonValue bsonValue = document.get(fieldName);
            if (bsonValue == null) {
                fields[i] = null;
            } else {
                Schema filedSchema = null;
                if (bsonValue.isDocument()) {
                    filedSchema = MongodbTypeUtils.createSchema(bsonValue.asDocument());
                }
                fields[i] = convertField(fidelConverters[i], bsonValue, filedSchema);
            }
        }

        return dataGenerator.generate(fields);
    }

    private static Object convertField(
            DeserializationRuntimeConverter fieldConverter, BsonValue fieldValue, Schema fieldSchema)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, fieldSchema);
        }
    }

    private static DeserializationRuntimeConverter wrapIntoNullableConverter(
            DeserializationRuntimeConverter converter) {
        return (bsonValue, schema) -> {
            if (bsonValue == null) {
                return null;
            }
            return converter.convert(bsonValue, schema);
        };
    }


    private interface DeserializationRuntimeConverter {
        Object convert(BsonValue bsonValue, Schema schema) throws Exception;
    }


}
