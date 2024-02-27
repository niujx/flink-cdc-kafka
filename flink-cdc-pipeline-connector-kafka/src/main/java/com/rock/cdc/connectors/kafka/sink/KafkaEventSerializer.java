package com.rock.cdc.connectors.kafka.sink;


import com.google.common.collect.Maps;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.*;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.common.utils.SchemaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.htrace.shaded.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaEventSerializer implements KafkaRecordSerializationSchema<Event> {

    private final String prefix;
    private final ZoneId zoneId;
    private final Boolean includeSchema;
    private ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<TableId, TableInfo> tableInfoMap;


    public KafkaEventSerializer(String prefix, ZoneId zoneId, Boolean includeSchema) {
        this.prefix = prefix;
        this.zoneId = zoneId;
        this.includeSchema = includeSchema;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        tableInfoMap = Maps.newHashMap();
    }

    @SneakyThrows
    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Event event, KafkaSinkContext kafkaSinkContext, Long aLong) {

        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            Schema newSchema;
            if (event instanceof CreateTableEvent) {
                newSchema = ((CreateTableEvent) event).getSchema();
            } else {
                TableInfo tableInfo = tableInfoMap.get(tableId);
                if (tableInfo == null) {
                    throw new RuntimeException("schema of " + tableId + " is not existed.");
                }
                newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.schema, (SchemaChangeEvent) event);
            }
            TableInfo tableInfo = new TableInfo();
            tableInfo.schema = newSchema;
            tableInfo.fieldGetters = new RecordData.FieldGetter[newSchema.getColumnCount()];
            for (int i = 0; i < newSchema.getColumnCount(); i++) {
                tableInfo.fieldGetters[i] =
                        KafkaUtils.createFieldGetter(newSchema.getColumns().get(i).getType(), i, zoneId);
            }
            tableInfoMap.put(tableId, tableInfo);
        } else if (event instanceof DataChangeEvent) {
            return applyChangeDataEvent((DataChangeEvent) event);
        }
        return null;
    }

    private ProducerRecord<byte[], byte[]> applyChangeDataEvent(DataChangeEvent event) throws JsonProcessingException {
        TableId tableId = event.tableId();
        TableInfo tableInfo = tableInfoMap.get(tableId);
        Preconditions.checkNotNull(tableInfo, event.tableId() + " is not existed");
        Map<String, Object> before = null;
        Map<String, Object> after = null;
        Map<String, Object> current;
        String op;

        switch (event.op()) {
            case INSERT:
                after = serializerRecord(event.after(), tableInfo);
                op = "c";
                current = after;
                break;
            case UPDATE:
            case REPLACE:
                before = serializerRecord(event.before(), tableInfo);
                after = serializerRecord(event.after(), tableInfo);
                current = after;
                op = "u";
                break;
            case DELETE:
                before = serializerRecord(event.before(), tableInfo);
                op = "d";
                current = before;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }

        List<Object> keys = keys(current, tableInfo);
        KafkaDebeziumRecord.Source source = new KafkaDebeziumRecord.Source();
        source.setDatabase(tableId.getSchemaName());
        source.setTable(tableId.getTableName());

        KafkaDebeziumRecord kafkaRecord = new KafkaDebeziumRecord();
        kafkaRecord.setSource(source);
        kafkaRecord.setOp(op);
        kafkaRecord.setBefore(before);
        kafkaRecord.setAfter(after);
        kafkaRecord.setKeys(keys);
        kafkaRecord.setPkNames(tableInfo.schema.primaryKeys());
        kafkaRecord.setIngestionTimestamp(System.currentTimeMillis());

        if (includeSchema) {
            kafkaRecord.setSchema(tableInfo.schema);
        }

        //  kafkaRecord.setFields();

        return new ProducerRecord(topic(source), key(kafkaRecord), value(kafkaRecord));

    }

    private String topic(KafkaDebeziumRecord.Source source) {
        return (prefix + "-" + source.getDatabase() + "-" + source.getTable()).replace("_", "-");
    }

    private byte[] key(KafkaDebeziumRecord kafkaRecord) {
        return kafkaRecord.getKeys().stream().map(String::valueOf).collect(Collectors.joining("$")).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] value(KafkaDebeziumRecord kafkaRecord) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(kafkaRecord);
    }

    private List<Object> keys(Map<String, Object> current, TableInfo tableInfo) {
        return tableInfo.schema.primaryKeys()
                .stream().filter(current::containsKey)
                .map(current::get)
                .collect(Collectors.toList());
    }

    private Map<String, Object> serializerRecord(RecordData recordData, TableInfo tableInfo) {
        List<Column> columns = tableInfo.schema.getColumns();
        //before 在表的schema变更情况下，有可能不会对齐
//        Preconditions.checkState(
//                columns.size() == recordData.getArity(),
//                "Column size does not match the data size");
        Map<String, Object> record = new HashMap<>(recordData.getArity() + 1);
        for (int i = 0; i < recordData.getArity(); i++) {
            record.put(columns.get(i).getName(), tableInfo.fieldGetters[i].getFieldOrNull(recordData));
        }
        return record;
    }


    private static class TableInfo {
        Schema schema;
        RecordData.FieldGetter[] fieldGetters;
    }

}
