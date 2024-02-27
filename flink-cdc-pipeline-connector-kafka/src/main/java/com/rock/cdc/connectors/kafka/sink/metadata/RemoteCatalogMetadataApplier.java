package com.rock.cdc.connectors.kafka.sink.metadata;

import com.rock.cdc.connectors.kafka.sink.CatalogConfig;
import com.ververica.cdc.common.event.*;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataType;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.htrace.shaded.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class RemoteCatalogMetadataApplier extends BaseCatalogMetadataApplier {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String catalogMetaTopic;
    private transient KafkaProducer<String, ChangeSchemaRequest> producer;
    private boolean open;


    public RemoteCatalogMetadataApplier(CatalogConfig catalogConfig) {
        super(catalogConfig);
        this.catalogMetaTopic = catalogConfig.getTopic();
    }

    @SneakyThrows
    @Override
    protected void applyCreateTable(CreateTableEvent createTableEvent) {
        SchemaChange schemaChange = SchemaChange.of(getTableName(createTableEvent.tableId()));
        schemaChange.setPrimaryKeys(createTableEvent.getSchema().primaryKeys());
        schemaChange.setCreateColumns(createTableEvent.getSchema().getColumns());
        request("CREATE", schemaChange);
    }

    @SneakyThrows
    @Override
    protected void applyAddColumn(AddColumnEvent addColumnEvent) {
        SchemaChange schemaChange = SchemaChange.of(getTableName(addColumnEvent.tableId()));
        List<Column>  addColumns= addColumnEvent.getAddedColumns().stream().map(c -> c.getAddColumn()).collect(Collectors.toList());
        schemaChange.setAddColumn(addColumns);
        request("ADD", schemaChange);
    }

    @SneakyThrows
    @Override
    protected void applyDropColumn(DropColumnEvent dropColumnEvent) {
        SchemaChange schemaChange = SchemaChange.of(getTableName(dropColumnEvent.tableId()));
        schemaChange.setDroppedColumns(dropColumnEvent.getDroppedColumns());
        request("DROP", schemaChange);

    }

    @SneakyThrows
    @Override
    protected void applyRenameColumn(RenameColumnEvent renameColumnEvent) {
        SchemaChange schemaChange = SchemaChange.of(getTableName(renameColumnEvent.tableId()));
        schemaChange.setRenameMapping(renameColumnEvent.getNameMapping());
        request("RENAME", schemaChange);

    }

    @SneakyThrows
    @Override
    protected void applyAlterColumn(AlterColumnTypeEvent alterColumnTypeEvent) {
        SchemaChange schemaChange = SchemaChange.of(getTableName(alterColumnTypeEvent.tableId()));
        schemaChange.setAlterMapping(alterColumnTypeEvent.getTypeMapping());
        request("ALTER", schemaChange);

    }

    @Override
    protected void open() {
        log.info("open kafka producer");
        if (open) return;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.put("acks", "all");
        producer = new KafkaProducer<>(properties);
        open = true;
    }


    private void request(String op, SchemaChange schemaChangeEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        String tableName =schemaChangeEvent.tableName;

        ChangeSchemaRequest changeSchemaRequest = ChangeSchemaRequest.of(op, identifier(tableName),
                schemaChangeEvent, tableProperties(tableName));

        ProducerRecord record = new ProducerRecord(catalogMetaTopic, tableName, mapper.writeValueAsString(changeSchemaRequest));
        producer.send(record).get();
    }

    @Data(staticConstructor = "of")
    private static class ChangeSchemaRequest {
        private final String op;
        private final ObjectIdentifier objectIdentifier;
        private final SchemaChange schemaChange;
        private final Map<String, String> properties;
    }

    @Data(staticConstructor = "of")
    private static class SchemaChange {
        private final String tableName;
        private List<String> primaryKeys;
        private List<Column> createColumns;
        private List<Column> addColumn;
        private List<Column> droppedColumns;
        private Map<String, DataType> alterMapping;
        private Map<String, String> renameMapping;

    }

}
