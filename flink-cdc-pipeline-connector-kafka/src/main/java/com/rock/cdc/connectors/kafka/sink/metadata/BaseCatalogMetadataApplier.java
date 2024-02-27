package com.rock.cdc.connectors.kafka.sink.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.rock.cdc.connectors.kafka.sink.CatalogConfig;
import com.ververica.cdc.common.event.*;
import com.ververica.cdc.common.sink.MetadataApplier;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Map;

import static com.ververica.cdc.common.utils.Preconditions.checkNotNull;

public abstract class BaseCatalogMetadataApplier implements MetadataApplier {

    protected final static String TOPIC_KEY = "topic";
    protected final CatalogConfig catalogConfig;
    protected final String prefix;
    protected final String bootstrapServers;
    protected ImmutableMap<String, String> connectorProperties;

    public BaseCatalogMetadataApplier(
            CatalogConfig catalogConfig) {
        this.catalogConfig = catalogConfig;
        this.prefix = catalogConfig.getPrefix();
        this.bootstrapServers = catalogConfig.getBootstrapServers();
        this.connectorProperties = ImmutableMap.<String, String>builder()
                .put("connector", "kafka")
                .put("properties.bootstrap.servers", checkNotNull(bootstrapServers, "kafka bootstrap.servers address is null"))
                .put("format", "debezium-json")
                .build();


    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        open();
        if (schemaChangeEvent instanceof CreateTableEvent) {
            applyCreateTable((CreateTableEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof AddColumnEvent) {
            applyAddColumn((AddColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof DropColumnEvent) {
            applyDropColumn((DropColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof RenameColumnEvent) {
            applyRenameColumn((RenameColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
            applyAlterColumn((AlterColumnTypeEvent) schemaChangeEvent);
        } else {
            throw new UnsupportedOperationException(catalogConfig.getCatalog() + " catalog doesn't support schema change event " + schemaChangeEvent);
        }

    }

    protected String getTableName(TableId tableId) {
        if (prefix != null) {
            return String.format("%s_%s_%s", prefix, tableId.getSchemaName(), tableId.getTableName()).replaceAll("-", "_").toLowerCase();
        }
        return String.format("%s_%s", tableId.getSchemaName(), tableId.getTableName()).replaceAll("-", "_").toLowerCase();
    }


    protected Map<String, String> tableProperties(String tableName) {
        Map<String, String> properties = Maps.newHashMap(connectorProperties);
        properties.put(TOPIC_KEY, tableName.replaceAll("_", "-"));
        return properties;
    }

    protected ObjectIdentifier identifier(String tableName) {
        return ObjectIdentifier.of(catalogConfig.getCatalog(), catalogConfig.getDatabase(), tableName);
    }

    protected abstract void applyCreateTable(CreateTableEvent createTableEvent);

    protected abstract void applyAddColumn(AddColumnEvent addColumnEvent);

    protected abstract void applyDropColumn(DropColumnEvent dropColumnEvent);

    protected abstract void applyRenameColumn(RenameColumnEvent renameColumnEvent);

    protected abstract void applyAlterColumn(AlterColumnTypeEvent alterColumnTypeEvent);

    protected abstract void open();


}
