package com.rock.cdc.connectors.kafka.sink.metadata;

import com.google.common.collect.Maps;
import com.rock.cdc.connectors.kafka.sink.CatalogConfig;
import com.ververica.cdc.common.event.*;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.utils.DataTypeUtils;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//目前只能在本地运行使用
@Slf4j
public class LocalCatalogMetadataApplier extends BaseCatalogMetadataApplier {

    private boolean isOpen;
    private CatalogManager catalogManager;
    private Map<String, TableSchemaSnapshot> tableSchemaSnapshotMap;

    public LocalCatalogMetadataApplier(
            CatalogConfig catalogConfig) {
        super(catalogConfig);
        tableSchemaSnapshotMap = new HashMap<>();
    }

    private CatalogTable snapshotToCatalogTable(TableSchemaSnapshot tableSchemaSnapshot) {
        TableSchema.Builder tableSchema = TableSchema.builder()
                .primaryKey(tableSchemaSnapshot.getPrimaryKeys().toArray(new String[0]));

        for (org.apache.flink.table.catalog.Column column : tableSchemaSnapshot.getColumns().values()) {
            tableSchema.add(TableColumn.physical(column.getName(), column.getDataType()));
        }

        CatalogTableImpl catalogTable = new CatalogTableImpl(
                tableSchema.build(),
                tableSchemaSnapshot.getProperties(),
                "");
        return catalogTable;
    }


    @SneakyThrows
    protected void open() {
        if (isOpen) {
            return;
        }
        CatalogStore catalogStore = new FileCatalogStore(catalogConfig.getCatalogStorePath());
        EnvironmentSettings environmentSettings = EnvironmentSettings
                .newInstance()
                .withCatalogStore(catalogStore)
                .build();

        TableEnvironmentImpl tableEnvironment = TableEnvironmentImpl.create(environmentSettings);
        catalogManager = tableEnvironment.getCatalogManager();
        isOpen = true;
    }


    private void handCreateTable(TableSchemaSnapshot schemaSnapshot) throws DatabaseAlreadyExistException {
        if (!catalogManager.schemaExists(catalogConfig.getCatalog(), catalogConfig.getDatabase())) {
            catalogManager.createDatabase(catalogConfig.getCatalog(), catalogConfig.getDatabase(), new CatalogDatabaseImpl(Maps.newHashMap(), ""), true);
        }

        String tableName = schemaSnapshot.getTableName();

        CatalogTable catalogTable = snapshotToCatalogTable(schemaSnapshot);

        ObjectIdentifier identifier = identifier(tableName);
        //如果发现表已经存在,删除历史表
        catalogManager.getTable(identifier)
                .ifPresent(table -> catalogManager.dropTable(identifier, true));
        catalogManager.createTable(catalogTable, identifier, true);
    }

    private void handAlterTable(TableSchemaSnapshot schemaSnapshot) throws TableNotExistException {
        String tableName = schemaSnapshot.getTableName();
        CatalogTable catalogTable = snapshotToCatalogTable(schemaSnapshot);
        catalogManager.alterTable(catalogTable, identifier(tableName), true);
    }

    @SneakyThrows
    protected void applyCreateTable(CreateTableEvent createTableEvent) {
        Schema schema = createTableEvent.getSchema();
        String tableName = getTableName(createTableEvent.tableId());

        Map<String, Column> columnMap = Maps.newHashMap();
        for (String columnName : schema.getColumnNames()) {
            Column column = Column.physical(columnName,toFlinkDataType(schema.getColumn(columnName).get().getType()));
            columnMap.put(columnName, column);
        }
        TableSchemaSnapshot tableSchemaSnapshot = TableSchemaSnapshot.of(tableName,
                schema.primaryKeys(), columnMap, tableProperties(tableName));
        handCreateTable(tableSchemaSnapshot);
        tableSchemaSnapshotMap.put(tableName, tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyAddColumn(AddColumnEvent addColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(addColumnEvent.tableId());
        for (AddColumnEvent.ColumnWithPosition addColum : addColumnEvent.getAddedColumns()) {
            Column column =Column.physical(addColum.getAddColumn().getName(),
                    toFlinkDataType(addColum.getAddColumn().getType()));
            tableSchemaSnapshot.getColumns().put(column.getName(), column);
        }
        handAlterTable(tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyDropColumn(DropColumnEvent dropColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(dropColumnEvent.tableId());
        for (com.ververica.cdc.common.schema.Column column : dropColumnEvent.getDroppedColumns()) {
            tableSchemaSnapshot.getColumns().remove(column.getName());
        }
        handAlterTable(tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyRenameColumn(RenameColumnEvent renameColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(renameColumnEvent.tableId());
        for (Map.Entry<String, String> rename : renameColumnEvent.getNameMapping().entrySet()) {
            Column oldColumn = tableSchemaSnapshot.getColumns().remove(rename.getKey());
            Column newColumn = Column.physical(rename.getValue(), oldColumn.getDataType());
            tableSchemaSnapshot.getColumns().put(newColumn.getName(), newColumn);
        }
        handAlterTable(tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyAlterColumn(AlterColumnTypeEvent alterColumnTypeEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(alterColumnTypeEvent.tableId());
        for (Map.Entry<String, com.ververica.cdc.common.types.DataType> alter : alterColumnTypeEvent.getTypeMapping().entrySet()) {
            tableSchemaSnapshot.getColumns().remove(alter.getKey());
            Column newColumn = Column.physical(alter.getKey(), toFlinkDataType(alter.getValue()));
            tableSchemaSnapshot.getColumns().put(newColumn.getName(), newColumn);
        }
        handAlterTable(tableSchemaSnapshot);
    }

    private TableSchemaSnapshot getTableSchema(TableId tableId) {
        String tableName = getTableName(tableId);
        TableSchemaSnapshot tableSchemaSnapshot = tableSchemaSnapshotMap.get(tableName);
        if (tableSchemaSnapshot == null) {
            tableSchemaSnapshot =  catalogManager.getTable(identifier(tableName))
                    .map(contextResolvedTable -> {
                        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();

                        List<String> primaryKeys = resolvedSchema
                                .getPrimaryKey()
                                .map(uniqueConstraint -> uniqueConstraint.getColumns())
                                .orElse(Collections.emptyList());

                        Map<String, Column> columns = resolvedSchema.getColumns().stream()
                                .collect(Collectors.toMap(Column::getName, c -> c));
                        return TableSchemaSnapshot.of(tableName, primaryKeys, columns, tableProperties(tableName));
                    }).get();

            tableSchemaSnapshotMap.put(tableName,tableSchemaSnapshot);
        }
        return tableSchemaSnapshot;
    }


    private DataType toFlinkDataType(  com.ververica.cdc.common.types.DataType dateType){
       DataType flinkDataType = DataTypeUtils.toFlinkDataType(dateType);
        if(dateType.isNullable()){
            flinkDataType= flinkDataType.nullable();
        }else {
            flinkDataType = flinkDataType.notNull();
        }
        return flinkDataType;
    }

    @Data(staticConstructor = "of")
    public static class TableSchemaSnapshot implements Serializable {
        private final String tableName;
        private final List<String> primaryKeys;
        private final Map<String, org.apache.flink.table.catalog.Column> columns;
        private final Map<String, String> properties;
    }
}
