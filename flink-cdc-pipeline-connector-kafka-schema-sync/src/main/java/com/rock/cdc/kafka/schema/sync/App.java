package com.rock.cdc.kafka.schema.sync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.ververica.cdc.common.types.DataTypeRoot;
import com.ververica.cdc.common.utils.Preconditions;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.CollectionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.*;

@Slf4j
@SpringBootApplication
public class App {

    @Value("${catalog.store.path}")
    private String catalogPath;
    @Value("${catalog.name}")
    private String catalogName;
    @Value("${catalog.hadoop.username}")
    private String hadoopUsername;

    @Autowired
    private CatalogManager catalogManager;

    private ObjectMapper mapper = new ObjectMapper();

    private Map<String, TableSchemaSnapshot> tableSchemaSnapshotMap = Maps.newHashMap();
    private List<String> invalidSchema = Lists.newArrayList();


    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public CatalogManager catalogManager() {
        System.setProperty("HADOOP_USER_NAME", hadoopUsername);
        CatalogStore catalogStore = new FileCatalogStore(catalogPath);
        EnvironmentSettings environmentSettings = EnvironmentSettings
                .newInstance()
                .withCatalogStore(catalogStore)
                .build();

        TableEnvironmentImpl tableEnvironment = TableEnvironmentImpl.create(environmentSettings);
        return tableEnvironment.getCatalogManager();
    }


    @Bean
    public Consumer<String> sinkProcess() {
        return value -> {
            ChangeSchemaRequest schemaRequest;
            try {
                schemaRequest = mapper.readValue(value, ChangeSchemaRequest.class);
            } catch (Exception e) {
                log.error("cannot  deserialization message {} skip ", value);
                return;
            }

            try {
                SchemaChange schemaChange = schemaRequest.getSchemaChange();
                log.info("receive schema change {}", schemaRequest.objectIdentifier);

                if (!checkSchema(schemaRequest)) {
                    return;
                }

                switch (schemaRequest.getOp()) {
                    case "CREATE":
                        applyCreateTable(schemaRequest, schemaChange);
                        break;
                    case "ADD":
                        applyAddColumn(schemaRequest, schemaChange);
                        break;
                    case "DROP":
                        applyDropColumn(schemaRequest, schemaChange);
                        break;
                    case "RENAME":
                        applyRenameColumn(schemaRequest, schemaChange);
                        break;
                    case "ALTER":
                        applyAlterColumn(schemaRequest, schemaChange);
                        break;
                    default:
                        throw new UnsupportedOperationException(schemaRequest.objectIdentifier + " catalog doesn't support schema change event " + schemaChange);
                }

            } catch (Exception e) {
                log.error("sync table schema error {}", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        };
    }

    @ServiceActivator(inputChannel = "schema-sync.errors")
    public void error(Message<String> message) {
        log.error("{}", message.getPayload());
    }

    @PreDestroy
    public void close() {
        catalogManager.close();
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


    private void handCreateTable(ChangeSchemaRequest request, TableSchemaSnapshot schemaSnapshot) throws DatabaseAlreadyExistException {
        ObjectIdentifier identifier = request.objectIdentifier.toObjectIdentifier();
        if (!catalogManager.schemaExists(identifier.getCatalogName(), identifier.getDatabaseName())) {
            catalogManager.createDatabase(identifier.getCatalogName(), identifier.getDatabaseName(), new CatalogDatabaseImpl(Maps.newHashMap(), ""), true);
        }

        CatalogTable catalogTable = snapshotToCatalogTable(schemaSnapshot);

        //如果发现表已经存在,删除历史表
        catalogManager.getTable(identifier)
                .ifPresent(table -> catalogManager.dropTable(identifier, true));
        catalogManager.createTable(catalogTable, identifier, true);
    }

    private void handAlterTable(ChangeSchemaRequest request, TableSchemaSnapshot schemaSnapshot) throws TableNotExistException {
        CatalogTable catalogTable = snapshotToCatalogTable(schemaSnapshot);
        catalogManager.alterTable(catalogTable, request.objectIdentifier.toObjectIdentifier(), true);
    }

    @SneakyThrows
    protected void applyCreateTable(ChangeSchemaRequest request, SchemaChange createTableEvent) {
        Map<String, Column> columnMap = Maps.newHashMap();
        for (SyncColumn syncColumn : createTableEvent.getCreateColumns()) {
            Column column = Column.physical(syncColumn.getName(), dataType(syncColumn.getType()));
            columnMap.put(column.getName(), column);
        }
        TableSchemaSnapshot tableSchemaSnapshot = TableSchemaSnapshot.of(request.objectIdentifier.getObjectName(),
                createTableEvent.getPrimaryKeys(), columnMap, request.properties);
        handCreateTable(request, tableSchemaSnapshot);
        tableSchemaSnapshotMap.put(request.objectIdentifier.getObjectName(), tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyAddColumn(ChangeSchemaRequest request, SchemaChange addColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(request);
        for (SyncColumn addColum : addColumnEvent.getAddColumn()) {
            Column column = Column.physical(addColum.getName(),
                    dataType(addColum.getType()));
            tableSchemaSnapshot.getColumns().put(column.getName(), column);
        }
        handAlterTable(request, tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyDropColumn(ChangeSchemaRequest request, SchemaChange dropColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(request);
        for (SyncColumn column : dropColumnEvent.getDroppedColumns()) {
            tableSchemaSnapshot.getColumns().remove(column.getName());
        }
        handAlterTable(request, tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyRenameColumn(ChangeSchemaRequest request, SchemaChange renameColumnEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(request);
        for (Map.Entry<String, String> rename : renameColumnEvent.getRenameMapping().entrySet()) {
            Column oldColumn = tableSchemaSnapshot.getColumns().remove(rename.getKey());
            Column newColumn = Column.physical(rename.getValue(), oldColumn.getDataType());
            tableSchemaSnapshot.getColumns().put(newColumn.getName(), newColumn);
        }
        handAlterTable(request, tableSchemaSnapshot);
    }

    @SneakyThrows
    protected void applyAlterColumn(ChangeSchemaRequest request, SchemaChange alterColumnTypeEvent) {
        TableSchemaSnapshot tableSchemaSnapshot = getTableSchema(request);
        for (Map.Entry<String, SyncDataType> alter : alterColumnTypeEvent.getAlterMapping().entrySet()) {
            tableSchemaSnapshot.getColumns().remove(alter.getKey());
            Column newColumn = Column.physical(alter.getKey(), dataType(alter.getValue()));
            tableSchemaSnapshot.getColumns().put(newColumn.getName(), newColumn);
        }
        handAlterTable(request, tableSchemaSnapshot);
    }

    private TableSchemaSnapshot getTableSchema(ChangeSchemaRequest changeSchemaRequest) {
        TableSchemaSnapshot tableSchemaSnapshot = tableSchemaSnapshotMap.get(changeSchemaRequest.objectIdentifier.getObjectName());
        if (tableSchemaSnapshot == null) {
            tableSchemaSnapshot = catalogManager.getTable(changeSchemaRequest.objectIdentifier.toObjectIdentifier())
                    .map(contextResolvedTable -> {
                        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();

                        List<String> primaryKeys = resolvedSchema
                                .getPrimaryKey()
                                .map(uniqueConstraint -> uniqueConstraint.getColumns())
                                .orElse(Collections.emptyList());

                        Map<String, Column> columns = resolvedSchema.getColumns().stream()
                                .collect(Collectors.toMap(Column::getName, c -> c));
                        return TableSchemaSnapshot.of(changeSchemaRequest.objectIdentifier.getObjectName(), primaryKeys, columns, contextResolvedTable.getTable().getOptions());
                    }).get();
            tableSchemaSnapshotMap.put(changeSchemaRequest.objectIdentifier.getObjectName(), tableSchemaSnapshot);
        }
        return tableSchemaSnapshot;
    }

    private boolean checkSchema(ChangeSchemaRequest changeSchemaRequest) {
        if (changeSchemaRequest.getOp().equals("CREATE")) {
            if (changeSchemaRequest.schemaChange.getPrimaryKeys().isEmpty()) {
                invalidSchema.add(changeSchemaRequest.objectIdentifier.getObjectName());
                return false;
            }
        }

        if (invalidSchema.contains(changeSchemaRequest.objectIdentifier.getObjectName())) {
            return false;
        }

        return true;
    }


    public static org.apache.flink.table.types.DataType toDataType(SyncDataType dataType) {
        List<SyncDataType> children = dataType.getChildren();
        int length = dataType.getLength();
        int precision = dataType.getPrecision();
        int scale = dataType.getScale();

        switch (DataTypeRoot.valueOf(dataType.getTypeRoot())) {
            case CHAR:
                return CHAR(length);
            case VARCHAR:
                return VARCHAR(length);
            case BOOLEAN:
                return BOOLEAN();
            case BINARY:
                return BINARY(length);
            case VARBINARY:
                return VARBINARY(length);
            case DECIMAL:
                return DECIMAL(precision, scale);
            case TINYINT:
                return TINYINT();
            case SMALLINT:
                return SMALLINT();
            case INTEGER:
                return INT();
            case DATE:
                return DATE();
            case TIME_WITHOUT_TIME_ZONE:
                return TIME(length);
            case BIGINT:
                return BIGINT();
            case FLOAT:
                return FLOAT();
            case DOUBLE:
                return DOUBLE();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP(length);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TIMESTAMP_WITH_LOCAL_TIME_ZONE(length);
            case ARRAY:
                Preconditions.checkState(children != null && children.size() > 0);
                return ARRAY(dataType(children.get(0)));
            case MAP:
                Preconditions.checkState(children != null && children.size() > 1);
                return MAP(
                        dataType(children.get(0)), dataType(children.get(1)));
            case ROW:
                Preconditions.checkState(!CollectionUtil.isNullOrEmpty(children));
                return ROW(
                        children.toArray(new org.apache.flink.table.types.DataType[]{}));

            default:
                throw new IllegalArgumentException("Illegal type: " + dataType);
        }

    }

    public static org.apache.flink.table.types.DataType dataType(SyncDataType dataType) {
        org.apache.flink.table.types.DataType flinkDataType = toDataType(dataType);
        if (dataType.isNullable()) {
            return flinkDataType.nullable();
        } else {
            return flinkDataType.notNull();
        }
    }


    @Component
    class NoOpBindingMeters {
        NoOpBindingMeters(MeterRegistry registry) {
            registry.config().meterFilter(
                    MeterFilter.denyNameStartsWith(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME));
        }
    }


    @Data
    public static class ChangeSchemaRequest {
        private String op;
        private SyncObjectIdentifier objectIdentifier;
        private Map<String, String> properties;
        private SchemaChange schemaChange;

    }

    @Data
    public static class SyncObjectIdentifier implements Serializable {
        private String catalogName;
        private String databaseName;
        private String objectName;

        public ObjectIdentifier toObjectIdentifier() {
            return ObjectIdentifier.of(catalogName, databaseName, objectName);
        }
    }

    @Data
    private static class SchemaChange {
        private String tableName;
        private List<String> primaryKeys;
        private List<SyncColumn> createColumns;
        private List<SyncColumn> addColumn;
        private List<SyncColumn> droppedColumns;
        private Map<String, SyncDataType> alterMapping;
        private Map<String, String> renameMapping;
    }


    @Data(staticConstructor = "of")
    public static class TableSchemaSnapshot implements Serializable {
        private final String tableName;
        private final List<String> primaryKeys;
        private final Map<String, org.apache.flink.table.catalog.Column> columns;
        private final Map<String, String> properties;
    }


    @Data
    public static class SyncColumn {
        private String name;
        private SyncDataType type;
        private String comment;
        private boolean physical;
    }

    @Data
    public static class SyncDataType {
        private String typeRoot;
        private boolean nullable;
        private List<SyncDataType> children;
        private int precision;
        private int scale;
        private int length;
    }
}
