package com.rock.cdc.connectors.tidb.source;

import com.rock.cdc.connectors.tidb.utils.TiTableInfoUtils;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.table.StartupMode;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.tikv.common.meta.TiTableInfo;

public class TiDBDataSource implements DataSource {
    private final TiDBDataSourceConfig config;

    public TiDBDataSource(TiDBDataSourceConfig config) {
        this.config = config;
    }

    @Override
    public FlinkSourceFunctionProvider getEventSourceProvider() {

        TiTableInfo  tiTableInfo = new TiTableInfoUtils(config.getTiConfiguration()).fetchTableInfo(config.getDatabase(), config.getTable());

        TableId tableId = TableId.tableId(config.getDatabase(),config.getTable());
        boolean changeEventIsSendCreateSchema = config.getStartupOptions().startupMode != StartupMode.INITIAL;
        RichParallelSourceFunction<Event> tiDBSource = TiDBSource.<Event>builder()
                .database(config.getDatabase())
                .tableName(config.getTable())
                .tiConf(config.getTiConfiguration())
                .snapshotEventDeserializer(new SimpleTiKVSnapshotEventDeserializationSchema(tiTableInfo,tableId))
                .changeEventDeserializer(new SimpleTiKVChangeEventDeserializationSchema(tiTableInfo,tableId,changeEventIsSendCreateSchema))
                .startupOptions(config.getStartupOptions())
                .build();

        return FlinkSourceFunctionProvider.of(tiDBSource);

    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return null;
    }
}
