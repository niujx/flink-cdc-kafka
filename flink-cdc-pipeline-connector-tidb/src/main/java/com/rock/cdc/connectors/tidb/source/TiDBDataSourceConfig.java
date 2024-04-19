package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.tikv.common.TiConfiguration;

public class TiDBDataSourceConfig {

    private final TiConfiguration tiConfiguration;

    private final String database;

    private final String table;

    private final StartupOptions startupOptions;

    public TiDBDataSourceConfig(TiConfiguration tiConfiguration, String database, String table, StartupOptions startupOptions) {
        this.tiConfiguration = tiConfiguration;
        this.database = database;
        this.table = table;
        this.startupOptions = startupOptions;
    }

    public TiConfiguration getTiConfiguration() {
        return tiConfiguration;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public StartupOptions getStartupOptions() {
        return startupOptions;
    }
}
