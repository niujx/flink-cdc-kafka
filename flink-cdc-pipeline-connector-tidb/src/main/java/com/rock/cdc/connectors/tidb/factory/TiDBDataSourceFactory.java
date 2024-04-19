package com.rock.cdc.connectors.tidb.factory;

import com.rock.cdc.connectors.tidb.source.TiDBDataSource;
import com.rock.cdc.connectors.tidb.source.TiDBDataSourceConfig;
import com.rock.cdc.connectors.tidb.source.TiDBDataSourceConfigFactory;
import com.rock.cdc.connectors.tidb.source.TiDBSourceOptions;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.apache.flink.table.api.ValidationException;

import java.util.HashSet;
import java.util.Set;

import static com.rock.cdc.connectors.tidb.source.TiDBSourceOptions.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class TiDBDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "tidb";

    @Override
    public DataSource createDataSource(Context context) {

        Configuration config = context.getFactoryConfiguration();
        String adAddresses = config.get(PD_ADDRESSES);
        String database = config.get(DATABASE);
        String tables = config.get(TABLES);

        Long tikvGrpcTimeout = config.get(TIKV_GRPC_TIMEOUT);
        Long tikvGrpcScanTimeout = config.get(TIKV_GRPC_SCAN_TIMEOUT);

        Integer tikvBatchGetConcurrency = config.get(TIKV_BATCH_GET_CONCURRENCY);
        Integer tikvBatchScanConcurrency = config.get(TIKV_BATCH_SCAN_CONCURRENCY);

        StartupOptions startupOptions = getStartupOptions(config);


        TiDBDataSourceConfig tiDBDataSourceConfig = TiDBDataSourceConfigFactory.create()
                .setAdAddresses(adAddresses)
                .setDatabase(database)
                .setTables(tables)
                .setTiKvGrpcTimeout(tikvGrpcTimeout)
                .setTiKvGrpcScanTimeout(tikvGrpcScanTimeout)
                .setTiKvBatchGetConcurrency(tikvBatchGetConcurrency)
                .setTiKvBatchScanConcurrency(tikvBatchScanConcurrency)
                .setStartupOptions(startupOptions)
                .getTiDBDataSourceConfig();

        return new TiDBDataSource(tiDBDataSourceConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PD_ADDRESSES);
        options.add(DATABASE);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(TIKV_GRPC_TIMEOUT);
        options.add(TIKV_GRPC_SCAN_TIMEOUT);
        options.add(TIKV_BATCH_GET_CONCURRENCY);
        options.add(TIKV_BATCH_SCAN_CONCURRENCY);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(Configuration config) {
        String scanStartupMode = config.get(SCAN_STARTUP_MODE);

        switch (scanStartupMode) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        checkNotNull(
                                config.get(SCAN_STARTUP_TIMESTAMP_MILLIS),
                                String.format(
                                        "To use timestamp startup mode, the startup timestamp millis '%s' must be set.",
                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key())));
            default:
                throw new ValidationException(String.format("Invalid value for option '%s'. Supported values are [%s,%s],but was: %s",
                        SCAN_STARTUP_MODE.key(),
                        SCAN_STARTUP_MODE_VALUE_INITIAL,
                        SCAN_STARTUP_MODE_VALUE_LATEST,
                        scanStartupMode));
        }


    }
}
