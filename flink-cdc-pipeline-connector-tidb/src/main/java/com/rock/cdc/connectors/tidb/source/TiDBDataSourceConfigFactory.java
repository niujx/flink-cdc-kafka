package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.tikv.common.TiConfiguration;

import java.util.Optional;

public class TiDBDataSourceConfigFactory {

    private String adAddresses;

    private String database;

    private String tables;

    private Long tiKvGrpcTimeout;

    private Long tiKvGrpcScanTimeout;

    private Integer tiKvBatchGetConcurrency;

    private Integer tiKvBatchScanConcurrency;

    private StartupOptions startupOptions;

    private TiDBDataSourceConfigFactory() {
    }

    public static TiDBDataSourceConfigFactory create() {
        return new TiDBDataSourceConfigFactory();
    }


    public TiDBDataSourceConfigFactory setAdAddresses(String adAddresses) {
        this.adAddresses = adAddresses;
        return this;
    }

    public TiDBDataSourceConfigFactory setDatabase(String database) {
        this.database = database;
        return this;
    }

    public TiDBDataSourceConfigFactory setTables(String tables) {
        this.tables = tables;
        return this;
    }

    public TiDBDataSourceConfigFactory setTiKvGrpcTimeout(Long tiKvGrpcTimeout) {
        this.tiKvGrpcTimeout = tiKvGrpcTimeout;
        return this;
    }

    public TiDBDataSourceConfigFactory setTiKvGrpcScanTimeout(Long tiKvGrpcScanTimeout) {
        this.tiKvGrpcScanTimeout = tiKvGrpcScanTimeout;
        return this;
    }

    public TiDBDataSourceConfigFactory setTiKvBatchGetConcurrency(Integer tiKvBatchGetConcurrency) {
        this.tiKvBatchGetConcurrency = tiKvBatchGetConcurrency;
        return this;
    }

    public TiDBDataSourceConfigFactory setTiKvBatchScanConcurrency(Integer tiKvBatchScanConcurrency) {
        this.tiKvBatchScanConcurrency = tiKvBatchScanConcurrency;
        return this;
    }

    public TiDBDataSourceConfigFactory setStartupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    public TiDBDataSourceConfig getTiDBDataSourceConfig() {

        TiConfiguration tiConfiguration = TiConfiguration.createDefault(adAddresses);

        Optional.ofNullable(tiKvGrpcTimeout).ifPresent(tiConfiguration::setTimeout);
        Optional.ofNullable(tiKvGrpcScanTimeout).ifPresent(tiConfiguration::setScanTimeout);
        Optional.ofNullable(tiKvBatchGetConcurrency).ifPresent(tiConfiguration::setBatchGetConcurrency);
        Optional.ofNullable(tiKvBatchScanConcurrency).ifPresent(tiConfiguration::setBatchScanConcurrency);

        return new TiDBDataSourceConfig(
                tiConfiguration,
                database,
                tables,
                startupOptions);

    }
}
