package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import org.tikv.common.ConfigUtils;

public class TiDBSourceOptions {

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the TiDB server to monitor.");

    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the TiDB database to monitor.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for TiDB CDC consumer, valid enumerations are "
                                    + "\"initial\", \"latest-offset\"");

    public static final ConfigOption<String> PD_ADDRESSES =
            ConfigOptions.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiKV cluster's PD address");

    public static final ConfigOption<Long> TIKV_GRPC_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC timeout in ms");

    public static final ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC scan timeout in ms");

    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch get concurrency");

    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch scan concurrency");

}
