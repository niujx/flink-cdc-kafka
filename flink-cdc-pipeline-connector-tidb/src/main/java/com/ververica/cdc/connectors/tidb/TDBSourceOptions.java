//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.ververica.cdc.connectors.tidb;

import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.tikv.common.TiConfiguration;

public class TDBSourceOptions {
    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name").stringType().noDefaultValue().withDescription("Database name of the TiDB server to monitor.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue().withDescription("Table name of the TiDB database to monitor.");
    public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions.key("scan.startup.mode").stringType().defaultValue("initial").withDescription("Optional startup mode for TiDB CDC consumer, valid enumerations are \"initial\", \"latest-offset\"");
    public static final ConfigOption<String> PD_ADDRESSES = ConfigOptions.key("pd-addresses").stringType().noDefaultValue().withDescription("TiKV cluster's PD address");
    public static final ConfigOption<Long> TIKV_GRPC_TIMEOUT = ConfigOptions.key("tikv.grpc.timeout_in_ms").longType().noDefaultValue().withDescription("TiKV GRPC timeout in ms");
    public static final ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT = ConfigOptions.key("tikv.grpc.scan_timeout_in_ms").longType().noDefaultValue().withDescription("TiKV GRPC scan timeout in ms");
    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY = ConfigOptions.key("tikv.batch_get_concurrency").intType().noDefaultValue().withDescription("TiKV GRPC batch get concurrency");
    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY = ConfigOptions.key("tikv.batch_scan_concurrency").intType().noDefaultValue().withDescription("TiKV GRPC batch scan concurrency");
    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
         ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    private TDBSourceOptions() {
    }

    public static TiConfiguration getTiConfiguration(String pdAddrsStr, Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
        configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
        configuration.getOptional(TIKV_BATCH_GET_CONCURRENCY).ifPresent(tiConf::setBatchGetConcurrency);
        configuration.getOptional(TIKV_BATCH_SCAN_CONCURRENCY).ifPresent(tiConf::setBatchScanConcurrency);
        return tiConf;
    }
}
