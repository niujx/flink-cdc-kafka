//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.ververica.cdc.connectors.tidb.table;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.table.utils.OptionUtils;
import com.ververica.cdc.debezium.utils.ResolvedSchemaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class TiDBTableSourceFactory implements DynamicTableSourceFactory {
    private static final String IDENTIFIER = "tidb-cdc";
    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    public TiDBTableSourceFactory() {
    }

    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        String databaseName = (String) config.get(TDBSourceOptions.DATABASE_NAME);
        String tableName = (String) config.get(TDBSourceOptions.TABLE_NAME);
        String pdAddresses = (String) config.get(TDBSourceOptions.PD_ADDRESSES);
        StartupOptions startupOptions = getStartupOptions(config);
        ResolvedSchema physicalSchema = ResolvedSchemaUtils.getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        OptionUtils.printOptions("tidb-cdc", ((Configuration) config).toMap());
        return new TiDBTableSource(physicalSchema, databaseName, tableName, pdAddresses, startupOptions, TiDBTableSourceFactory.TiKVOptions.getTiKVOptions(context.getCatalogTable().getOptions()));
    }

    public String factoryIdentifier() {
        return "tidb-cdc";
    }

    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(TDBSourceOptions.DATABASE_NAME);
        options.add(TDBSourceOptions.TABLE_NAME);
        options.add(TDBSourceOptions.PD_ADDRESSES);
        return options;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(TDBSourceOptions.SCAN_STARTUP_MODE);
        options.add(TDBSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(TDBSourceOptions.TIKV_GRPC_TIMEOUT);
        options.add(TDBSourceOptions.TIKV_GRPC_SCAN_TIMEOUT);
        options.add(TDBSourceOptions.TIKV_BATCH_GET_CONCURRENCY);
        options.add(TDBSourceOptions.TIKV_BATCH_SCAN_CONCURRENCY);
        return options;
    }

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = (String) config.get(TDBSourceOptions.SCAN_STARTUP_MODE);
        switch (modeString.toLowerCase()) {
            case "initial":
                return StartupOptions.initial();
            case "latest-offset":
                return StartupOptions.latest();
            case "timestamp":
                return StartupOptions.timestamp(
                        checkNotNull(
                                config.get(SCAN_STARTUP_TIMESTAMP_MILLIS),
                                String.format(
                                        "To use timestamp startup mode, the startup timestamp millis '%s' must be set.",
                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key())));
            default:
                throw new ValidationException(String.format("Invalid value for option '%s'. Supported values are [%s, %s], but was: %s", TDBSourceOptions.SCAN_STARTUP_MODE.key(), "initial", "latest-offset", modeString));
        }
    }

    static class TiKVOptions {
        private static final String TIKV_OPTIONS_PREFIX = "tikv.";

        TiKVOptions() {
        }

        public static Map<String, String> getTiKVOptions(Map<String, String> properties) {
            Map<String, String> tikvOptions = new HashMap();
            if (hasTiKVOptions(properties)) {
                properties.keySet().stream().filter((key) -> {
                    return key.startsWith("tikv.");
                }).forEach((key) -> {
                    String value = (String) properties.get(key);
                    tikvOptions.put(key, value);
                });
            }

            return tikvOptions;
        }

        private static boolean hasTiKVOptions(Map<String, String> options) {
            return options.keySet().stream().anyMatch((k) -> {
                return k.startsWith("tikv.");
            });
        }
    }
}
