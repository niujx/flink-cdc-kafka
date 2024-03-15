package com.rock.cdc.connectors.mongodb.factory;

import com.rock.cdc.connectors.mongodb.source.MongodbDataSource;
import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.utils.OptionUtils;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils;
import com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.ValidationException;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static com.rock.cdc.connectors.mongodb.source.MongodbSourceOptions.*;
import static org.apache.flink.util.Preconditions.checkNotNull;


@Slf4j
@Internal
public class MongodbDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "mongodb";

    @Override
    public DataSource createDataSource(Context context) {
        Configuration config = context.getFactoryConfiguration();
        String scheme = config.get(SCHEME);
        String hosts = config.get(HOSTS);
        String connectionOptions = config.getOptional(CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);

        String databases = config.getOptional(DATABASE).orElse(null);
        String collection = config.getOptional(COLLECTION).orElse(null);

        Integer batchSize = config.get(BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis = config.get(HEARTBEAT_INTERVAL_MILLIS);

        StartupOptions startupOptions = getStartupOptions(config);
        Integer initialSnapshottingQueueSize =
                config.getOptional(INITIAL_SNAPSHOTTING_QUEUE_SIZE).orElse(null);

        ZoneId serverTimeZone = getServerTimeZone(config);

        boolean enableCloseIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        int splitSizeMB = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int samplesPerChunk = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);

        boolean enableFullDocumentPrePostImage =
                config.getOptional(FULL_DOCUMENT_PRE_POST_IMAGE).orElse(false);

        boolean noCursorTimeout = config.getOptional(SCAN_NO_CURSOR_TIMEOUT).orElse(true);

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        MongoDBSourceConfigFactory configFactory = new MongoDBSourceConfigFactory()
                .scheme(scheme)
                .hosts(hosts)
                .connectionOptions(connectionOptions)
                .username(username)
                .password(password)
                .databaseList(".*")
                .collectionList(".*")
                .startupOptions(startupOptions)
                .batchSize(batchSize)
                .pollMaxBatchSize(pollMaxBatchSize)
                .pollAwaitTimeMillis(pollAwaitTimeMillis)
                .heartbeatIntervalMillis(heartbeatIntervalMillis)
                .batchSize(batchSize)
                .closeIdleReaders(enableCloseIdleReaders)
                .splitSizeMB(splitSizeMB)
                .splitMetaGroupSize(splitMetaGroupSize)
                .samplesPerChunk(samplesPerChunk)
                .skipSnapshotBackfill(skipSnapshotBackfill)
                .disableCursorTimeout(noCursorTimeout)
                .scanFullChangelog(enableFullDocumentPrePostImage);

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(collection).build();
        String[] capturedTables = getTableList(configFactory.create(0), selectors, Arrays.asList(StringUtils.split(databases, ",")));
        if (capturedTables.length == 0) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + collection);
        }
        configFactory.collectionList(capturedTables);
        return new MongodbDataSource(configFactory,serverTimeZone);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECTION_OPTIONS);
        options.add(DATABASE);
        options.add(COLLECTION);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(INITIAL_SNAPSHOTTING_QUEUE_SIZE);
        options.add(BATCH_SIZE);
        options.add(POLL_MAX_BATCH_SIZE);
        options.add(POLL_AWAIT_TIME_MILLIS);
        options.add(HEARTBEAT_INTERVAL_MILLIS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(FULL_DOCUMENT_PRE_POST_IMAGE);
        options.add(SCAN_NO_CURSOR_TIMEOUT);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private String[] getTableList(MongoDBSourceConfig sourceConfig, Selectors selectors, List<String> databaseNames) {
        return CollectionDiscoveryUtils.collectionNames(MongoUtils.clientFor(sourceConfig), databaseNames, (Predicate<String>) s -> true)
                .stream().map(this::toTableId)
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .toArray(String[]::new);
    }

    private TableId toTableId(String collectionName) {
        return TableId.tableId(collectionName.substring(0, collectionName.indexOf(".")),
                collectionName.substring(collectionName.indexOf(".")+1, collectionName.length()));
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
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
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private static ZoneId getServerTimeZone(Configuration config) {
        final String serverTimeZone = config.get(SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            log.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    SERVER_TIME_ZONE.key());
            return ZoneId.systemDefault();
        }
    }

}
