package com.rock.cdc.connectors.kafka.factory;

import com.rock.cdc.connectors.kafka.sink.CatalogConfig;
import com.rock.cdc.connectors.kafka.sink.KafkaDataSink;
import com.rock.cdc.connectors.kafka.sink.metadata.MetaDataType;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.utils.Preconditions;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.rock.cdc.connectors.kafka.sink.KafkaDataSinkOptions.*;

public class KafkaDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();

        String boostrapServers = config.get(BOOTSTRAP_SERVERS);
        String topicPrefix = config.get(TOPIC_PREFIX);
        Boolean includeSchema = config.get(INCLUDE_SCHEMA);

        DeliveryGuarantee deliveryGuarantee = config.getOptional(DELIVERY_GUARANTEE).orElse(null);
        String transactionalIdPrefix = config.getOptional(TRANSACTIONAL_ID_PREFIX).orElse(null);

        ZoneId zoneId = ZoneId.of(
                context.getPipelineConfiguration()
                        .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));

        Boolean enableCatalog = config.get(ENABLE_CATALOG);
        MetaDataType metaDataType = config.get(CATALOG_TYPE);
        CatalogConfig catalogConfig = null;
        if (enableCatalog) {
            String catalogStorePath = config.get(CATALOG_LOCAL_STORE_PATH);
            String catalogName = config.get(CATALOG_NAME);
            String databaseName = config.get(CATALOG_DATABASE_NAME);
            String topic = config.get(CATALOG_REMOTE_KAFKA_TOPIC);
            catalogConfig = CatalogConfig.of(catalogStorePath, catalogName, databaseName, topic,topicPrefix,boostrapServers);
        }else{
            metaDataType  =MetaDataType.LOG;
        }

        return new KafkaDataSink(
                boostrapServers,
                topicPrefix,
                deliveryGuarantee,
                zoneId,
                catalogConfig,
                includeSchema,
                metaDataType,
                transactionalIdPrefix
        );
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        options.add(TOPIC_PREFIX);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DELIVERY_GUARANTEE);
        options.add(TRANSACTIONAL_ID_PREFIX);
        options.add(INCLUDE_SCHEMA);
        options.add(CATALOG_TYPE);
        options.add(ENABLE_CATALOG);
        options.add(CATALOG_LOCAL_STORE_PATH);
        options.add(CATALOG_REMOTE_KAFKA_TOPIC);
        options.add(CATALOG_NAME);
        options.add(CATALOG_DATABASE_NAME);
        return options;
    }
}
