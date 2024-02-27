package com.rock.cdc.connectors.kafka.sink;


import com.rock.cdc.connectors.kafka.sink.metadata.MetaDataType;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;

public class KafkaDataSinkOptions {

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap-servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kafka bootstrap address");


    public static final ConfigOption<String> TOPIC_PREFIX =
            ConfigOptions.key("topic-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kafka topic prefix");


    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("deliver-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .noDefaultValue()
                    .withDescription("deliver semantic");


    public static final ConfigOption<String> TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key("transactional-id-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("set kafka transaction id prefix");

    public static final ConfigOption<Boolean> INCLUDE_SCHEMA =
            ConfigOptions.key("include-schema")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("send kafka data  whether include table schema");


    public static final ConfigOption<Boolean> ENABLE_CATALOG =
            ConfigOptions.key("catalog-enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("enable catalog manage table schema");

    public static final ConfigOption<MetaDataType> CATALOG_TYPE =
            ConfigOptions.key("catalog-type")
                    .enumType(MetaDataType.class)
                    .defaultValue(MetaDataType.LOG)
                    .withDescription("use catalog type");

    public static final ConfigOption<String> CATALOG_LOCAL_STORE_PATH =
            ConfigOptions.key("catalog-local-store-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("flink file catalog  store path");

    public static final ConfigOption<String> CATALOG_NAME =
            ConfigOptions.key("catalog-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("catalog name");

    public static final ConfigOption<String> CATALOG_DATABASE_NAME =
            ConfigOptions.key("catalog-database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("store schema  database name");

    public static final ConfigOption<String> CATALOG_REMOTE_KAFKA_TOPIC =
            ConfigOptions.key("catalog-remote-kafka-topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("send metadata kafka topic ");
}
