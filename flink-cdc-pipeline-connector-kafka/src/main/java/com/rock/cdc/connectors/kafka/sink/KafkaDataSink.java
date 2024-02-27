package com.rock.cdc.connectors.kafka.sink;

import com.rock.cdc.connectors.kafka.sink.metadata.LogMetadataApplier;
import com.rock.cdc.connectors.kafka.sink.metadata.MetaDataType;
import com.rock.cdc.connectors.kafka.sink.metadata.MetadataApplierFactory;
import com.rock.cdc.connectors.kafka.sink.metadata.RemoteCatalogMetadataApplier;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

import java.io.Serializable;
import java.time.ZoneId;

import static com.ververica.cdc.common.utils.Preconditions.checkNotNull;

@Slf4j
public class KafkaDataSink implements DataSink, Serializable {


    private final String boostrapServers;
    private final String topicPrefix;
    private final DeliveryGuarantee deliveryGuarantee;
    private final ZoneId zoneId;
    private final CatalogConfig catalogConfig;
    private final boolean includeSchema;
    private final MetaDataType metaDataType;

    public KafkaDataSink(String boostrapServers,
                         String topicPrefix,
                         DeliveryGuarantee deliveryGuarantee,
                         ZoneId zoneId,
                         CatalogConfig catalogConfig,
                         Boolean includeSchema,
                         MetaDataType metaDataType,
                         String transactionalIdPrefix) {
        this.boostrapServers = checkNotNull(boostrapServers);
        this.topicPrefix = topicPrefix;
        this.deliveryGuarantee = deliveryGuarantee;
        this.zoneId = zoneId;
        this.catalogConfig = catalogConfig;
        this.includeSchema = includeSchema;
        this.metaDataType = metaDataType;
        this.transactionalIdPrefix = transactionalIdPrefix;
    }

    private final String transactionalIdPrefix;

    @Override
    public EventSinkProvider getEventSinkProvider() {

        KafkaSinkBuilder<Event> builder = KafkaSink.<Event>builder()
                .setBootstrapServers(boostrapServers);

        if (deliveryGuarantee != null) {
            builder.setDeliveryGuarantee(deliveryGuarantee);
        }

        if (StringUtils.isNotBlank(transactionalIdPrefix)) {
            builder.setTransactionalIdPrefix(transactionalIdPrefix);
        }

        KafkaSink<Event> kafkaSink = builder
                .setRecordSerializer(new KafkaEventSerializer(topicPrefix, zoneId, includeSchema))
                .build();

        return FlinkSinkProvider.of(kafkaSink);

    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return MetadataApplierFactory.metadataApplier(metaDataType, catalogConfig);
    }
}
