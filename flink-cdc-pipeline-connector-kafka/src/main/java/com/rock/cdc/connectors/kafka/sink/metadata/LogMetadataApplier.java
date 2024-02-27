package com.rock.cdc.connectors.kafka.sink.metadata;

import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.sink.MetadataApplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogMetadataApplier implements MetadataApplier {
    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        log.info("schema change: ", schemaChangeEvent);
    }
}
