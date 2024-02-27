package com.rock.cdc.connectors.kafka.sink;

import lombok.Data;

import java.io.Serializable;

@Data(staticConstructor = "of")
public class CatalogConfig implements Serializable {

    private final String catalogStorePath;
    private final String catalog;
    private final String database;
    private final String topic;
    private final String prefix;
    private final String bootstrapServers;
}
