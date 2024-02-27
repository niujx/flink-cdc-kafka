package com.rock.cdc.connectors.kafka.sink;

import com.ververica.cdc.common.schema.Schema;
import lombok.Data;
import org.apache.htrace.shaded.fasterxml.jackson.annotation.JsonProperty;


import java.util.List;
import java.util.Map;

@Data
public class KafkaDebeziumRecord {
    private String op;
    private Source source;
    private List<String> pkNames;
    private List<Object> keys;
    private Map<String, Object> after;
    private Map<String, Object> before;
    @JsonProperty("ingestion-timestamp")
    private Long ingestionTimestamp;
    private Schema schema;


    @Data
    public static class Source {
        private Long timestamp;
        private String database;
        private String table;
    }
}
