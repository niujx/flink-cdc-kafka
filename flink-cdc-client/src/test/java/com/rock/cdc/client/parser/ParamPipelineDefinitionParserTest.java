package com.rock.cdc.client.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ParamPipelineDefinitionParserTest {

    private final PipelineDef testPipelineConf = new PipelineDef(
            new SourceDef("mysql", "mysql-source", Configuration.fromMap(
                    ImmutableMap.<String, String>builder()
                            .put("host", "127.0.0.1")
                            .put("port", "3306")
                            .put("username", "admin")
                            .put("password", "pass")
                            .put("tables", "db_gmc_ofc_t_user_info,db_gmcf_ofc_t_test")
                            .build())),
            new SinkDef("kafka", "kafka-sink",
                    Configuration.fromMap(ImmutableMap.<String, String>builder()
                            .put("bootstrap-servers", "127.19.10.1:8089")
                            .build())),
            Lists.newArrayList(),
            null,
            new Configuration()
    );

    @Test
    void testPropertiesParsingDefinition() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("source.type", "mysql");
        properties.setProperty("source.name", "mysql-source");
        properties.setProperty("source.port", "3306");
        properties.setProperty("source.host", "127.0.0.1");
        properties.setProperty("source.username", "admin");
        properties.setProperty("source.password", "pass");
        properties.setProperty("source.tables", "db_gmc_ofc_t_user_info,db_gmcf_ofc_t_test");
        properties.setProperty("sink.type", "kafka");
        properties.setProperty("sink.name", "kafka-sink");
        properties.setProperty("sink.bootstrap-servers", "127.19.10.1:8089");
        ParamPipelineDefinitionParser paramPipelineDefinitionParser = new ParamPipelineDefinitionParser();
        PipelineDef pipelineDef = paramPipelineDefinitionParser.parse(properties);
        assertThat(pipelineDef).isEqualTo(testPipelineConf);
    }
}