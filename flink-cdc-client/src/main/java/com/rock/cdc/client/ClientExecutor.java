package com.rock.cdc.client;

import com.rock.cdc.client.composer.ClientPipelineComposer;
import com.rock.cdc.client.parser.ParamPipelineDefinitionParser;
import com.rock.cdc.client.parser.PipelineDefinitionParser;
import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.collect.Maps;

import java.util.Properties;

@Slf4j
public class ClientExecutor {

    private final Properties properties;

    public ClientExecutor(Properties properties) {
        this.properties = properties;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        PipelineDefinitionParser parser = new ParamPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(properties);
        PipelineComposer composer = getComposer();
        PipelineExecution execution = composer.compose(pipelineDef);
        return execution.execute();
    }

    private PipelineComposer getComposer() {
        Configuration configuration = Configuration.fromMap(Maps.fromProperties(properties));
        return ClientPipelineComposer.create(configuration);
    }


}
