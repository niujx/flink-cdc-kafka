package com.rock.cdc.client.composer;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.flink.FlinkEnvironmentUtils;
import com.ververica.cdc.composer.flink.FlinkPipelineExecution;
import com.ververica.cdc.composer.flink.coordination.OperatorIDGenerator;
import com.ververica.cdc.composer.flink.translator.*;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//重新创建了StreamExecutionEnvironment copy FlinkPipelineComposer
public class ClientPipelineComposer implements PipelineComposer {

    private final StreamExecutionEnvironment env;

    private ClientPipelineComposer(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public static ClientPipelineComposer create(org.apache.flink.configuration.Configuration configuration) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        executionEnvironment.getConfig().setGlobalJobParameters(configuration);
        return new ClientPipelineComposer(executionEnvironment);
    }

    @Override
    public PipelineExecution compose(PipelineDef pipelineDef) {
        int parallelism = pipelineDef.getConfig().get(PipelineOptions.PIPELINE_PARALLELISM);
        env.getConfig().setParallelism(parallelism);

        // Source
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();
        DataStream<Event> stream =
                sourceTranslator.translate(pipelineDef.getSource(), env, pipelineDef.getConfig());

        // Route
        RouteTranslator routeTranslator = new RouteTranslator();
        stream = routeTranslator.translate(stream, pipelineDef.getRoute());

        // Create sink in advance as schema operator requires MetadataApplier
        DataSink dataSink = createDataSink(pipelineDef.getSink(), pipelineDef.getConfig());

        // Schema operator
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID));
        stream =
                schemaOperatorTranslator.translate(
                        stream, parallelism, dataSink.getMetadataApplier());
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        // Add partitioner
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();
        stream =
                partitioningTranslator.translate(
                        stream, parallelism, parallelism, schemaOperatorIDGenerator.generate());

        // Sink
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                pipelineDef.getSink(), stream, dataSink, schemaOperatorIDGenerator.generate());

        return new FlinkPipelineExecution(env,pipelineDef.getConfig().get(PipelineOptions.PIPELINE_NAME),true);
    }

    private DataSink createDataSink(SinkDef sinkDef, Configuration pipelineConfig) {
        // Search the data sink factory
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        // Include sink connector JAR
        FactoryDiscoveryUtils.getJarPathByIdentifier(sinkDef.getType(), DataSinkFactory.class)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // Create data sink
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader()));
    }


}
