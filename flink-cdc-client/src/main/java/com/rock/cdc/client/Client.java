package com.rock.cdc.client;

import com.ververica.cdc.composer.PipelineExecution;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

@Slf4j
public class Client {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool
                .fromPropertiesFile(Client.class.getClassLoader().getResourceAsStream("application.properties"))
                .mergeWith(ParameterTool.fromArgs(args));

        System.setProperty("HADOOP_USER_NAME",parameterTool.get("hadoop.username"));

        Properties properties = parameterTool.getProperties();
        ClientExecutor clientExecutor = new ClientExecutor(properties);
        PipelineExecution.ExecutionInfo result = clientExecutor.run();
        printExecutionInfo(result);
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }

}
