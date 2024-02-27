package com.rock.cdc.client.parser;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.ververica.cdc.common.utils.Preconditions.checkNotNull;


// 兼容flink cdc pipeline 配置支持从参数启动任务
public class ParamPipelineDefinitionParser implements PipelineDefinitionParser {

    // Parent node keys
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String PIPELINE_KEY = "pipeline";

    // Source / sink keys
    private static final String TYPE_KEY = "type";
    private static final String NAME_KEY = "name";

    // Route keys
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = ".sink";
    private static final String ROUTE_DESCRIPTION_KEY = ".description";


    @Override
    public PipelineDef parse(Properties properties) throws Exception {

        // Source is required
        SourceDef sourceDef =
                toSourceDef(
                        checkNotNull(
                                config(properties, SOURCE_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SOURCE_KEY));

        // Sink is required
        SinkDef sinkDef =
                toSinkDef(
                        checkNotNull(
                                config(properties,SINK_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SINK_KEY));

        // Routes are optional
        List<RouteDef> routeDefs;
        routeDefs = Optional.ofNullable(config(properties,ROUTE_KEY))
                .map(configMap -> routeDefs(configMap))
                .orElse(Collections.EMPTY_LIST);

        // Pipeline configs are optional
        Configuration userPipelineConfig = toPipelineConfig(config(properties,PIPELINE_KEY));

        // Merge user config into global config
        Configuration pipelineConfig = new Configuration();
        //   pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);

        return new PipelineDef(sourceDef, sinkDef, routeDefs, null, pipelineConfig);
    }

    private SourceDef toSourceDef(Map<String, String> sourceMap) {

        // "type" field is required
        String type =
                checkNotNull(
                        sourceMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in source configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sourceMap.remove(NAME_KEY);

        return new SourceDef(type, name, Configuration.fromMap(sourceMap));
    }


    private SinkDef toSinkDef(Map<String, String> sinkMap) {
        // "type" field is required
        String type =
                checkNotNull(
                        sinkMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in sink configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sinkMap.remove(NAME_KEY);

        return new SinkDef(type, name, Configuration.fromMap(sinkMap));
    }


    private List<RouteDef> routeDefs(Map<String,String> routeMap){
        List<RouteDef> routeDefs = new ArrayList<>();
        for(Map.Entry<String,String> route:routeMap.entrySet()){
            if(!route.getKey().contains(ROUTE_SINK_TABLE_KEY))continue;
            routeDefs.add(toRouteDef(route,routeMap));
        }
        return routeDefs;
    }

    private RouteDef toRouteDef(Map.Entry<String,String> route,Map<String,String> routeMap) {

        String sourceTable = checkNotNull(StringUtils.remove(route.getKey(), ROUTE_SINK_TABLE_KEY)
                , "Missing required field \"%s\" in route configuration",
                ROUTE_SOURCE_TABLE_KEY );

        String sinkTable =   checkNotNull(route.getValue(),
                "Missing required field \"%s\" in route configuration",
                ROUTE_SINK_TABLE_KEY);

        String description =
                Optional.ofNullable(routeMap.get( sourceTable+ROUTE_DESCRIPTION_KEY))
                        .orElse(null);
        return new RouteDef(sourceTable, sinkTable, description);
    }

    private Configuration toPipelineConfig(Map<String, String> pipelineConfigMap) {
        if (pipelineConfigMap == null || pipelineConfigMap.isEmpty()) {
            return new Configuration();
        }
        return Configuration.fromMap(pipelineConfigMap);
    }

    private Map<String, String> config(Properties properties, String root) {
        String configPrefix = root + ".";
        Map<String, String> configMap = new HashMap<>();
        properties.entrySet().stream().filter(entry ->
                        entry.getKey().toString().startsWith(configPrefix))
                .forEach(objectObjectEntry -> configMap.put(StringUtils.remove(objectObjectEntry.getKey().toString(), configPrefix),
                        objectObjectEntry.getValue().toString()));
        return configMap;
    }

}
