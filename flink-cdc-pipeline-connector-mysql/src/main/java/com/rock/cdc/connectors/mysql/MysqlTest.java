package com.rock.cdc.connectors.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlTest {

    public static void main(String[] args) throws Exception {
        MySqlSource<String>  source= MySqlSource.<String>builder()
                .hostname("10.152.18.41")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("db_gmcf_emc")
                .tableList("db_gmcf_emc.TestTable")
                .startupOptions(StartupOptions.earliest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"mysql cdc")
                .setParallelism(1).print();

        env.execute();
    }
}
