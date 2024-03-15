package com.rock.cdc.connectors.mongodb.factory;

import com.google.common.collect.Maps;
import com.rock.cdc.connectors.mongodb.source.MongodbDataSource;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.source.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.support.hierarchical.ThrowableCollector;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongodbDataSourceFactoryTest {

    @Test
    void createDataSource() {

        Map<String,String> optons = Maps.newHashMap();
        optons.put("hosts","m8001-prd-market-sms-channel.db.gmfcloud.com:8001,s8001-prd-market-sms-channel.db.gmfcloud.com:8001,h8001-prd-market-sms-channel.db.gmfcloud.com:8001");
        optons.put("username","flinkuser");
        optons.put("password","YmM0MmE4NDllNzU2MGMw");
        optons.put("database","market_db");
        optons.put("collection","market_db.t_channel_user_strike_result_0");
        optons.put("scan.startup.mode","latest-offset");


        Configuration configuration = Configuration.fromMap(optons);

        MongodbDataSourceFactory mongodbDataSourceFactory = new MongodbDataSourceFactory();

        DataSource dataSource = mongodbDataSourceFactory.createDataSource(new FactoryHelper.DefaultContext(configuration,
                configuration, Thread.currentThread().getContextClassLoader()));

        assertTrue(dataSource instanceof DataSource);

    }
}