package com.rock.cdc.connectors.kafka.sink;

import junit.framework.TestCase;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.*;

import java.util.List;
import java.util.Optional;

import static java.lang.System.out;

public class CatalogTest extends TestCase {

    private static ConfigOption<String> TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kafka bootstrap address");

    private CatalogStore catalogStore = new FileCatalogStore(CatalogTest.class.getResource("/").getPath());

    private String catalogName = "ods_test";


    @Test
    public void testManualCreateCatalog() {
        catalogStore.open();
        Optional<CatalogDescriptor> mysqlCatalog = catalogStore.getCatalog(catalogName);
        CatalogDescriptor catalogDescriptor = mysqlCatalog.get();
        CatalogFactory catalogFactory = FactoryUtil.discoverFactory(ClassLoader.getSystemClassLoader(), CatalogFactory.class,
                catalogDescriptor.getConfiguration().getString(TYPE));

        catalogDescriptor.getConfiguration().removeKey("name");
        catalogDescriptor.getConfiguration().removeKey("type");

        CatalogFactory.Context defaultCatalogContext = new FactoryUtil.DefaultCatalogContext(
                catalogName,
                catalogDescriptor.getConfiguration().toMap(),
                catalogDescriptor.getConfiguration(),
                ClassLoader.getSystemClassLoader()
        );
        Catalog catalog = catalogFactory.createCatalog(defaultCatalogContext);
        catalog.open();
        List<String> strings = catalog.listDatabases();
        strings.forEach(out::println);
        Assert.assertFalse(strings.isEmpty());
    }

    @Test
    public void testUseCatalogManager() {
        catalogStore.open();

        CatalogManager catalogManager = CatalogManager.newBuilder()
                .catalogStoreHolder(CatalogStoreHolder
                        .newBuilder()
                        .catalogStore(catalogStore)
                        .factory(new FileCatalogStoreFactory())
                        .config(new Configuration())
                        .classloader(Thread.currentThread().getContextClassLoader())
                        .build())
                .classLoader(Thread.currentThread().getContextClassLoader())
                .config(new Configuration())
                .defaultCatalog("defaultCatalogName", new GenericInMemoryCatalog("default"))
                .build();

        Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
        catalog.ifPresent(c->c.listDatabases().forEach(out::println));
        catalog.ifPresent(c -> Assert.assertFalse(c.listDatabases().isEmpty()));


    }

}