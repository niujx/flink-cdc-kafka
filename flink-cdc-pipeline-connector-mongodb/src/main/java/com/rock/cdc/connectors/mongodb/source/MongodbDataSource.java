package com.rock.cdc.connectors.mongodb.source;

import com.rock.cdc.connectors.mongodb.utils.MongodbValueUtils;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.TimeZone;

public class MongodbDataSource implements DataSource {

    private final MongoDBSourceConfigFactory configFactory;
    private final ZoneId zoneId;

    public MongodbDataSource(MongoDBSourceConfigFactory configFactory, ZoneId zoneId) {
        this.configFactory = configFactory;
        this.zoneId = zoneId;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        MongodbValueUtils mongodbValueUtils = new MongodbValueUtils(TimeZone.getTimeZone(zoneId));
        MongodbEventDeserializer deserializationSchema = new MongodbEventDeserializer(mongodbValueUtils);
        MongoDBSource<Event> mongoDBSource = createSource()
                .deserializer(deserializationSchema)
                .build();
        return FlinkSourceProvider.of(mongoDBSource);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return null;
    }


    private MongoDBSourceBuilder<Event> createSource() {
        Unsafe unsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
            long fieldOffset = unsafe.objectFieldOffset(MongoDBSourceBuilder.class.getDeclaredField("configFactory"));
            MongoDBSourceBuilder<Event> builder = new MongoDBSourceBuilder<>();
            unsafe.putObject(builder, fieldOffset, configFactory);
            return builder;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
