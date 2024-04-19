package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb;

import java.util.Collections;

import static org.tikv.common.codec.TableCodec.decodeObjects;

public class SimpleTiKVSnapshotEventDeserializationSchema extends TiKVEventDeserializationSchemaBase implements TiKVSnapshotEventDeserializationSchema<Event> {

    private boolean sendCreate = true;


    public SimpleTiKVSnapshotEventDeserializationSchema(TiTableInfo tiTableInfo, TableId tableId) {
        super(tiTableInfo, tableId);
    }


    @Override
    public void deserialize(Kvrpcpb.KvPair record, Collector<Event> collector) throws Exception {
        Object[] tikvValues =
                decodeObjects(
                        record.getValue().toByteArray(),
                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                        tableInfo);
        //只发送一个schema信息
        if (sendCreate) {
            collector.collect(new CreateTableEvent(tableId, schema));
            sendCreate = false;
        }

        RecordData rowDataUpdateAfter =
                (RecordData) physicalConverter.convert(tikvValues, tableInfo, null);

        DataChangeEvent insertEvent = DataChangeEvent.insertEvent(tableId, rowDataUpdateAfter, Collections.emptyMap());
        collector.collect(insertEvent);


    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

}
