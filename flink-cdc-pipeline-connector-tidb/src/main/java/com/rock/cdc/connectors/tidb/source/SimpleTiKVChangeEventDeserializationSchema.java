package com.rock.cdc.connectors.tidb.source;

import com.rock.cdc.connectors.tidb.utils.TiTableInfoUtils;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.RowDataTiKVEventDeserializationSchemaBase;
import com.ververica.cdc.connectors.tidb.table.TiKVMetadataConverter;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

import static org.tikv.common.codec.TableCodec.decodeObjects;
@Slf4j
public class SimpleTiKVChangeEventDeserializationSchema extends TiKVEventDeserializationSchemaBase implements TiKVChangeEventDeserializationSchema<Event> {


    //根据任务启动数据初始化配置如果是INITIAL就交给 Snapshot 来发送schema
    private boolean sendCreate;

    public SimpleTiKVChangeEventDeserializationSchema(TiTableInfo tableInfo, TableId tableId, boolean sendCreate) {
        super(tableInfo, tableId);
        this.sendCreate = sendCreate;
    }

    @Override
    public void deserialize(Cdcpb.Event.Row row, Collector<Event> collector) throws Exception {

        if (sendCreate) {
            collector.collect(new CreateTableEvent(tableId, schema));
            sendCreate = false;
        }

        RowKey rawKey = RowKey.decode(row.getKey().toByteArray());
        long handle = rawKey.getHandle();
        Object[] oldValues = decodeObjects(row.getOldValue().toByteArray(),handle,tableInfo);
        Object[] currentValues = decodeObjects(row.getValue().toByteArray(),handle,tableInfo);

        ChangeEvent changeEvent = null;
        switch (row.getOpType()) {
            case PUT:
                RecordData rowDataUpdateBefore = null;
                RecordData rowDataUpdateAfter;
                if (row.getOldValue() != null && !row.getOldValue().isEmpty()) {
                    rowDataUpdateBefore =
                            (RecordData) physicalConverter.convert(oldValues, tableInfo, null);
                }
                rowDataUpdateAfter =
                        (RecordData) physicalConverter.convert(currentValues, tableInfo, null);


                if (rowDataUpdateBefore == null) {
                    changeEvent = DataChangeEvent.insertEvent(tableId, rowDataUpdateAfter);
                }else{
                    changeEvent = DataChangeEvent.updateEvent(tableId,rowDataUpdateAfter,rowDataUpdateBefore);
                }
                break;
            case DELETE:
                rowDataUpdateBefore =
                        (RecordData) physicalConverter.convert(oldValues, tableInfo, null);
                changeEvent = DataChangeEvent.deleteEvent(tableId,rowDataUpdateBefore);
                break;
            default:
                log.info("not support opType:{}",row.getOpType());
        }

        if(changeEvent!=null){
            collector.collect(changeEvent);
        }


    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }


}
