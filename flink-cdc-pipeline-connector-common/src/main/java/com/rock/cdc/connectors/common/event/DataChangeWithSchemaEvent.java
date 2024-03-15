package com.rock.cdc.connectors.common.event;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;

import java.util.Map;

public class DataChangeWithSchemaEvent implements ChangeEvent {

    private DataChangeEvent dataChangeEvent;
    private Schema schema;


    private DataChangeWithSchemaEvent(DataChangeEvent dataChangeEvent, Schema schema) {
        this.dataChangeEvent = dataChangeEvent;
        this.schema = schema;
    }

    @Override
    public TableId tableId() {
        return dataChangeEvent.tableId();
    }
    public Schema schema() {
        return schema;
    }

    public DataChangeEvent dataChangeEvent(){
        return  dataChangeEvent;
    }

    public static DataChangeWithSchemaEvent insertEvent(TableId tableId, RecordData after, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.insertEvent(tableId, after), schema);
    }

    public static DataChangeWithSchemaEvent insertEvent(
            TableId tableId, RecordData after, Map<String, String> meta, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.insertEvent(tableId, after, meta), schema);
    }

    public static DataChangeWithSchemaEvent deleteEvent(TableId tableId, RecordData before, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.deleteEvent(tableId, before), schema);
    }

    public static DataChangeWithSchemaEvent deleteEvent(
            TableId tableId, RecordData before, Map<String, String> meta, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.deleteEvent(tableId, before, meta), schema);
    }

    public static DataChangeWithSchemaEvent updateEvent(
            TableId tableId, RecordData before, RecordData after, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.updateEvent(tableId, before, after), schema);
    }

    public static DataChangeWithSchemaEvent updateEvent(
            TableId tableId, RecordData before, RecordData after, Map<String, String> meta, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.updateEvent(tableId, before, after, meta), schema);
    }

    public static DataChangeWithSchemaEvent replaceEvent(TableId tableId, RecordData after, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.replaceEvent(tableId, after), schema);
    }

    public static DataChangeWithSchemaEvent replaceEvent(
            TableId tableId, RecordData after, Map<String, String> meta, Schema schema) {
        return new DataChangeWithSchemaEvent(DataChangeEvent.replaceEvent(tableId, after, meta), schema);
    }

}
