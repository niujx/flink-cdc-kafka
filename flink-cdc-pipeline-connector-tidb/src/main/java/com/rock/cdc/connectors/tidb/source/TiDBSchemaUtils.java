package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.common.schema.Schema;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;

public class TiDBSchemaUtils {


    public static Schema createSchema(TiTableInfo tiTableInfo){
        Schema.Builder builder = Schema.newBuilder();
        for (TiColumnInfo column : tiTableInfo.getColumns()) {
            builder.physicalColumn(column.getName(),TiDBTypesUtils.toDateType(column));
        }
        builder.primaryKey(tiTableInfo.getPKIsHandleColumn().getName());
        return builder.build();
    }



}
