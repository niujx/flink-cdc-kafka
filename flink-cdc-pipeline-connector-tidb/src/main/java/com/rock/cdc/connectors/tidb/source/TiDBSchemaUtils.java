package com.rock.cdc.connectors.tidb.source;

import com.ververica.cdc.common.schema.Schema;
import org.apache.commons.compress.utils.Lists;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;

import java.util.List;

public class TiDBSchemaUtils {


    public static Schema createSchema(TiTableInfo tiTableInfo) {
        Schema.Builder builder = Schema.newBuilder();
        List<String> primaryKeys = Lists.newArrayList();
        for (TiColumnInfo column : tiTableInfo.getColumns()) {
            builder.physicalColumn(column.getName(),
                    column.getType().isNotNull() ? TiDBTypesUtils.toDateType(column).notNull() : TiDBTypesUtils.toDateType(column));
            if (column.isPrimaryKey()) {
                primaryKeys.add(column.getName());
            }
        }

        builder.primaryKey(primaryKeys);

        return builder.build();
    }


}
