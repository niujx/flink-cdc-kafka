package com.rock.cdc.connectors.tidb.source;


import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import org.tikv.common.meta.TiColumnInfo;

import static org.tikv.common.types.MySQLType.TypeDecimal;

public class TiDBTypesUtils {

    public static DataType toDateType(TiColumnInfo columnInfo) {
        int length = Long.valueOf(columnInfo.getType().getLength()).intValue();
        int scale = columnInfo.getType().getDecimal();

        switch (columnInfo.getType().getType()) {
            case TypeDecimal:
            case TypeNewDecimal:
                int precision = length;
                return DataTypes.DECIMAL(precision, scale);
            case TypeTiny:
                return DataTypes.TINYINT();
            case TypeShort:
                return DataTypes.SMALLINT();
            case TypeLong:
            case TypeLonglong:
                return DataTypes.BIGINT();
            case TypeFloat:
                return DataTypes.FLOAT();
            case TypeDouble:
                return DataTypes.DOUBLE();
            case TypeTimestamp:
                return scale >= 0 ?
                        DataTypes.TIMESTAMP(scale) :
                        DataTypes.TIMESTAMP();
            case TypeInt24:
                return DataTypes.INT();
            case TypeDate:
                return DataTypes.DATE();
//            case TypeDuration :
//                return DataTypes.INTERVAL(columnInfo)
            case TypeDatetime:
                return scale > 0 ?
                        DataTypes.TIMESTAMP(scale) :
                        DataTypes.TIMESTAMP(0);
            case TypeVarchar:
                return DataTypes.VARCHAR(length);
            case TypeBit:
                return length == 1 ? DataTypes.BOOLEAN() :
                        DataTypes.BINARY(length);
            case TypeJSON:
            case TypeEnum:
            case TypeVarString:
            case TypeString:
            case TypeGeometry:
                return DataTypes.STRING();
            case TypeSet:
                return DataTypes.ARRAY(DataTypes.STRING());
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeLongBlob:
            case TypeBlob:
                return DataTypes.BYTES();


            default:
                return null;
        }
    }
}
