package com.rock.cdc.connectors.mongodb.utils;

import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import org.apache.commons.compress.utils.Lists;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.util.List;
import java.util.Map;

public class MongodbTypeUtils {

    private MongodbTypeUtils() {
    }

    public static  Schema createSchema(BsonDocument bsonDocument){
        Schema.Builder builder = Schema.newBuilder();
        for (Map.Entry<String, BsonValue> column : bsonDocument.entrySet()) {
            if(column.getValue().isArray() && column.getValue().asArray().isEmpty()){
                continue;
            }
            builder.physicalColumn(column.getKey(),createDataType(column.getValue()));
        }
        builder.comment("mongodb");
        builder.primaryKey("_id");
        return builder.build();
    }

    public static DataType createDataType(BsonValue bsonValue) {
        BsonType type = bsonValue.getBsonType();
        switch (type) {
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
            case OBJECT_ID:
            case SYMBOL:
            case JAVASCRIPT_WITH_SCOPE:
            case JAVASCRIPT:
            case ARRAY:
                return DataTypes.STRING();
            case DOCUMENT:
                return convertToRow(bsonValue.asDocument());
            //    return convertToArray(bsonValue.asArray());
            case BINARY:
                return DataTypes.BINARY(Integer.MAX_VALUE);
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case INT32:
                return DataTypes.INT();
            case DATE_TIME:
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(3);
            case INT64:
                return DataTypes.BIGINT();
            case DECIMAL128:
                return DataTypes.DECIMAL(18, 6);
            case DB_POINTER:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DataType convertToRow(BsonDocument document) {
        List<DataField>  dataFields= Lists.newArrayList();
        for (Map.Entry<String, BsonValue> column : document.entrySet()) {
            if(column.getValue().isArray() && column.getValue().asArray().isEmpty()) {
                continue;
            }
            DataField field  = new DataField(column.getKey(),MongodbTypeUtils.createDataType(column.getValue()));
            dataFields.add(field);

        }
        return DataTypes.ROW(dataFields.toArray(new DataField[]{}));
    }

    private static DataType convertToArray(BsonArray bsonArray) {
        return DataTypes.ARRAY(createDataType(bsonArray.get(0)));
    }
}
