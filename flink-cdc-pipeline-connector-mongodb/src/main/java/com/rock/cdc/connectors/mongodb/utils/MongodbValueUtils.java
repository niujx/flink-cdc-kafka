package com.rock.cdc.connectors.mongodb.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.io.Serializable;
import java.util.*;

public class MongodbValueUtils implements Serializable {
    protected static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    protected final TimeZone localTimeZone;

    public MongodbValueUtils(TimeZone localTimeZone) {
        this.localTimeZone = localTimeZone;
    }

     public  Map<String, Object> extractColumns(BsonDocument document) throws JsonProcessingException {
        Map<String, Object> columns = Maps.newHashMap();
        for (Map.Entry<String, BsonValue> column : document.entrySet()) {
            final String fieldName = column.getKey();
            final BsonValue value = column.getValue();
            Object o = toValue(value);
            columns.put(fieldName, o);
        }
        return columns;
    }

    public Object toValue(BsonValue bsonValue) throws JsonProcessingException {
        switch (bsonValue.getBsonType()) {
            case INT32:
                return bsonValue.asInt32().getValue();
            case INT64:
                return bsonValue.asInt64().getValue();
            case ARRAY:
                final List<Object> collect = new ArrayList<>();
                for (BsonValue bsonValue1 : bsonValue.asArray()) {
                    Object o;
                    if (bsonValue1 instanceof BsonDocument) {
                        o = extractColumns(bsonValue1.asDocument());
                    }else{
                        o = toValue(bsonValue1);
                    }
                    collect.add(o);
                }
                return new ObjectMapper().writeValueAsString(collect);
            case DOCUMENT:
                return bsonValue.asDocument();
            case DATE_TIME:
                final long value = bsonValue.asDateTime().getValue();
                return DateFormatUtils.format(new Date(value), DATE_FORMAT_PATTERN, localTimeZone);
            case TIMESTAMP:
                return DateFormatUtils.format(new Date(bsonValue.asTimestamp().getTime() * 1000L), DATE_FORMAT_PATTERN, localTimeZone);
            case DOUBLE:
                return bsonValue.asDouble().getValue();
            case OBJECT_ID:
                return bsonValue.asObjectId().getValue().toString();
            case BOOLEAN:
                return bsonValue.asBoolean().getValue();
            case NULL:
                return null;
            case DECIMAL128:
                return bsonValue.asDecimal128().getValue().toString();
            case STRING:
            case BINARY:
            default:
                return bsonValue.asString().getValue();
        }
    }
}
