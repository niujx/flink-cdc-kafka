package com.rock.cdc.connectors.kafka.sink.metadata;

import com.rock.cdc.connectors.kafka.sink.CatalogConfig;
import com.ververica.cdc.common.sink.MetadataApplier;

public class MetadataApplierFactory {

    public static  MetadataApplier metadataApplier(MetaDataType metaDataType, CatalogConfig catalogConfig){
         switch (metaDataType){
            case LOG:
                return new LogMetadataApplier();
             case LOCAL:
                 return new LocalCatalogMetadataApplier(catalogConfig);
             case REMOTE:
                 return new RemoteCatalogMetadataApplier(catalogConfig);
             default:
                 throw new RuntimeException("not support this type"+metaDataType);
        }
    }
}
