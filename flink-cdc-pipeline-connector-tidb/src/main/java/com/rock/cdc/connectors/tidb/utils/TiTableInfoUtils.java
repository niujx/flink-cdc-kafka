package com.rock.cdc.connectors.tidb.utils;

import com.ververica.cdc.common.event.TableId;
import org.apache.flink.util.FlinkRuntimeException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiTableInfo;

public class TiTableInfoUtils {

    private TiConfiguration tiConfiguration;

    public TiTableInfoUtils(TiConfiguration tiConfiguration) {
        this.tiConfiguration = tiConfiguration;
    }

    public TiTableInfo fetchTableInfo(TableId tableId) {
        try (final TiSession session = TiSession.create(tiConfiguration)) {
            return session.getCatalog().getTable(tableId.getSchemaName(), tableId.getTableName());
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public TiTableInfo fetchTableInfo(String dbName, Long tableId) {
        try (final TiSession session = TiSession.create(tiConfiguration)) {
            TiDBInfo tiDBInfo = session.getCatalog().getDatabase(dbName);
            return session.getCatalog().getTable(tiDBInfo, tableId);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public TiTableInfo fetchTableInfo(String dbName, String tableName) {
        try (final TiSession session = TiSession.create(tiConfiguration)) {
            return session.getCatalog().getTable(dbName, tableName);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }


}
