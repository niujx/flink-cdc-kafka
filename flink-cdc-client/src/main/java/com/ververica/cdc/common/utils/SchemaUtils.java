/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.utils;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for {@link Schema} to perform the ability of evolution. */
@PublicEvolving
public class SchemaUtils {

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        return createFieldGetters(schema.getColumns());
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Column} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(List<Column> columns) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(RecordData.createFieldGetter(columns.get(i).getType(), i));
        }
        return fieldGetters;
    }

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
    public static Schema applySchemaChangeEvent(Schema schema, SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            return applyAddColumnEvent((AddColumnEvent) event, schema);
        } else if (event instanceof DropColumnEvent) {
            return applyDropColumnEvent((DropColumnEvent) event, schema);
        } else if (event instanceof RenameColumnEvent) {
            return applyRenameColumnEvent((RenameColumnEvent) event, schema);
        } else if (event instanceof AlterColumnTypeEvent) {
            return applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, schema);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported schema change event type \"%s\"",
                            event.getClass().getCanonicalName()));
        }
    }

    private static Schema applyAddColumnEvent(AddColumnEvent event, Schema oldSchema) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                {
                    columns.addFirst(columnWithPosition.getAddColumn());
                    break;
                }
                case LAST:
                {
                    columns.addLast(columnWithPosition.getAddColumn());
                    break;
                }
                case BEFORE:
                {
                    Preconditions.checkNotNull(
                            columnWithPosition.getExistingColumn().getName(),
                            "existedColumnName could not be null in BEFORE type AddColumnEvent");
                    List<String> columnNames =
                            columns.stream().map(Column::getName).collect(Collectors.toList());
                    int index = columnNames.indexOf(columnWithPosition.getExistingColumn().getName());
                    if (index < 0) {
                        throw new IllegalArgumentException(
                                columnWithPosition.getExistingColumn().getName()
                                        + " of AddColumnEvent is not existed");
                    }
                    columns.add(index, columnWithPosition.getAddColumn());
                    break;
                }
                case AFTER:
                {
                    Preconditions.checkNotNull(
                            columnWithPosition.getExistingColumn().getName(),
                            "existedColumnName could not be null in AFTER type AddColumnEvent");
                    List<String> columnNames =
                            columns.stream().map(Column::getName).collect(Collectors.toList());
                    int index = columnNames.indexOf(columnWithPosition.getExistingColumn().getName()
                    );
                    if (index < 0) {
                        throw new IllegalArgumentException(
                                columnWithPosition.getExistingColumn().getName()
                                        + " of AddColumnEvent is not existed");
                    }
                    columns.add(index + 1, columnWithPosition.getAddColumn());
                    break;
                }
            }
        }
        return oldSchema.copy(columns);
    }

    private static Schema applyDropColumnEvent(DropColumnEvent event, Schema oldSchema) {
        List<String> droppedColumnNames = event.getDroppedColumns().stream().map(column -> column.getName()).collect(Collectors.toList());
        List<Column> columns =
                oldSchema.getColumns().stream()
                        .filter(
                                (column ->
                                        !droppedColumnNames.contains(column.getName())))
                        .collect(Collectors.toList());
        return oldSchema.copy(columns);
    }

    private static Schema applyRenameColumnEvent(RenameColumnEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getNameMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getNameMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(AlterColumnTypeEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }
}
