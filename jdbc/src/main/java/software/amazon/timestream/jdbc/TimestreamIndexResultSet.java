/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.timestream.jdbc;

import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ResultSet for returning the empty list of indexes in Timestream.
 */
public class TimestreamIndexResultSet extends TimestreamEmptyBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BOOLEAN, "NON_UNIQUE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "INDEX_QUALIFIER"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "INDEX_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "ORDINAL_POSITION"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "COLUMN_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "ASC_OR_DESC"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BIGINT, "CARDINALITY"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BIGINT, "PAGES"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FILTER_CONDITION"));

  /**
   * Constructor.
   */
  TimestreamIndexResultSet() {
    super(COLUMNS);
  }
}
