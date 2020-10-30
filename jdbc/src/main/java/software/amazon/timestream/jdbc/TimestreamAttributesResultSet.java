/*
 *  Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package software.amazon.timestream.jdbc;

import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Result set to return an empty list of attributes in Timestream.
 */
public class TimestreamAttributesResultSet extends TimestreamEmptyBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "ATTR_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "ATTR_TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "ATTR_SIZE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DECIMAL_DIGITS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NUM_PREC_RADIX"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NULLABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "REMARKS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "ATTR_DEF"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATETIME_SUB"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "CHAR_OCTET_LENGTH"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "ORDINAL_POSITION"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "IS_NULLABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_CATALOG"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_SCHEMA"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_TABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SOURCE_DATA_TYPE"));


  /**
   * Constructor.
   */
  TimestreamAttributesResultSet() {
    super(COLUMNS);
  }
}
