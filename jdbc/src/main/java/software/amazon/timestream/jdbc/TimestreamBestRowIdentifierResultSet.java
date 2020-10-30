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
 * Result set to return an empty list of best row identifiers in Timestream.
 */
public class TimestreamBestRowIdentifierResultSet extends TimestreamEmptyBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SCOPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "COLUMN_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "COLUMN_SIZE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "BUFFER_LENGTH"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DECIMAL_DIGITS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "PSEUDO_COLUMN"));

  /**
   * Constructor.
   */
  TimestreamBestRowIdentifierResultSet() {
    super(COLUMNS);
  }
}
