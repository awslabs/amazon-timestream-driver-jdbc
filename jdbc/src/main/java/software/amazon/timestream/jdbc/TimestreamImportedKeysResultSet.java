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
 * ResultSet for returning the empty list of imported (foreign) keys in Timestream.
 */
public class TimestreamImportedKeysResultSet extends TimestreamEmptyBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PKTABLE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PKTABLE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PKTABLE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PKCOLUMN_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FKTABLE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FKTABLE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FKTABLE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FKCOLUMN_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "KEY_SEQ"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "UPDATE_RULE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DELETE_RULE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "FK_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PK_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DEFERRABILITY"));

  /**
   * Constructor.
   */
  TimestreamImportedKeysResultSet() {
    super(COLUMNS);
  }
}
