/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Result set to return an empty list of procedures in Timestream.
 */
public class TimestreamProceduresResultSet extends TimestreamEmptyBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PROCEDURE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PROCEDURE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "PROCEDURE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "NUM_INPUT_PARAMS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "NUM_OUTPUT_PARAMS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "NUM_RESULT_SETS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "REMARKS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "PROCEDURE_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SPECIFIC_NAME"));

  /**
   * Constructor.
   */
  TimestreamProceduresResultSet() {
    super(COLUMNS);
  }
}
