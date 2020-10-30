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
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.Row;
import com.google.common.collect.ImmutableList;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * ResultSet containing information about each Timestream data type.
 */
public class TimestreamTypeInfoResultSet extends TimestreamBaseResultSet {
  private static final Datum NULL_DATUM = new Datum().withNullValue(Boolean.TRUE);
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "PRECISION"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "LITERAL_PREFIX"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "LITERAL_SUFFIX"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "CREATE_PARAMS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NULLABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BOOLEAN, "CASE_SENSITIVE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SEARCHABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BOOLEAN, "UNSIGNED_ATTRIBUTE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BOOLEAN, "FIXED_PREC_SCALE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.BOOLEAN, "AUTO_INCREMENT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "LOCAL_TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "MINIMUM_SCALE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "MAXIMUM_SCALE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATETIME_SUB"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NUM_PREC_RADIX"));

  private final List<Row> types = new ArrayList<>();
  private boolean isAfterLast = false;

  /**
   * Constructor for TimestreamTypeInfoResultSet.
   */
  public TimestreamTypeInfoResultSet() {
    super(null, 1000);
    this.rsMeta = createColumnMetadata(COLUMNS);
    populateCurrentRows();
  }

  @Override
  protected void doClose() {
    // Do nothing.
  }

  @Override
  protected boolean doNextPage() {
    isAfterLast = true;
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return isAfterLast;
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return !isAfterLast && !rowItr.hasNext();
  }

  /**
   * Map the list of type info into a Timestream Row type.
   */
  private void populateCurrentRows() {
    Stream.of(TimestreamDataType.values()).forEach(
      type ->
        types.add(new Row().withData(
          createDatum(type.toString()),
          createDatum(type.getJdbcType().jdbcCode),
          createDatum(type.getPrecision()),
          getPrefixes(type),
          getSuffixes(type),
          NULL_DATUM,
          createDatum(DatabaseMetaData.typeNullable),
          (type == TimestreamDataType.VARCHAR)
            ? createDatum(Boolean.TRUE.toString())
            : NULL_DATUM,
          createDatum(DatabaseMetaData.typeSearchable),
          createDatum(Boolean.FALSE.toString()),
          createDatum(Boolean.FALSE.toString()),
          createDatum(Boolean.FALSE.toString()),
          createDatum(type.getClassName()),
          (type == TimestreamDataType.TIME || type == TimestreamDataType.TIMESTAMP)
            ? createDatum(9)
            : NULL_DATUM,
          (type == TimestreamDataType.TIME || type == TimestreamDataType.TIMESTAMP)
            ? createDatum(9)
            : NULL_DATUM,
          createDatum(type.getJdbcType().jdbcCode),
          ((type == TimestreamDataType.DATE)
            || (type == TimestreamDataType.TIME)
            || (type == TimestreamDataType.TIMESTAMP))
            ? createDatum(type.getJdbcType().jdbcCode % 90) : NULL_DATUM,
          (type == TimestreamDataType.DOUBLE) ? createDatum(10) : NULL_DATUM))
    );

    this.rowItr = types.iterator();
  }

  /**
   * Gets the literal prefix for the given {@link TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType}.
   * @return the literal prefix.
   */
  private Datum getPrefixes(TimestreamDataType type) {
    switch (type) {
      case VARCHAR: {
        return createDatum("'");
      }

      case BIGINT: {
        return createDatum("BIGINT '");
      }

      case DOUBLE: {
        return createDatum("DOUBLE '");
      }

      case INTEGER: {
        return createDatum("INT '");
      }

      default: {
        return NULL_DATUM;
      }
    }
  }

  /**
   * Gets the literal suffix for the given {@link TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType}.
   * @return the literal suffix.
   */
  private Datum getSuffixes(TimestreamDataType type) {
    switch (type) {
      case VARCHAR:
      case BIGINT:
      case DOUBLE:
      case INTEGER: {
        return createDatum("'");
      }

      default: {
        return NULL_DATUM;
      }
    }
  }
}
