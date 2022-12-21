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
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ResultSet for returning the list of tables in Timestream.
 */
public class TimestreamColumnsResultSet extends TimestreamBaseResultSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamColumnsResultSet.class);
  private static final Datum NULL_DATUM = new Datum().withNullValue(Boolean.TRUE);
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "COLUMN_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "COLUMN_SIZE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "BUFFER_LENGTH"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "DECIMAL_DIGITS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NUM_PREC_RADIX"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "NULLABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "REMARKS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "COLUMN_DEF"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SQL_DATETIME_SUB"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "CHAR_OCTET_LENGTH"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "ORDINAL_POSITION"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "IS_NULLABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_CATALOG"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_SCHEMA"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SCOPE_TABLE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "SCOPE_DATA_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "IS_AUTOINCREMENT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "IS_GENERATEDCOLUMN"));

  /* Index of table schema value in the resultSet returned from getTables() */
  private final int TABLE_SCHEM_INDX = 2;
  /* Index of table name value in the resultSet returned from getTables() */
  private final int TABLE_NAME_INDX = 3;

  private final TimestreamStatement statement;
  private ResultSet result;
  private final TimestreamTablesResultSet tablesResult;
  private final Matcher columnNameMatcher;
  private String curDatabase;
  private String curTable;

  /**
   * Constructor.
   *
   * @param connection        the parent connection of the result set.
   * @param database          the database to search within.
   * @param tableNamePattern  the regex pattern to use to match table names.
   * @param columnNamePattern the regex pattern to use to match column names.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamColumnsResultSet(TimestreamConnection connection, String database,
    String tableNamePattern,
    String columnNamePattern) throws SQLException {
    super(null, 1000);
    this.statement = connection.createStatement();
    this.tablesResult = new TimestreamTablesResultSet(connection, database, tableNamePattern, null);
    this.rsMeta = createColumnMetadata(COLUMNS);

    // Create the matcher to be used for the column name, which will be reset each row.
    if (null == columnNamePattern) {
      columnNameMatcher = Pattern.compile(".*").matcher("");
    } else {
      columnNameMatcher = Pattern.compile(columnNamePattern).matcher("");
    }

    // This will trigger the initial retrieval without actually moving to the first row, so the subsequent
    // call to next() will work correctly.
    doNextPage();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return !rowItr.hasNext() && tablesResult.isAfterLast();
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return (null != result) && result.isLast() && !rowItr.hasNext();
  }

  /**
   * Close this result set.
   */
  @Override
  protected void doClose() throws SQLException {
    this.statement.close();
  }

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   * @throws SQLException if there is an error retrieving the next page.
   */
  @Override
  protected boolean doNextPage() throws SQLException {
    if (!tablesResult.next()) {
      result = null;
      return false;
    }

    // Get the columns for the next table.
    curDatabase = tablesResult.getString(TABLE_SCHEM_INDX);
    curTable = tablesResult.getString(TABLE_NAME_INDX);
    result = statement.executeQuery(String.format("DESCRIBE \"%s\".\"%s\"", curDatabase, curTable));

    populateCurrentRows();
    return true;
  }

  /**
   * Map the list of columns into a Timestream Row type to allow reuse of the common ResultSet
   * retrieval path.
   *
   * @throws SQLException if there is an error accessing the database.
   */
  private void populateCurrentRows() throws SQLException {
    final List<TimestreamResultSetMetaData.ColInfo> colInfo = new ArrayList<>();
    while (result.next()) {
      final String columnName = result.getString(1);
      if (!this.columnNameMatcher.reset(columnName).matches()) {
        // This column name does not pass the column pattern.
        continue;
      }

      colInfo.add(new TimestreamResultSetMetaData.ColInfo(
        new Type().withScalarType(result.getString(2)),
        columnName));
    }

    final ResultSetMetaData rsMeta = new TimestreamResultSetMetaData(colInfo);
    final int numColumns = rsMeta.getColumnCount();
    final List<Row> columns = new ArrayList<>();
    for (int i = 1; i <= numColumns; ++i) {
      final int columnType = rsMeta.getColumnType(i);
      columns.add(new Row().withData(
        NULL_DATUM,
        createDatum(curDatabase),
        createDatum(curTable),
        createDatum(rsMeta.getColumnName(i)),
        createDatum(columnType),
        createDatum(rsMeta.getColumnTypeName(i)),
        createDatum(rsMeta.getColumnDisplaySize(i)),
        NULL_DATUM,
        ((columnType == Types.TIME) || (columnType == Types.TIMESTAMP)) ? createDatum(9)
          : NULL_DATUM,
        (columnType == Types.DOUBLE) ? createDatum(10) : NULL_DATUM,
        createDatum(DatabaseMetaData.columnNullable),
        NULL_DATUM,
        NULL_DATUM,
        createDatum(columnType),
        ((columnType == Types.DATE) || (columnType == Types.TIME) || (columnType
          == Types.TIMESTAMP)) ?
          createDatum(columnType % 90) : NULL_DATUM,
        (columnType == Types.VARCHAR) ? createDatum(Integer.MAX_VALUE) : NULL_DATUM,
        createDatum(i),
        createDatum(Constants.YES_STRING),
        NULL_DATUM,
        NULL_DATUM,
        NULL_DATUM,
        NULL_DATUM,
        createDatum(Constants.NO_STRING),
        createDatum(Constants.NO_STRING)));
    }

    this.rowItr = columns.iterator();
  }
}
