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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * ResultSet for returning the list of tables in Timestream.
 */
public class TimestreamTablesResultSet extends TimestreamBaseResultSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamTablesResultSet.class);
  private static final Datum NULL_DATUM = new Datum().withNullValue(Boolean.TRUE);
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_TYPE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "REMARKS"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_CAT"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_SCHEM"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TYPE_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "SELF_REFERENCING_COL_NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "REF_GENERATION"));

  private final TimestreamConnection connection;
  private final String namePattern;
  private final Iterator<String> databaseItr;
  private boolean isAfterLast = false;

  /**
   * Constructor.
   *
   * @param connection       the parent connection of the result set.
   * @param schemaPattern    the schema pattern to use to match database names.
   * @param tableNamePattern the regex pattern to use to match table names.
   * @param types            the types that should be returned.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamTablesResultSet(TimestreamConnection connection, String schemaPattern,
    String tableNamePattern,
    String[] types) throws SQLException {
    super(null, 20);
    this.connection = connection;
    this.rsMeta = createColumnMetadata(COLUMNS);
    this.namePattern = tableNamePattern;

    if ((null == types) || ((1 == types.length) && (Constants.TABLE_TYPE.equals(types[0])))) {
      this.databaseItr = getDatabases(schemaPattern);
      doNextPage();
    } else {
      // There's currently only a single type of table, if the specified type doesn't match there are now rows,
      // or there are no databases that can be used.
      LOGGER.debug("No databases and tables available.");
      databaseItr = Collections.emptyIterator();
      this.rowItr = Collections.emptyIterator();
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return isAfterLast;
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    LOGGER.debug("Checking whether the last row of this TimestreamTablesResultSet has been reached.");
    return !isAfterLast && !databaseItr.hasNext() && !rowItr.hasNext();
  }

  /**
   * Close this result set.
   */
  @Override
  protected void doClose() {
    LOGGER.debug("Closed is called on this TimestreamTablesResultSet, do nothing as the result set has already been closed.");
    // Do nothing.
  }

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   * @throws SQLException if there is an error retrieving the next page.
   */
  @Override
  protected boolean doNextPage() throws SQLException {
    if (!databaseItr.hasNext()) {
      LOGGER.debug("No more databases to retrieve tables from.");
      isAfterLast = true;
      return false;
    }

    LOGGER.debug("Retrieve more tables from another database.");
    populateCurrentRows();
    return true;
  }

  /**
   * Retrieve the databases in the Timestream instance to retrieve tables for.
   *
   * @param schemaPattern the schema pattern to use to match database names, maybe null.
   * @return the databases in the Timestream instance to list tables for.
   * @throws SQLException if there is an error reading from Timestream.
   */
  private Iterator<String> getDatabases(String schemaPattern) throws SQLException {
    try (ResultSet rs = new TimestreamSchemasResultSet(connection, schemaPattern)) {
      final List<String> databases = new ArrayList<>();
      while (rs.next()) {
        databases.add(rs.getString(1));
      }
      return databases.iterator();
    }
  }

  /**
   * Map the list of Tables into a Timestream Row type to allow reuse of the common ResultSet
   * retrieval path.
   *
   * @throws SQLException if there is an error retrieving the next set of tables.
   */
  private void populateCurrentRows() throws SQLException {
    final List<Row> tables = new ArrayList<>();

    try (Statement statement = connection.createStatement()) {
      final String database = databaseItr.next();
      final String query = "SHOW TABLES FROM \"" + database + "\"" +
        (Strings.isNullOrEmpty(namePattern) ? "" : " LIKE '" + namePattern + "'");
      LOGGER.debug("Retrieving tables using query: \"{}\"", query);
      try (ResultSet rs = statement.executeQuery(query)) {
        while (rs.next()) {
          tables.add(new Row().withData(
            NULL_DATUM,
            new Datum().withScalarValue(database),
            new Datum().withScalarValue(rs.getString(1)),
            new Datum().withScalarValue(Constants.TABLE_TYPE),
            NULL_DATUM,
            NULL_DATUM,
            NULL_DATUM,
            NULL_DATUM,
            NULL_DATUM,
            NULL_DATUM));
        }
      }
    }

    this.rowItr = tables.iterator();
  }
}
