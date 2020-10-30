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
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * ResultSet for returning the list of databases in Timestream.
 */
public class TimestreamDatabasesResultSet extends TimestreamBaseResultSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamDatabasesResultSet.class);
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_CAT"));

  private boolean isAfterLast = false;

  /**
   * Constructor.
   *
   * @param connection the parent connection of the result set.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamDatabasesResultSet(TimestreamConnection connection) throws SQLException {
    super(null, 20);
    this.rsMeta = createColumnMetadata(COLUMNS);

    populateCurrentRows(connection);
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return isAfterLast;
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    LOGGER.debug("Checking whether the last row of this TimestreamDatabasesResultSet has been reached.");
    return !isAfterLast && !this.rowItr.hasNext();
  }

  @Override
  protected void doClose() {
    LOGGER.debug("Closed is called on this TimestreamDatabasesResultSet, do nothing as the result set has already been closed.");
  }

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   */
  @Override
  protected boolean doNextPage() {
    LOGGER.debug("Attempting to retrieve the next page of results. There are no more pages, return false.");
    this.isAfterLast = true;
    return false;
  }

  /**
   * Map the list of databases into a Timestream Row type to allow reuse of the common ResultSet
   * retrieval path.
   *
   * @param connection The parent connection to retrieve databases from.
   * @throws SQLException if there is an error listing the databases.
   */
  private void populateCurrentRows(TimestreamConnection connection) throws SQLException {
    final List<String> databases = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      LOGGER.debug("Retrieving a list of databases.");
      try (ResultSet rs = statement.executeQuery("SHOW DATABASES")) {
        while (rs.next()) {
          databases.add(rs.getString(1));
        }
      }
    }
    LOGGER.debug("Retrieved {} databases.", databases.size());
    this.rowItr = databases.stream()
      .map(d -> new Datum().withScalarValue(d))
      .map(d -> new Row().withData(d))
      .iterator();
  }
}
