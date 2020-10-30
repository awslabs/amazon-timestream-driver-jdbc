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
import com.amazonaws.services.timestreamquery.model.Row;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Result set to return all the supported connection properties in Timestream.
 */
public class TimestreamPropertiesResultSet extends TimestreamBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "NAME"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, "MAX_LEN"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "DEFAULT_VALUE"),
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "DESCRIPTION")
  );

  private final List<Row> columns = new ArrayList<>();

  /**
   * TimestreamPropertiesResultSet constructor.
   */
  public TimestreamPropertiesResultSet() {
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
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return getRow() > columns.size();
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return !rowItr.hasNext();
  }

  /**
   * Map the list of columns into a Timestream Row type to allow reuse of the common ResultSet
   * retrieval path.
   */
  private void populateCurrentRows() {
    for (final TimestreamConnectionProperty property : TimestreamConnectionProperty.values()) {
      columns.add(new Row().withData(
        createDatum(property.getConnectionProperty()),
        createDatum(0),
        createDatum(property.getDefaultValue()),
        createDatum(property.getDescription())));
    }

    this.rowItr = columns.iterator();
  }
}
