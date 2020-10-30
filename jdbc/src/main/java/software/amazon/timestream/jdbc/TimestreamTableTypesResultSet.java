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

import java.sql.SQLException;
import java.util.List;

/**
 * ResultSet for returning the table type in Timestream.
 */
public class TimestreamTableTypesResultSet extends TimestreamBaseResultSet {
  private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
    TimestreamDataType.createColumnInfo(TimestreamDataType.VARCHAR, "TABLE_TYPE"));

  public TimestreamTableTypesResultSet() {
    super(null, 1000);
    this.rsMeta = createColumnMetadata(COLUMNS);

    this.rowItr = ImmutableList
      .of(new Row().withData(new Datum().withScalarValue(Constants.TABLE_TYPE))).iterator();
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

    // This works because there's only a single result.
    return (getRow() > 1);
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();

    // This works because there's only a single result.
    return (getRow() == 1);
  }
}
