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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Base ResultSet for returning an empty result with a specified set of columns.
 */
class TimestreamEmptyBaseResultSet extends TimestreamBaseResultSet {
  /**
   * Constructor.
   *
   * @param columns the columns of the empty result set.
   */
  TimestreamEmptyBaseResultSet(List<ColumnInfo> columns) {
    super(null, 20);
    this.rsMeta = createColumnMetadata(columns);
    this.rowItr = Collections.emptyIterator();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return true;
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return false;
  }

  /**
   * Close this result set.
   */
  @Override
  protected void doClose() {
    // Do nothing.
  }

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   */
  @Override
  protected boolean doNextPage() {
    return false;
  }
}
