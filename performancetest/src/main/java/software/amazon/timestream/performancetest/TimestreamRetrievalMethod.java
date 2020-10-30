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

package software.amazon.timestream.performancetest;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Lambda expressions retrieving data from a {@link ResultSet} using a specific method.
 */
@FunctionalInterface
interface TimestreamRetrievalMethod {
  /**
   * Get the data in the {@link ResultSet}.
   *
   * @param resultSet The result set containing all the data.
   * @param index     The 1-based column index.
   * @throws SQLException if there is an error converting the data.
   */
  void get(ResultSet resultSet, int index) throws SQLException;
}
