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

import com.amazonaws.ClientConfiguration;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Test subclass for TimestreamConnection that overrides the creation of {@link
 * TimestreamConnection} to avoid creating a real connection.
 */
class MockTimestreamDriver extends TimestreamDriver {

  @Override
  protected MockTimestreamConnection createTimestreamConnection(final Properties properties,
      final ClientConfiguration configuration) throws SQLException {
    return new MockTimestreamConnection(properties, null, null, null);
  }
}
