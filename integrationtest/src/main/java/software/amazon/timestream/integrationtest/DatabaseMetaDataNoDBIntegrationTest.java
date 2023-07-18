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

package software.amazon.timestream.integrationtest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.SQLException;


/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 * Test case: No database
 */
class DatabaseMetaDataNoDBIntegrationTest {
  private final DatabaseMetaDataTest dbTest = new DatabaseMetaDataTest(Constants.NO_DB_NO_TB_REGION,
    Constants.NO_DB_DATABASE_NAMES,
    Constants.NO_TB_TABLE_NAMES);

  @BeforeAll
  private static void setUp() {
    DatabaseMetaDataTest.setUp(
      Constants.NO_DB_NO_TB_REGION,
      Constants.NO_DB_DATABASE_NAMES,
      Constants.NO_TB_TABLE_NAMES);
  }

  @AfterAll
  private static void cleanUp() {
    DatabaseMetaDataTest.cleanUp(
      Constants.NO_DB_NO_TB_REGION,
      Constants.NO_DB_DATABASE_NAMES,
      Constants.NO_TB_TABLE_NAMES);
  }

  @BeforeEach
  private void init() throws SQLException {
    dbTest.init();
  }

  @AfterEach
  private void terminate() throws SQLException {
    dbTest.terminate();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getCatalogs(). Empty result set should be returned")
  void testCatalogs() throws SQLException {
    dbTest.testCatalogs();
  }

  /**
   * Test getSchemas returns empty ResultSet.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getSchemas(), no database should be returned.")
  void testSchemas() throws SQLException {
    dbTest.testSchemas();
  }

  /**
   * Test getTables returns no table.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getTables(), no table should be returned.")
  void testTables() throws SQLException {
    dbTest.testTables();
  }
}
