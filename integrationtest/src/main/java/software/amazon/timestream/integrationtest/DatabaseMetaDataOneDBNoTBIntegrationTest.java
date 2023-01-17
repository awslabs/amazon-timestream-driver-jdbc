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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.SQLException;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 * Test case: One database with no tables
 */
class DatabaseMetaDataOneDBNoTBIntegrationTest {
  private final DatabaseMetaDataTest dbTest = new DatabaseMetaDataTest(Constants.ONE_DB_NO_TB_REGION,
    Constants.ONE_DB_NO_TB_DATABASE_NAME,
    Constants.ONE_DB_NO_TB_TABLE_NAMES);

  @BeforeAll
  private static void setUp() {
    DatabaseMetaDataTest.setUp(
      Constants.ONE_DB_NO_TB_REGION,
      Constants.ONE_DB_NO_TB_DATABASE_NAME,
      Constants.ONE_DB_NO_TB_TABLE_NAMES);
  }

  @AfterAll
  private static void cleanUp() {
    DatabaseMetaDataTest.cleanUp(
      Constants.ONE_DB_NO_TB_REGION,
      Constants.ONE_DB_NO_TB_DATABASE_NAME,
      Constants.ONE_DB_NO_TB_TABLE_NAMES);
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
   * Test getSchemas returns the database.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test retrieving the database.")
  void testSchemas() throws SQLException {
    dbTest.testSchemas();
  }

  /**
   * Test getSchemas returns database "EmptyDb_1_2.34" when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @ValueSource(strings = {"Empty_b!_1!_2.34' escape '!", "EmptyDb%", "%___2.34"})
  @DisplayName("Test retrieving database name EmptyDb_1_2.34 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    dbTest.testGetSchemasWithSchemaPattern(schemaPattern);
  }

  /**
   * Test getTables returns no tables from empty database.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getTables returns no tables from empty database \"EmptyDb_1_2.34\"")
  void testTables() throws SQLException {
    dbTest.testTables();
  }
}
