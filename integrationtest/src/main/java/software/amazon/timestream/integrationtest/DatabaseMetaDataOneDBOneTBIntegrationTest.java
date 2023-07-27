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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.SQLException;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 * Test case: One databases with one table
 */
class DatabaseMetaDataOneDBOneTBIntegrationTest {
  private final DatabaseMetaDataTest dbTest = new DatabaseMetaDataTest(Constants.ONE_DB_ONE_TB_REGION,
    Constants.ONE_DB_ONE_TB_DATABASE_NAME,
    Constants.ONE_DB_ONE_TB_TABLE_NAME);

  @BeforeAll
  private static void setUp() {
    DatabaseMetaDataTest.setUp(
      Constants.ONE_DB_ONE_TB_REGION,
      Constants.ONE_DB_ONE_TB_DATABASE_NAME,
      Constants.ONE_DB_ONE_TB_TABLE_NAME);
  }

  @AfterAll
  private static void cleanUp() {
    DatabaseMetaDataTest.cleanUp(
      Constants.ONE_DB_ONE_TB_REGION,
      Constants.ONE_DB_ONE_TB_DATABASE_NAME,
      Constants.ONE_DB_ONE_TB_TABLE_NAME);
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
   * Test getSchemas returns database "JDBC_.IntegrationTestDB0088" when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @ValueSource(strings = {"%0_88", "%!_.Integration%' escape '!", "JDBC%_ntegrationTestD_00_8"})
  @DisplayName("Test retrieving database name JDBC_.IntegrationTestDB0088 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    dbTest.testGetSchemasWithSchemaPattern(schemaPattern);
  }

  /**
   * Test getTables returns the table from database "JDBC_.IntegrationTestDB0088".
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getTables returns the table from database \"JDBC_.IntegrationTestDB0088\"")
  void testTables() throws SQLException {
    dbTest.testTables();
  }

  /**
   * Test getTables returns tables from IntegrationTestTable0888 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in Constants.ONE_DB_ONE_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%Test%le08%, %0_88, 0",
    "In_egratio_TestTable_888, %!_.Integration%' escape '!, 0",
    "%8, JDBC%_ntegrationTestD_00_8, 0",
  })
  @DisplayName("Test retrieving IntegrationTestTable0888 from JDBC_.IntegrationTestDB0088.")
  void testTablesWithPatternFromDBWithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    dbTest.testTablesWithPatternFromDBWithPattern(tablePattern, schemaPattern, index);
  }

  /**
   * Test getTables returns tables from IntegrationTestTable0888 when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in Constants.ONE_DB_ONE_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%Test%le08%, 0",
    "In_egratio_TestTable_888, 0",
    "%8, 0",
  })
  @DisplayName("Test retrieving IntegrationTestTable0888 from JDBC_.IntegrationTestDB0088.")
  void testTablesWithPattern(final String tablePattern, final int index) throws SQLException {
    dbTest.testTablesWithPattern(tablePattern, index);
  }
}
