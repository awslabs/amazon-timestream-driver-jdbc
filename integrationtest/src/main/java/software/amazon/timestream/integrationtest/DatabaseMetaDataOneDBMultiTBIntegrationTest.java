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
 * Test case: One database with multiple tables
 */
class DatabaseMetaDataOneDBMultiTBIntegrationTest {
  private final DatabaseMetaDataTest dbTest = new DatabaseMetaDataTest(Constants.ONE_DB_MUTLI_TB_REGION,
    Constants.ONE_DB_MUTLI_TB_DATABASES_NAME,
    Constants.ONE_DB_MUTLI_TB_TABLE_NAMES);

  @BeforeAll
  private static void setUp() {
    DatabaseMetaDataTest.setUp(
      Constants.ONE_DB_MUTLI_TB_REGION,
      Constants.ONE_DB_MUTLI_TB_DATABASES_NAME,
      Constants.ONE_DB_MUTLI_TB_TABLE_NAMES);
  }

  @AfterAll
  private static void cleanUp() {
    DatabaseMetaDataTest.cleanUp(
      Constants.ONE_DB_MUTLI_TB_REGION,
      Constants.ONE_DB_MUTLI_TB_DATABASES_NAME,
      Constants.ONE_DB_MUTLI_TB_TABLE_NAMES);
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
   * Test getSchemas returns database "JDBC_Inte.gration_Te.st_DB_01" when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @ValueSource(strings = {"%_01", "%_Inte.gration%", "%Te.st_DB%"})
  @DisplayName("Test retrieving database name JDBC_Inte.gration_Te.st_DB_01 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    dbTest.testGetSchemasWithSchemaPattern(schemaPattern);
  }

  /**
   * Test getTables returns all tables from database "JDBC_Inte.gration_Te.st_DB_01".
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getTables returns all tables from database \"JDBC_Inte.gration_Te.st_DB_01\"")
  void testTables() throws SQLException {
    dbTest.testTables();
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in Constants.ONE_DB_MUTLI_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%tion_Tes_t%, %_01, 0",
    "_nte.grat_%, %_Inte.gration%, 0",
    "%_Tes_t_Tab_le_%, %Te.st_DB%, 0",
    "%g.ration_Te_st%, %_01, 1",
    "%_nteg.rat_%, %_Inte.gration%, 1",
    "%_Te_st_T_able_0_, %Te.st_DB%, 1",
    "%tion_Test%, %_01, 2",
    "_ntegr.at_%, %_Inte.gration%, 2",
    "%_Test_Ta_ble_02, %Te.st_DB%, 2"
  })
  @DisplayName("Test retrieving Inte.gration_Tes_t_Tab_le_03, Integ.ration_Te_st_T_able_01, Integr.ation_Test_Ta_ble_02 from JDBC_Inte.gration_Te.st_DB_01.")
  void testTablesWithPatternFromDBWithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    dbTest.testTablesWithPatternFromDBWithPattern(tablePattern, schemaPattern, index);
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in Constants.ONE_DB_MUTLI_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%tion_Tes_t%, 0",
    "_nte.grat_%, 0",
    "%_Tes_t_Tab_le_%, 0",
    "%g.ration_Te_st%, 1",
    "%_nteg.rat_%, 1",
    "%_Te_st_T_able_0_, 1",
    "%tion_Test%, 2",
    "_ntegr.at_%, 2",
    "%_Test_Ta_ble_02, 2"
  })
  @DisplayName("Test retrieving Inte.gration_Tes_t_Tab_le_03, Integ.ration_Te_st_T_able_01, Integr.ation_Test_Ta_ble_02 from JDBC_Inte.gration_Te.st_DB_01.")
  void testTablesWithPattern(final String tablePattern, final int index) throws SQLException {
    dbTest.testTablesWithPattern(tablePattern, index);
  }
}
