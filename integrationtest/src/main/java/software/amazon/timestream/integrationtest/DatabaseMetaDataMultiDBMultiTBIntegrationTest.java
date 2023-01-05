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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 * Test case: Multiples databases with multiple tables
 */
class DatabaseMetaDataMultiDBMultiTBIntegrationTest {
  private DatabaseMetaData metaData;
  private Connection connection;
  private List<String[]> tables;

  @BeforeAll
  private static void setUp() {
    TableManager.createDatabases(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[0]);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[1]);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[2]);
  }

  @AfterAll
  private static void cleanUp() {
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[0]);
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[1]);
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[2]);
    TableManager.deleteDatabases(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
  }

  @BeforeEach
  private void init() throws SQLException {
    final Properties p = new Properties();
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
    tables = new ArrayList<>();
    tables.add(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1);
    tables.add(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2);
    tables.add(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3);
  }

  @AfterEach
  private void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getCatalogs(). Empty result set should be returned")
  void testCatalogs() throws SQLException {
    final List<String> catalogsList = new ArrayList<>();
    try (ResultSet catalogs = metaData.getCatalogs()) {
      while (catalogs.next()) {
        catalogsList.add(catalogs.getString("TABLE_CAT"));
      }
    }
    Assertions.assertTrue(catalogsList.isEmpty());
  }

  /**
   * Test getSchemas returns the databases.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test retrieving the databases.")
  void testSchemas() throws SQLException {
    final List<String> databasesList = Arrays.asList(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
    final List<String> schemasList = new ArrayList<>();
    try (ResultSet schemas = metaData.getSchemas()) {
      while (schemas.next()) {
        schemasList.add(schemas.getString("TABLE_SCHEM"));
      }
    }
    Assertions.assertTrue(schemasList.containsAll(databasesList));
  }

  /**
   * Test getTables returns all tables.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getTables returns all tables")
  void testTables() throws SQLException {
    final List<String> allTables = new ArrayList<>();

    // Test all tables from specified database are returned
    for (int i = 0; i < tables.size(); i++) {
      final List<String> tableList = Arrays.asList(tables.get(i));
      allTables.addAll(tableList);
      final List<String> returnTableList = new ArrayList<>();
      try (ResultSet tables = metaData.getTables(null, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[i], null, null)) {
        while (tables.next()) {
          returnTableList.add(tables.getString("TABLE_NAME"));
        }
      }
      Assertions.assertTrue(returnTableList.containsAll(tableList));
    }

    // Test all tables from region are returned when no database is specified
    final List<String> returnAllTableList = new ArrayList<>();
    try (ResultSet tables = metaData.getTables(null, null, null, null)) {
      while (tables.next()) {
        returnAllTableList.add(tables.getString("TABLE_NAME"));
      }
    }
    Assertions.assertTrue(returnAllTableList.containsAll(allTables));
  }

  /**
   * Test getSchemas returns databases when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @param index         index of database name in Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%ion!_T%' escape '!, 0",
    "%DB_001, 0",
    "JDB.C%, 1",
    "%DB_002, 1",
    "JD-BC%, 2",
    "%DB!_003' escape '!, 2",
  })
  @DisplayName("Test retrieving database name JD_BC_Int.egration_Test_DB_001, JDB.C_Integration-Test_DB_002, JD-BC_Integration.Test_DB_003 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern, int index) throws SQLException {
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      Assertions.assertTrue(schemas.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[index], schemas.getString("TABLE_SCHEM"));
    }
  }

  /**
   * Test getTables returns tables from JD-BC_Integration.Test_DB_003 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param dbIndex       index of database. 0 corresponds to Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1
   * @param tbIndex       index of table name in specified database
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "%_Table_01_01, %ion!_T%' escape '!, 0, 0",
    "%-gration2_Te-st%, JD_BC_Int.%, 0, 1",
    "_nte-gration_Test_3Ta-ble_0__03, %DB_001, 0, 2",
    "%!_02!_01' escape '!, %DB_002, 1, 0",
    "%gration.-Te-st%, %ion-T%, 1, 1",
    "JD-BC__ntegration-Test_Ta1ble_0__01, %DB!_003' escape '!, 2, 0",
    "%.-Te-st_T%, %ion.T%, 2, 1",
    "%3_03, JD-BC%, 2, 2",
    "%a.ble%' escape '!, %DB!_003' escape '!, 2, 3"
  })
  @DisplayName("Test retrieving tables from databases when given matching table and schema patterns.")
  void testTablesWithTBPatternDBPattern(final String tablePattern, final String schemaPattern, final int dbIndex, final int tbIndex) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, schemaPattern, tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(tables.get(dbIndex)[tbIndex], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from databases when given matching table patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param dbIndex       index of database. 0 corresponds to Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1
   * @param tbIndex       index of table name in specified database
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
    "_nte-gration_Tes1t_Table_0__01, 0, 0",
    "%_Table_01_02%, 0, 1",
    "%-gration_Test%, 0, 2",
    "%!_02!_01' escape '!, 1, 0",
    "%_Table_02_02, 1, 1",
    "%-BC_Integration-Test_Ta1%, 2, 0",
    "%.-Te-st_T%, 2, 1",
    "%t2!_T%' escape '!, 2, 2",
    "%a.ble%' escape '!, 2, 3"
  })
  @DisplayName("Test retrieving tables from databases when given matching table patterns.")
  void testTablesWithTBPattern(final String tablePattern, final int dbIndex, final int tbIndex) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[dbIndex], tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(tables.get(dbIndex)[tbIndex], tableResultSet.getObject("TABLE_NAME"));
    }
  }
}
