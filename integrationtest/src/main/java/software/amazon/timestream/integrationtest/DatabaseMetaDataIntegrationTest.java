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
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 */
class DatabaseMetaDataIntegrationTest {
  private DatabaseMetaData metaData;
  private Connection connection;

  @BeforeAll
  private static void setUp() {
    TableManager.createTable();
    TableManager.writeRecords();
  }

  @AfterAll
  private static void cleanUp() {
    TableManager.deleteTable();
  }

  @BeforeEach
  private void init() throws SQLException {
    final Properties p = new Properties();
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
  }

  @AfterEach
  private void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
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
   * Test getSchemas returns the list of all databases.
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test retrieving all databases.")
  void testSchemas() throws SQLException {
    final List<String> databasesList = Arrays.asList(Constants.DATABASES_NAMES);
    final List<String> schemasList = new ArrayList<>();
    try (ResultSet schemas = metaData.getSchemas()) {
      while (schemas.next()) {
        schemasList.add(schemas.getString("TABLE_SCHEM"));
      }
    }
    Assertions.assertTrue(schemasList.containsAll(databasesList));
  }

  /**
   * Test getSchemas returns database "JDBC_Integration07_Test_DB" when given matching patterns.
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @ValueSource(strings = {"JDBC_%", "%_Integration%", "%Test_DB", "JDBC_Integration___Test_DB", "JDBC!_Integration07!_Test!_DB' escape '!"})
  @DisplayName("Test retrieving database name JDBC_Integration07_Test_DB with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      while (schemas.next()) {
        Assertions.assertEquals(Constants.DATABASE_NAME, schemas.getString("TABLE_SCHEM"));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%tion_Test%", "_ntegrat_%", "%_Test_Table_0_"})
  @DisplayName("Test retrieving Integration_Test_Table_07 from JDBC_Integration07_Test_DB.")
  void testTablesAndColumns(final String pattern) throws SQLException {
    final List<String> columnNamesList = Arrays.asList(Constants.COLUMN_NAMES);
    try (ResultSet tableResultSet = metaData.getTables(null, null, pattern, null)) {
      while (tableResultSet.next()) {
        Assertions.assertEquals(Constants.TABLE_NAME, tableResultSet.getObject("TABLE_NAME"));
      }
    }

    try (ResultSet columnsResultSet = metaData.getColumns(null, null, pattern, null)) {
      final List<String> actualColumns = new ArrayList<>();
      while (columnsResultSet.next()) {
        Assertions.assertEquals(Constants.TABLE_NAME, columnsResultSet.getObject("TABLE_NAME"));
        actualColumns.add(columnsResultSet.getObject("COLUMN_NAME").toString());
      }

      Collections.sort(actualColumns);
      Collections.sort(columnNamesList);

      Assertions.assertEquals(Constants.COLUMN_NAMES.length, actualColumns.size());
      Assertions.assertIterableEquals(columnNamesList, actualColumns);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%sure_v%", "_easure_v%", "%ea_%_v%"})
  @DisplayName("Test retrieving all measure_value::* columns from  in JDBC_Integration07_Test_DB.")
  void testColumns(final String pattern) throws SQLException {
    final List<String> measureValueColList = Arrays.asList(Constants.MEASURE_VALUE_COLUMNS);
    try (ResultSet columnsResultSet = metaData.getColumns(
      null,
      null,
      "Integration_Test_Table_07",
      pattern)) {
      final List<String> actualColumns = new ArrayList<>();
      while (columnsResultSet.next()) {
        Assertions.assertEquals(Constants.TABLE_NAME, columnsResultSet.getObject("TABLE_NAME"));
        actualColumns.add(columnsResultSet.getObject("COLUMN_NAME").toString());
      }
      Collections.sort(actualColumns);
      Collections.sort(measureValueColList);

      Assertions.assertEquals(Constants.MEASURE_VALUE_COLUMNS.length, actualColumns.size());
      Assertions.assertIterableEquals(measureValueColList, actualColumns);
    }
  }
}
