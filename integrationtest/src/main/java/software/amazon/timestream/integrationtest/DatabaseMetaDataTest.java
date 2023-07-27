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

import org.junit.jupiter.api.Assertions;
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
 */
class DatabaseMetaDataTest {
  private DatabaseMetaData metaData;
  private Connection connection;

  private final String region;
  private final String[] databases;
  private final String[] tables;

  DatabaseMetaDataTest(String r, String[] ds, String[] ts) {
    region = r;
    databases = ds;
    tables = ts;
  }

  static void setUp(String r, String[] ds, String[] ts) {
    TableManager.setRegion(r);
    TableManager.createDatabases(ds);
    TableManager.createTables(ts, ds);
  }

  static void cleanUp(String r, String[] ds, String[] ts) {
    TableManager.setRegion(r);
    TableManager.deleteTables(ts, ds);
    TableManager.deleteDatabases(ds);
  }

  void init() throws SQLException {
    final Properties p = new Properties();
    p.setProperty("Region", region);
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
  }

  void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   *
   * @throws SQLException the exception thrown
   */
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
   * Test getSchemas returns the database.
   *
   * @throws SQLException the exception thrown
   */
  public void testSchemas() throws SQLException {
    final List<String> databaseList = Arrays.asList(databases);
    final List<String> schemasList = new ArrayList<>();
    try (ResultSet schemas = metaData.getSchemas()) {
      while (schemas.next()) {
        schemasList.add(schemas.getString("TABLE_SCHEM"));
      }
    }
    Assertions.assertEquals(databaseList, schemasList);
  }

  /**
   * Test getSchemas returns databases when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  public void testGetSchemasWithSchemaPattern(final String schemaPattern) throws SQLException {
    int schemasSize = 0;
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      while (schemas.next()) {
        final String schema = schemas.getString("TABLE_SCHEM");
        final String match = Arrays
            .stream(databases)
            .filter(x -> x.equals(schema))
            .findFirst()
            .orElse(null);
        Assertions.assertNotNull(match);
        schemasSize++;
      }
      // Check the number of databases found match the actual number of databases
      Assertions.assertEquals(databases.length, schemasSize);
    }
  }

  /**
   * Retrieve all tables from database
   *
   * @param schemaPattern database pattern
   * @throws SQLException the exception thrown
   */
  private void getAllTables(String schemaPattern) throws SQLException {
    final List<String> tableList = Arrays.asList(tables);
    final List<String> returnTableList = new ArrayList<>();
    try (ResultSet tables = metaData.getTables(null, schemaPattern, null, null)) {
      while (tables.next()) {
        returnTableList.add(tables.getString("TABLE_NAME"));
      }
    }
    Assertions.assertEquals(tableList, returnTableList);
  }

  /**
   * Test getTables returns the tables from the database.
   *
   * @throws SQLException the exception thrown
   */
  public void testTables() throws SQLException {
    if (databases.length == 0) {
      getAllTables(null);
    }
    for (String database : databases) {
      getAllTables(database);
    }
  }

  /**
   * Test getTables returns tables from database when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in the database
   * @throws SQLException the exception thrown
   */
  public void testTablesWithPattern(final String tablePattern, final int index) throws SQLException {
    for (String database : databases) {
      try (ResultSet tableResultSet = metaData.getTables(null, database, tablePattern, null)) {
        Assertions.assertTrue(tableResultSet.next());
        Assertions.assertEquals(tables[index], tableResultSet.getObject("TABLE_NAME"));
      }
    }
  }

  /**
   * Test getTables returns tables from database when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in the database
   * @throws SQLException the exception thrown
   */
  public void testTablesWithPatternFromDBWithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    for (String database : databases) {
      try (ResultSet tableResultSet = metaData.getTables(null, schemaPattern, tablePattern, null)) {
        Assertions.assertTrue(tableResultSet.next());
        Assertions.assertEquals(tables[index], tableResultSet.getObject("TABLE_NAME"));
      }
    }
  }
}
