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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Unit tests of TimestreamDatabaseMetaData.
 */
class TimestreamDatabaseMetaDataTest {

  private DatabaseMetaData dbMetaData;

  @Mock
  private TimestreamConnection mockConnection;

  @Mock
  private TimestreamStatement mockStatement;

  @BeforeEach
  void init() throws SQLException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);
    dbMetaData = new TimestreamDatabaseMetaData(mockConnection);
  }

  /**
   * Checks that an empty result set is returned for getCatalogs
   */
  @Test
  void testGetCatalogsWithResult() throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
            .getCatalogs()) {
      Assertions.assertFalse(resultSet.next());
    }
  }

  /**
   * Checks that a result set containing database name "testDB" is returned for getSchemas with no parameters
   */
  @Test
  void testGetSchemasWithResult() throws SQLException {
    initializeWithResult();

    try (ResultSet resultSet = dbMetaData
            .getSchemas()) {
      testGetSchemasResult(resultSet);
    }
  }

  /**
   * Checks that a result set containing database name "testDB" is returned for getSchemas with null parameters
   */
  @Test
  void testGetSchemasNullParamWithResult() throws SQLException {
    initializeWithResult();

    try (ResultSet resultSet = dbMetaData
            .getSchemas(null, null)) {
      testGetSchemasResult(resultSet);
    }
  }

  /**
   * Checks that a result set containing database name "testDB" is returned for getSchemas with schemaPattern
   * @param schemaPattern Schema pattern to be tested
   * Input values tested for schemaPattern: {"%", "testDB", "%testDB%"}
   */
  @ParameterizedTest
  @ValueSource(strings = {"%", "testDB", "%testDB%"})
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
            .getSchemas(null, schemaPattern)) {
      testGetSchemasResult(resultSet);
    }
  }

  @Test
  void testGetColumnsWithResult() throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getColumns(null, null, null, null)) {
      testGetColumnsResult(resultSet);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%test%", "_estTabl_", "%Ta_le"})
  void testGetColumnsWithTableNamePattern(String pattern) throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getColumns(null, null, pattern, null)) {
      resultSet.next();
      Assertions.assertEquals("testTable", resultSet.getString(3));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%Col%", "_olNam_", "%N_me"})
  void testGetColumnsWithColNamePattern(String pattern) throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getColumns(null, null, null, pattern)) {
      resultSet.next();
      Assertions.assertEquals("ColName", resultSet.getString(4));
    }
  }

  @Test
  void testGetColumnsResultMetadata() throws SQLException {
    initializeWithResult();
    final TimestreamColumnsResultSet tablesResultSet = new TimestreamColumnsResultSet(
      mockConnection, null, null, null);
    final ResultSetMetaData expectation = tablesResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData
      .getColumns(null, null, null, null)) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetImportedKeysOfColInfo() throws SQLException {
    final TimestreamImportedKeysResultSet importedKeysResultSet = new TimestreamImportedKeysResultSet();
    final ResultSetMetaData expectation = importedKeysResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData
      .getImportedKeys(null, null, null)) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetIndexInfoOfColInfo() throws SQLException {
    final TimestreamIndexResultSet indexResultSet = new TimestreamIndexResultSet();
    final ResultSetMetaData expectation = indexResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData
      .getIndexInfo(null, null, null, Boolean.TRUE, Boolean.TRUE)) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetPrimaryKeysOfColInfo() throws SQLException {
    final TimestreamPrimaryKeysResultSet primaryKeysResultSet = new TimestreamPrimaryKeysResultSet();
    final ResultSetMetaData expectation = primaryKeysResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData
      .getPrimaryKeys(null, null, null)) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetTableTypesOfColInfo() throws SQLException {
    final TimestreamTableTypesResultSet tableTypesResultSet = new TimestreamTableTypesResultSet();
    final ResultSetMetaData expectation = tableTypesResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData.getTableTypes()) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetTablesWithResult() throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, null)) {
      testGetTableResult(resultSet);
    }
  }

  /**
   * Checks that empty result set is returned for empty database
   */
  @Test
  void testGetTablesWithEmptyDatabase() throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
            .getTables(null, "emptyDB", null, null)) {
      Assertions.assertFalse(resultSet.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%test%", "_estTabl_", "%Ta_le"})
  void testGetTablesWithTableNamePattern(String pattern) throws SQLException {
    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, pattern, null)) {
      resultSet.next();
      Assertions.assertEquals("testTable", resultSet.getString(3));
    }
  }

  @Test
  void testGetTablesOfColInfo() throws SQLException {
    initializeWithResult();
    final TimestreamTablesResultSet tablesResultSet = new TimestreamTablesResultSet(
      mockConnection, null, null, null);
    final ResultSetMetaData expectation = tablesResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, null)) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @Test
  void testGetTablesWithEmptyTypes() throws SQLException {
    final String[] emptyTypes = {"", ""};

    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, emptyTypes)) {
      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  void testGetTablesWithInvalidTypes() throws SQLException {
    final String[] invalidTypes = {"Invalid", "data_type"};

    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, invalidTypes)) {
      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  void testGetTablesWithValidAndInvalidTypes() throws SQLException {
    final String[] validAndInvalidTypes = {"TABLE", "data_type"};

    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, validAndInvalidTypes)) {
      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  void testGetTablesWithValidType() throws SQLException {
    final String[] validTypes = {"TABLE"};

    initializeWithResult();
    try (ResultSet resultSet = dbMetaData
      .getTables(null, null, null, validTypes)) {
      testGetTableResult(resultSet);
    }
  }

  @Test
  void testGetTypeInfoOfColInfo() throws SQLException {
    final TimestreamTypeInfoResultSet typeInfoResultSet = new TimestreamTypeInfoResultSet();
    final ResultSetMetaData expectation = typeInfoResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData.getTypeInfo()) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#urlArguments")
  void testGetURL(
    final Properties input,
    final String expected) throws SQLException {
    Mockito.when(mockConnection.getConnectionProperties()).thenReturn(input);
    Assertions.assertEquals(expected, dbMetaData.getURL());
  }

  @Test
  void testGetClientInfoPropertiesOfColInfo() throws SQLException {
    final TimestreamPropertiesResultSet clientInfoResultSet = new TimestreamPropertiesResultSet();
    final ResultSetMetaData expectation = clientInfoResultSet.getMetaData();
    try (ResultSet resultSet = dbMetaData.getClientInfoProperties()) {
      final ResultSetMetaData actual = resultSet.getMetaData();
      testGetOfColInfo(expectation, actual);
    }
  }

  /**
   * Initialize the catalog metadata results.
   *
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void initializeWithResult() throws SQLException {
    final ResultSet emptyResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(emptyResultSet.next()).thenReturn(false);

    final ResultSet emptydbResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(emptydbResultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(emptydbResultSet.getString(1)).thenReturn("emptyDB");

    final ResultSet dbResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(dbResultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(dbResultSet.getString(1)).thenReturn("testDB");
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES")).thenReturn(dbResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES LIKE '%'")).thenReturn(dbResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES LIKE '%test%'")).thenReturn(dbResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES LIKE 'testDB'")).thenReturn(dbResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES LIKE '%testDB%'")).thenReturn(dbResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW DATABASES LIKE 'emptyDB'")).thenReturn(emptydbResultSet);

    final ResultSet singleTableResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(singleTableResultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(singleTableResultSet.getString(1)).thenReturn("testTable");

    final ResultSet tableResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(tableResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(tableResultSet.getString(1)).thenReturn("testTable").thenReturn("secondTable");

    Mockito.when(mockStatement.executeQuery("SHOW TABLES FROM \"testDB\""))
      .thenReturn(tableResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW TABLES FROM \"testDB\" LIKE '%test%'"))
      .thenReturn(singleTableResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW TABLES FROM \"testDB\" LIKE '_estTabl_'"))
      .thenReturn(singleTableResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW TABLES FROM \"testDB\" LIKE '%Ta_le'"))
      .thenReturn(singleTableResultSet);
    Mockito.when(mockStatement.executeQuery("SHOW TABLES FROM \"emptyDB\""))
            .thenReturn(emptyResultSet);

    final ResultSet columnsResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(columnsResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(columnsResultSet.getString(Mockito.anyInt())).thenReturn("ColName");
    Mockito.when(mockStatement.executeQuery("DESCRIBE \"testDB\".\"testTable\""))
      .thenReturn(columnsResultSet);
    Mockito.when(mockStatement.executeQuery("DESCRIBE \"testDB\".\"secondTable\""))
            .thenReturn(columnsResultSet);
  }

  /**
   * Validate column info of two resultSetMetaData.
   *
   * @param expectation Expected resultSetMetaData.
   * @param actual      Test resultSetMetaData.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void testGetOfColInfo(
    final ResultSetMetaData expectation,
    final ResultSetMetaData actual) throws SQLException {
    Assertions.assertEquals(expectation.getColumnCount(), actual.getColumnCount());
    for (int i = 1; i <= expectation.getColumnCount(); ++i) {
      Assertions.assertEquals(expectation.getColumnName(i), actual.getColumnName(i));
      Assertions.assertEquals(expectation.getColumnType(i), actual.getColumnType(i));
    }
  }

  /**
   * Validate resultSet MetaData returned from getSchemas.
   *
   * @param resultSet ResultSet need to be validated.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void testGetSchemasResult(ResultSet resultSet) throws SQLException {
    final String[] string1 = {"", "testDB", null};
    final List<String[]> strings = new ArrayList<>();
    strings.add(string1);

    int numRows = 0;
    while (resultSet.next()) {
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
        Assertions.assertEquals(strings.get(numRows)[i], resultSet.getString(i));
      }
      numRows++;
    }
    Assertions.assertEquals(1, numRows);
  }

  /**
   * Validate resultSet MetaData returned from getColumns.
   *
   * @param resultSet ResultSet need to be validated.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void testGetColumnsResult(ResultSet resultSet) throws SQLException {
    final String[] string1 = {"", null, "testDB", "testTable", "ColName", "12", "UNKNOWN",
      "2147483647", null, null, null, "1", null, null, "12", null, "2147483647", "1",
      "YES", null, null, null, null, "NO", "NO"};
    final String[] string2 = {"", null, "testDB", "testTable", "ColName", "12", "UNKNOWN",
      "2147483647", null, null, null, "1", null, null, "12", null, "2147483647", "2",
      "YES", null, null, null, null, "NO", "NO"};
    final List<String[]> strings = new ArrayList<>();
    strings.add(string1);
    strings.add(string2);

    int numRows = 0;
    while (resultSet.next()) {
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
        Assertions.assertEquals(strings.get(numRows)[i], resultSet.getString(i));
      }
      numRows++;
    }
    Assertions.assertEquals(2, numRows);
  }

  /**
   * Validate resultSet MetaData returned from getTables.
   *
   * @param resultSet ResultSet need to be validated.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void testGetTableResult(ResultSet resultSet) throws SQLException {
    final String[] string1 = {"", null, "testDB", "testTable", "TABLE", null, null, null, null,
            null, null};
    final String[] string2 = {"", null, "testDB", "secondTable", "TABLE", null, null, null, null,
            null, null};
    final List<String[]> strings = new ArrayList<>();
    strings.add(string1);
    strings.add(string2);

    int numRows = 0;
    while (resultSet.next()) {
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
        Assertions.assertEquals(strings.get(numRows)[i], resultSet.getString(i));
      }
      numRows++;
    }
    Assertions.assertEquals(2, numRows);
  }
}
