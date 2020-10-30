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

import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.timestream.jdbc.TimestreamDriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Integration tests for query execution and different data type retrieval.
 */
class QueryExecutionIntegrationTest {
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
    connection = new TimestreamDriver().connect("jdbc:timestream", p);
  }

  @AfterEach
  private void terminate() throws SQLException {
    connection.close();
  }

  @Test
  void testExecuteQueryWithNotExistedTables() {
    final String query = String
        .format("SELECT * FROM %s.%s", Constants.DATABASE_NAME, Constants.NON_EXISTENT_TABLE_NAME);
    Assertions
        .assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(query));
  }

  @Test
  void testExecuteQueryTablesSize() throws SQLException {
    final String query = String
        .format("SELECT * FROM %s.%s", Constants.DATABASE_NAME, Constants.TABLE_NAME);

    try (ResultSet result = connection.createStatement().executeQuery(query)) {
      final ResultSetMetaData rsMeta = result.getMetaData();
      while (result.next());

      Assertions.assertEquals(Constants.TABLE_COLUMN_NUM, rsMeta.getColumnCount());
      Assertions.assertEquals(Constants.TABLE_ROW_SIZE, result.getRow());
    }
  }

  @ParameterizedTest
  @EnumSource(MeasureValueType.class)
  void testDifferentDataTypes(MeasureValueType dataType) throws SQLException {
    final String query = String
        .format("SELECT measure_value::%s FROM %s.%s WHERE measure_name='%s'",
            dataType.toString().toLowerCase(),
            Constants.DATABASE_NAME,
            Constants.TABLE_NAME,
            dataType.toString());
    try (ResultSet result = connection.createStatement().executeQuery(query)) {
      Assertions.assertTrue(result.next());
      Assertions.assertEquals(result.getString(1), Constants.DATATYPE_VALUE.get(dataType));
    }
  }

  @Test
  void testNullDataType() throws SQLException {
    final String query = String
        .format("SELECT measure_value::boolean FROM %s.%s WHERE measure_value::boolean is NULL",
            Constants.DATABASE_NAME,
            Constants.TABLE_NAME
        );
    try (ResultSet result = connection.createStatement().executeQuery(query)) {
      Assertions.assertTrue(result.next());
      Assertions.assertNull(result.getString(1));
    }
  }
}
