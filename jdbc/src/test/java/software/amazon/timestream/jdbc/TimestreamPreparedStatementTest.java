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
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamquery.model.AmazonTimestreamQueryException;
import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

class TimestreamPreparedStatementTest {
  private static final String INVALID_QUERY = "SELECT FROM sampleDB.IoT";
  private static final String VALID_QUERY = "SELECT truck_id FROM sampleDB.IoT";
  private TimestreamPreparedStatement statement;

  @Mock
  private TimestreamConnection mockConnection;
  @Mock
  private AmazonTimestreamQuery mockQueryClient;
  @Mock
  private AmazonTimestreamQueryClientBuilder mockClientBuilder;
  @Mock
  private QueryResult mockResult;

  @BeforeEach
  void init() throws SQLException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(mockConnection.getQueryClientBuilder()).thenReturn(mockClientBuilder);
    Mockito.when(mockClientBuilder.getClientConfiguration()).thenReturn(new ClientConfiguration());
    Mockito.when(mockClientBuilder.withClientConfiguration(Mockito.any()))
        .thenReturn(mockClientBuilder);
    Mockito.when(mockClientBuilder.withClientConfiguration(Mockito.any()).build())
        .thenReturn(mockQueryClient);
    statement = new TimestreamPreparedStatement(mockConnection, VALID_QUERY);
  }

  @Test
  void testExecuteQueryWithInvalidQuery() throws SQLException {
    statement = new TimestreamPreparedStatement(mockConnection, INVALID_QUERY);
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any()))
        .thenThrow(AmazonTimestreamQueryException.class);
    Assertions.assertThrows(SQLException.class, () -> statement.executeQuery());
  }

  @Test
  void testExecuteQueryWithValidQuery() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    Assertions.assertFalse(statement.executeQuery().isClosed());
  }

  @Test
  void testExecuteQueryWithOpenedResultSet() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);

    final ResultSet openResultSet = statement.executeQuery();
    Assertions.assertFalse(openResultSet.isClosed());

    statement.executeQuery();
    Assertions.assertTrue(openResultSet.isClosed());
  }

  @Test
  void testExecuteWithValidQuery() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    Assertions.assertTrue(statement.execute());
  }

  @Test
  void testExecuteWithInvalidQuery() throws SQLException {
    statement = new TimestreamPreparedStatement(mockConnection, INVALID_QUERY);
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any()))
        .thenThrow(AmazonTimestreamQueryException.class);
    Assertions.assertThrows(SQLException.class, () -> statement.execute());
  }

  @Test
  void testGetMetaDataWithInvalidQueryEnabledPreparedStatement() throws SQLException {
    statement = new TimestreamPreparedStatement(mockConnection, INVALID_QUERY);
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockConnection.isMetadataPreparedStatementEnabled()).thenReturn(true);
    Mockito.when(mockQueryClient.query(Mockito.any()))
        .thenThrow(AmazonTimestreamQueryException.class);
    Assertions.assertNull(statement.getMetaData());
  }

  @Test
  void testGetMetaDataWithValidQueryEnabledPreparedStatement() throws SQLException {
    final ImmutableList<ColumnInfo> columnInfos = ImmutableList
        .of(new ColumnInfo().withName("Double").withType(new Type().withScalarType("Double")));
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockConnection.isMetadataPreparedStatementEnabled()).thenReturn(true);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    Mockito.when(mockResult.getColumnInfo()).thenReturn(columnInfos);
    final ResultSetMetaData rsMetaData = statement.getMetaData();

    Assertions.assertEquals("Double", rsMetaData.getColumnName(1));
    Assertions.assertEquals("Double", rsMetaData.getColumnLabel(1));
  }

  @Test
  void testGetMetaDataWithValidQueryNotEnabledPreparedStatement() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    Assertions.assertNull(statement.getMetaData());
  }

  @Test
  void testSetterException() {
    Assertions.assertThrows(SQLException.class, () -> statement.setInt(1, 1));
    Assertions.assertThrows(SQLException.class, () -> statement.setString(1, "1"));
  }
}

