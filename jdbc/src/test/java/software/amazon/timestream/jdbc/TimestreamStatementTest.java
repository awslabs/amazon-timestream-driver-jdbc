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
import com.amazonaws.http.timers.client.ClientExecutionTimeoutException;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamquery.model.AmazonTimestreamQueryException;
import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.ConflictException;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class TimestreamStatementTest {

  private static final String INVALID_QUERY = "SELECT FROM sampleDB.IoT";
  private static final String VALID_QUERY = "SELECT truck_id FROM sampleDB.IoT";
  private static final String QUERY_ID = "queryID";

  private TimestreamStatement statement;

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
    statement = new TimestreamStatement(mockConnection);
  }

  @Test
  void testGetMaxRowsWithMaxLongTwice() throws SQLException {
    Assertions.assertEquals(0, statement.getMaxRows());

    statement.setLargeMaxRows(Long.MAX_VALUE);

    Assertions.assertNull(statement.getWarnings());
    Assertions.assertEquals(Integer.MAX_VALUE, statement.getMaxRows());
    Assertions.assertNotNull(statement.getWarnings());

    // Test for appending warning when warning list is not null.
    Assertions.assertNull(statement.getWarnings().getNextWarning());
    statement.getMaxRows();
    Assertions.assertNotNull(statement.getWarnings().getNextWarning());
  }

  @Test
  void testGetMoreResultsWithNoResultSet() throws SQLException {
    Assertions.assertFalse(statement.getMoreResults());
    Assertions.assertFalse(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
  }

  @Test
  void testGetMoreResultsWithOpenResultSet() throws SQLException {
    prepareResultSetForQuery();

    final ResultSet openResultSet = statement.executeQuery("foo");
    Assertions.assertFalse(openResultSet.isClosed());
    Assertions.assertFalse(statement.getMoreResults());
    Assertions.assertTrue(openResultSet.isClosed());
    Assertions.assertNull(statement.getResultSet());
  }

  @Test
  void testGetMoreResultsOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.getMoreResults());
  }

  @Test
  void testGetAndSetQueryTimeout() throws SQLException {
    Assertions.assertEquals(0, statement.getQueryTimeout());
    statement.setQueryTimeout(2);
    Assertions.assertEquals(2, statement.getQueryTimeout());
  }

  @Test
  void testSetQueryTimeout() throws SQLException {
    final ArgumentCaptor<ClientConfiguration> captor = ArgumentCaptor.forClass(ClientConfiguration.class);
    statement.setQueryTimeout(900);

    Mockito
      .verify(mockClientBuilder, Mockito.atLeastOnce())
      .withClientConfiguration(captor.capture());

    Assertions.assertEquals(900, statement.getQueryTimeout());
    Assertions.assertEquals(900000, captor.getValue().getClientExecutionTimeout());

    statement.setQueryTimeout(0);
    Assertions.assertEquals(0, statement.getQueryTimeout());
    Assertions.assertEquals(0, captor.getValue().getClientExecutionTimeout());
  }

  @Test
  void testGetQueryTimeoutOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.getQueryTimeout());
  }

  @Test
  void testisPoolableOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.isPoolable());
  }

  @Test
  void testSetPoolableOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.setPoolable(true));
  }

  @Test
  void testGetFetchDirectionOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.getFetchDirection());
  }

  @Test
  void testSetFetchDirection() throws SQLException {
    statement.setFetchDirection(ResultSet.FETCH_FORWARD);

    Assertions.assertThrows(SQLException.class,
      () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  void testSetFetchDirectionOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.setFetchDirection(ResultSet.FETCH_FORWARD));
  }

  @Test
  void testSetFetchSizeOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.setFetchSize(0));
  }

  @Test
  void testSetFetchSizeWithInvalidSize() {
    Assertions.assertThrows(SQLException.class, () -> statement.setFetchSize(0));
  }

  @Test
  void testSetFetchSizeWithValueGreaterThanMaxFetchSize() throws SQLException {
    Assertions.assertEquals(0, statement.getFetchSize());
    statement.setFetchSize(100000);
    Assertions.assertEquals(1000, statement.getFetchSize());
  }

  @Test
  void testGetAndSetLargeMaxRows() throws SQLException {
    Assertions.assertEquals(0, statement.getLargeMaxRows());
    statement.setLargeMaxRows(10);
    Assertions.assertEquals(10, statement.getLargeMaxRows());
  }

  @Test
  void testSetLargeMaxRowsOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.setLargeMaxRows(0));
  }

  @Test
  void testSetLargeMaxRowsWithInvalidValue() {
    Assertions.assertThrows(SQLException.class, () -> statement.setLargeMaxRows(-1));
  }

  @Test
  void testGetAndSetMaxFieldSize() throws SQLException {
    Assertions.assertEquals(0, statement.getMaxFieldSize());
    statement.setMaxFieldSize(10);
    Assertions.assertEquals(10, statement.getMaxFieldSize());
  }

  @Test
  void testSetMaxFieldSizeOnClosedStatement() throws SQLException {
    testMethodOnClosedStatement(() -> statement.setMaxFieldSize(0));
  }

  @Test
  void testSetMaxFieldSizeWithInvalidValue() {
    Assertions.assertThrows(SQLException.class, () -> statement.setMaxFieldSize(-1));
  }

  @Test
  void testCancelOnClosedStatement() throws SQLException {
    statement.close();
    Assertions.assertThrows(SQLException.class, () -> statement.cancel());
  }

  @Test
  void testCancelOnNotExecutingStatement() throws SQLException {
    statement.cancel();
    Mockito.verify(mockQueryClient, Mockito.never()).cancelQuery(Mockito.any());
  }

  @RepeatedTest(100)
  @DisplayName("Test cancelling an executing statement.")
  void testCancelOnExecutingStatement()
    throws ExecutionException, InterruptedException, SQLException {
    prepareResultSetForQuery();
    Mockito.when(mockQueryClient.cancelQuery(Mockito.any())).thenReturn(null);
    Mockito.when(mockResult.getNextToken()).thenReturn("next");

    final CompletableFuture<Void> executeThread = CompletableFuture.runAsync(() -> {
      final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> statement.executeQuery("foo"));
      TimestreamTestUtils.validateException(
        exception,
        Error.lookup(Error.QUERY_CANCELED, QUERY_ID));
    });

    final CompletableFuture<Void> cancelThread = CompletableFuture.runAsync(() -> {
      try {
        while (!statement.canCancel.get()) {
          // Busy wait until statement has made the first call to query(). This is to avoid calling
          // statement.cancel() before statement.executeQuery() has started.
          Thread.sleep(1);
        }
        statement.cancel();

        // When a query is successfully canceled, attempting to call query() will result in a
        // ConflictException.
        Mockito.when(mockQueryClient.query(Mockito.any())).thenThrow(ConflictException.class);
      } catch (SQLException | InterruptedException e) {
        Assertions.fail(e.getMessage(), e);
      }
    });

    // Make sure the two threads end without exceptions.
    cancelThread.get();
    executeThread.get();

    Mockito
      .verify(mockQueryClient, Mockito.atLeastOnce())
      .cancelQuery(Mockito.any());
    Assertions.assertNull(statement.getResultSet());
  }

  @RepeatedTest(100)
  @DisplayName("Test closing a statement while executeQuery is still running.")
  void testCloseOnExecutingStatement()
    throws ExecutionException, InterruptedException {
    prepareResultSetForQuery();
    Mockito.when(mockQueryClient.cancelQuery(Mockito.any())).thenReturn(null);
    Mockito.when(mockResult.getNextToken()).thenReturn("next");

    final CompletableFuture<Void> executeThread = CompletableFuture.runAsync(() -> {
      final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> statement.executeQuery("foo"));
      TimestreamTestUtils.validateException(
        exception,
        Error.lookup(Error.STMT_CLOSED_DURING_EXECUTE, QUERY_ID));
    });

    final CompletableFuture<Void> cancelThread = CompletableFuture.runAsync(() -> {
      try {
        while (!statement.canCancel.get()) {
          // Busy wait until statement has made the first call to query(). This is to avoid calling
          // statement.cancel() before statement.executeQuery() has started.
          Thread.sleep(1);
        }
        statement.close();
      } catch (SQLException | InterruptedException e) {
        Assertions.fail(e.getMessage(), e);
      }
    });

    // Make sure the two threads end without exceptions.
    cancelThread.get();
    executeThread.get();

    Mockito
      .verify(mockQueryClient, Mockito.atLeastOnce())
      .cancelQuery(Mockito.any());
    Assertions.assertTrue(statement.isClosed());
  }

  @RepeatedTest(100)
  @DisplayName("Test cancelling a statement where cancelQuery throws an exception.")
  void testCancelWithException() throws ExecutionException, InterruptedException {
    final AmazonTimestreamQueryException exception = new AmazonTimestreamQueryException("exception msg");
    prepareResultSetForQuery();
    Mockito.when(mockResult.getNextToken()).thenReturn("next");
    Mockito
      .doThrow(exception)
      .when(mockQueryClient)
      .cancelQuery(Mockito.any());
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);

    final CompletableFuture<Void> executeThread = CompletableFuture.runAsync(() -> {
      Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("foo"));
    });

    final CompletableFuture<Void> cancelThread = CompletableFuture.runAsync(() -> {
      try {
        while (!statement.canCancel.get()) {
          // Busy wait until statement has made the first call to query(). This is to avoid calling
          // statement.cancel() before statement.executeQuery() has started.
          Thread.sleep(1);
        }

        statement.cancel();
        TimestreamTestUtils.validateWarning(
          statement.getWarnings(),
          Warning.lookup(
            Warning.ERROR_CANCELING_QUERY,
            QUERY_ID,
            exception.getLocalizedMessage())
        );

        // Since we should not stub or verify a shared mock from multiple threads, instead of
        // mocking the return value of mockResult, interrupt the execution thread by closing the statement.
        statement.close();
      } catch (SQLException | InterruptedException throwables) {
        Assertions.fail(throwables.getMessage(), throwables);
      }
    });

    cancelThread.get();
    executeThread.get();
  }

  @Test
  void testCloseTwice() throws SQLException {
    Assertions.assertFalse(statement.isClosed());
    statement.close();
    Assertions.assertTrue(statement.isClosed());
    statement.close();
    Assertions.assertTrue(statement.isClosed());
  }

  @Test
  void testCloseWithResultSet() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    final ResultSet openResultSet = statement.executeQuery("foo");

    Assertions.assertFalse(statement.isClosed());
    statement.close();
    Assertions.assertTrue(statement.isClosed());
    Assertions.assertTrue(openResultSet.isClosed());
  }

  @Test
  void testCloseOnCompletionOnClosedStatement() throws SQLException {
    Assertions.assertFalse(statement.isCloseOnCompletion());
    testMethodOnClosedStatement(() -> statement.closeOnCompletion());
  }

  @Test
  void testCloseOnCompletion() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);

    Assertions.assertFalse(statement.isCloseOnCompletion());
    statement.closeOnCompletion();
    Assertions.assertTrue(statement.isCloseOnCompletion());

    Assertions.assertFalse(statement.isClosed());
    final ResultSet resultSet = statement.executeQuery("foo");
    resultSet.close();
    Assertions.assertTrue(statement.isClosed());
  }

  @Test
  void testExecuteQueryWithValidQuery() throws SQLException {
    final ArgumentCaptor<QueryRequest> requestArgumentCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(requestArgumentCaptor.capture())).thenReturn(mockResult);

    Assertions.assertNotNull(statement.executeQuery(VALID_QUERY));
    Assertions.assertNull(requestArgumentCaptor.getValue().getMaxRows());
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 1000, 5000})
  void testExecuteQueryWithFetchSize(final int fetchSize) throws SQLException {
    final ArgumentCaptor<QueryRequest> requestArgumentCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(requestArgumentCaptor.capture())).thenReturn(mockResult);

    statement.setFetchSize(fetchSize);
    statement.executeQuery(VALID_QUERY);

    final QueryRequest actualRequest = requestArgumentCaptor.getValue();
    Assertions.assertEquals(Math.min(fetchSize, 1000), actualRequest.getMaxRows());
  }

  @Test
  void testExecuteQueryTwiceWithOpenResultSet() throws SQLException {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);

    final ResultSet openResultSet = statement.executeQuery(VALID_QUERY);
    Assertions.assertFalse(openResultSet.isClosed());

    final ResultSet secondResultSet = statement.executeQuery(VALID_QUERY);
    Assertions.assertTrue(openResultSet.isClosed());
    Assertions.assertFalse(secondResultSet.isClosed());
  }

  @Test
  void testExecuteQueryWithInvalidQuery() {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any()))
      .thenThrow(AmazonTimestreamQueryException.class);
    Assertions.assertThrows(SQLException.class, () -> statement.executeQuery(INVALID_QUERY));
  }

  @Test
  void testExecuteQueryWithTimeOutException() {
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any()))
      .thenThrow(ClientExecutionTimeoutException.class);
    Assertions.assertThrows(SQLTimeoutException.class, () -> statement.executeQuery(INVALID_QUERY));
  }

  @Test
  void setQueryTimeoutWithNegativeSecond() {
    final int invalidSec = -1;
    Assertions.assertThrows(SQLException.class, () -> statement.setQueryTimeout(invalidSec));
  }

  @Test
  void setQueryTimeoutWithValidSecond() throws SQLException {
    final int validSec = 1;
    statement.setQueryTimeout(validSec);
  }

  /**
   * Test calling a method on a closed {@link TimestreamStatement}.
   *
   * @param method The method executable.
   * @throws SQLException If an error occurs while closing the {@link TimestreamStatement}.
   */
  private void testMethodOnClosedStatement(final Executable method) throws SQLException {
    statement.close();

    final Exception exception = Assertions.assertThrows(SQLException.class, method);
    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(Error.STMT_CLOSED));
  }

  /**
   * Prepares a mock result set for {@link TimestreamStatement#execute(String)}
   */
  private void prepareResultSetForQuery() {
    final ColumnInfo testColumnInfo = new ColumnInfo();
    testColumnInfo.withName("foo");
    testColumnInfo.withType(new Type().withScalarType("bar"));
    Mockito.when(mockConnection.getQueryClient()).thenReturn(mockQueryClient);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(mockResult);
    Mockito.when(mockResult.getQueryId()).thenReturn(QUERY_ID);
    Mockito.when(mockResult.getRows()).thenReturn(new ArrayList<>());
    Mockito.when(mockResult.getColumnInfo()).thenReturn(ImmutableList.of(testColumnInfo));
  }
}
