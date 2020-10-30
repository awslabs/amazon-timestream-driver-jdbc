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

import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ResultSet for returning results of an arbitrary query against Timestream.
 */
public class TimestreamResultSet extends TimestreamBaseResultSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamResultSet.class);
  private QueryResult result;
  private final long largeMaxRows;
  private int totalRows;
  private final TimestreamResultRetriever resultRetriever;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  @VisibleForTesting
  static final QueryResult TERMINATION_MARKER = new QueryResult();

  /**
   * Constructor.
   *
   * @param statement the parent statement of the result set.
   * @param query     the query that produced this result.
   * @param result    the first chunk of the result of the issued query.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamResultSet(TimestreamStatement statement, String query, QueryResult result)
      throws SQLException {
    this(statement, query, result, new HashMap<>(), 0,  0);
  }

  /**
   * Constructor with default type map.
   *
   * @param statement    the parent statement of the result set.
   * @param query        the query that produced this result.
   * @param result       the first chunk of the result of the issued query.
   * @param map          the conversion map specifying the default conversions for types.
   * @param largeMaxRows the total number of rows that can be retrieved by this result set.
   * @param maxFieldSize The maximum number of bytes that can be returned for character and binary
   *                     column values.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamResultSet(
    final TimestreamStatement statement,
    final String query,
    final QueryResult result,
    final Map<String, Class<?>> map,
    final long largeMaxRows,
    final int maxFieldSize)
    throws SQLException {
    this(statement, query, result, map, largeMaxRows, maxFieldSize, 0, 0);
  }

  /**
   * Constructor with default type map and the time taken to retrieve the first batch of result
   * set.
   *
   * @param statement                      the parent statement of the result set.
   * @param query                          the query that produced this result.
   * @param result                         the first chunk of the result of the issued query.
   * @param map                            the conversion map specifying the default conversions for
   *                                       types.
   * @param largeMaxRows                   the total number of rows that can be retrieved by this
   *                                       result set.
   * @param maxFieldSize                   The maximum number of bytes that can be returned for
   *                                       character and binary column values.
   * @param executionTimeForFirstResultSet the time taken to retrieve the first batch of result
   *                                       set.
   * @param numPages                       the number of calls to retrieve next page of result set.
   * @throws SQLException if a database access error occurs.
   */
  TimestreamResultSet(
    final TimestreamStatement statement,
    final String query,
    final QueryResult result,
    final Map<String, Class<?>> map,
    final long largeMaxRows,
    final int maxFieldSize,
    final long executionTimeForFirstResultSet,
    final int numPages)
    throws SQLException {
    super(statement, statement.getFetchSize(), map, maxFieldSize);
    this.result = result;
    final List<Row> rows = result.getRows();
    this.rowItr = ((rows == null) ? Collections.emptyIterator() : rows.iterator());
    this.rsMeta = createColumnMetadata(result.getColumnInfo());
    this.largeMaxRows = largeMaxRows;

    final String token = result.getNextToken();
    if (token == null) {
      this.resultRetriever = new TimestreamNoOpResultRetriever();
    } else {
      this.resultRetriever = new TimestreamResultRetriever(
        this,
        this.getStatement().getClient(),
        this.getFetchSize(),
        query,
        token,
        executionTimeForFirstResultSet,
        numPages);
      executorService.execute(this.resultRetriever);
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return (null == result);
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return (null != result) && (null == result.getNextToken()) && !rowItr.hasNext();
  }

  /**
   * Close this result set.
   *
   * @throws SQLException if there is an error closing the result set.
   */
  @Override
  protected void doClose() throws SQLException {
    try {
      this.resultRetriever.interrupt();
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
      this.resultRetriever.addTerminationMarker();
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
      throw Error.createSQLException(LOGGER, e, Error.FAILED_TO_SHUTDOWN_RETRIEVAL_EXECUTOR_SERVICE);
    } finally {
      this.getStatement().childClose();
    }
  }

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   * @throws SQLException if there is an error retrieving the next page.
   */
  @Override
  protected boolean doNextPage() throws SQLException {
    if (((largeMaxRows != 0) && (totalRows >= largeMaxRows))
      || (result == null)
      || (result.getNextToken() == null)) {
      result = null;
      LOGGER.debug("Reached max rows limit or no more result sets.");
      return false;
    }

    final TimestreamResultHolder resultHolder = this.resultRetriever.getResult();
    result = resultHolder.queryResult;
    if (result == TERMINATION_MARKER) {
      LOGGER.debug("Retrieved a termination marker.");
      return false;
    }

    List<Row> rows = result.getRows();
    final int rowSize = rows.size();

    LOGGER.info(
      "QueryID: {}\nNumber of rows: {}", result.getQueryId(), rowSize);

    LOGGER.debug("Execution time to retrieve the next page: {}ms", resultHolder.executionTime);

    if (largeMaxRows != 0) {
      totalRows += rowSize;
      final long overflow = largeMaxRows - totalRows;
      if (overflow < 0) {
        // Silently truncate the extra rows.
        LOGGER.debug(
          "Total number of rows retrieved has exceeded max rows limit of {}, truncating the extra {} rows.",
          largeMaxRows,
          Math.abs(overflow));
        rows = rows.subList(0, rowSize - (int) Math.abs(overflow));
      }
    }
    rowItr = rows.iterator();
    return true;
  }

  /**
   * Check the current buffer size. Used in tests.
   *
   * @return the buffer size.
   */
  synchronized int getBufferSize() {
    return this.resultRetriever.getBufferSize();
  }

  /**
   * Checks whether the executor service has successfully terminated. Used in tests.
   * @return {@code true} if the executor service has terminated; {@code false} otherwise.
   */
  boolean isTerminated() {
    return executorService.isTerminated();
  }

  /**
   * A {@link QueryResult} holder for the producer thread that asynchronously retrieves more pages
   * of result set from Timestream. If the retrieval was successful, the class contains a page of
   * result set from Timestream, and the time taken to retrieve this result set. If the retrieval
   * resulted in an exception, this class holds the exception thrown by Timestream.
   */
  private static class TimestreamResultHolder {
    final QueryResult queryResult;
    final long executionTime;
    final SQLException exception;

    TimestreamResultHolder(QueryResult queryResult, long executionTime, SQLException exception) {
      this.queryResult = queryResult;
      this.executionTime = executionTime;
      this.exception = exception;
    }
  }

  /**
   * Class retrieving next set of result asynchronously.
   */
  private static class TimestreamResultRetriever implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamResultRetriever.class);
    private final long executionTimeForFirstResultSet;
    private final AtomicInteger numRequests = new AtomicInteger();
    private final AtomicLong totalReadingTimeMilli = new AtomicLong();
    private final BlockingQueue<TimestreamResultHolder> resultSets = new LinkedBlockingDeque<>(2);
    private final TimestreamResultSet resultSet;
    private final AmazonTimestreamQuery client;
    private final int fetchSize;
    private final String query;
    private String nextToken;
    private volatile boolean isInterrupted;

    TimestreamResultRetriever(
      final TimestreamResultSet resultSet,
      final AmazonTimestreamQuery client,
      final int fetchSize,
      final String query,
      final String nextToken,
      final long executionTimeForFirstResultSet,
      final int numPages) {
      this.executionTimeForFirstResultSet = executionTimeForFirstResultSet;
      this.resultSet = resultSet;
      this.client = client;
      this.fetchSize = fetchSize;
      this.query = query;
      this.nextToken = nextToken;
      this.numRequests.addAndGet(numPages);
    }

    @Override
    public void run() {
      final QueryRequest request = new QueryRequest().withQueryString(query);

      if (fetchSize != 0) {
        request.withMaxRows(fetchSize);
      }

      while (!isInterrupted && nextToken != null) {
        try {
          final long startExecutionTime = System.nanoTime();
          final QueryResult result = client.query(request.withNextToken(nextToken));
          final long executionTimeMilli = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startExecutionTime);
          final String queryId = result.getQueryId();
          numRequests.incrementAndGet();
          totalReadingTimeMilli.addAndGet(executionTimeMilli);
          nextToken = result.getNextToken();
          while (!resultSets.offer(
            new TimestreamResultHolder(result, executionTimeMilli, null),
            50,
            TimeUnit.MILLISECONDS)) {
            if (resultSet.isClosed()) {
              // Stop the retrieval process if the result set is closed.
              LOGGER.info(
                "Result set is closed while trying to add more result sets to the buffer.\n"
                  + "Query ID: {}\n"
                  + "Time to read results: {}ms\n"
                  + "Total execution time: {}ms\n"
                  + "Total number of pages: {}",
                queryId,
                totalReadingTimeMilli.get(),
                totalReadingTimeMilli.get() + executionTimeForFirstResultSet,
                numRequests);
              return;
            }
          }
        } catch (final Exception e) {
          resultSets.clear();
          nextToken = null;
          if (!resultSets.offer(
            new TimestreamResultHolder(
              null,
              -1,
              Error.createSQLException(LOGGER, e, Error.ASYNC_RETRIEVAL_ERROR, query)))) {
            throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.FAILED_TO_PROPAGATE_ERROR));
          }
        }
      }
      resultSet.getStatement().setResultNoMoreRows();
    }

    /**
     * Getter for the current buffer size, used in unit tests.
     *
     * @return the buffer size.
     */
    synchronized int getBufferSize() {
      return resultSets.size();
    }

    /**
     * Replaces the head of the queue with a termination marker to notify the caller to stop asking
     * for more result sets.
     *
     * @throws InterruptedException if interrupted while waiting.
     * @throws RuntimeException     if failed to add termination marker to the {@link
     *                              LinkedBlockingDeque}.
     */
    synchronized void addTerminationMarker() throws InterruptedException {
      resultSets.clear();
      LOGGER.info(
        "Terminating background thread retrieving more result sets. \n"
            + "Time to read results: {}ms\n"
            + "Total execution time: {}ms\n"
            + "Total number of pages: {}",
        totalReadingTimeMilli,
        totalReadingTimeMilli.get() + executionTimeForFirstResultSet,
        numRequests);
      if (!resultSets.offer(
        new TimestreamResultHolder(TERMINATION_MARKER, -1, null),
        50,
        TimeUnit.MILLISECONDS)) {
        throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.FAILED_TO_NOTIFY_CONSUMER_THREAD));
      }
    }

    /**
     * Get a result set from the buffer.
     *
     * @return a {@link TimestreamResultHolder}.
     * @throws SQLException if an error occurred while retrieving the result.
     */
    TimestreamResultHolder getResult() throws SQLException {
      try {
        final TimestreamResultHolder result = resultSets.take();
        if (result.exception != null) {
          throw result.exception;
        }
        return result;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Error.createSQLException(LOGGER, e, Error.FAILED_TO_BUFFER_RESULT_SET);
      }
    }

    /**
     * Interrupts the result set retrieval thread.
     */
    void interrupt() {
      isInterrupted = true;
    }
  }

  /**
   * An no-op {@link TimestreamResultRetriever} to reduce redundant null checks.
   */
  private static class TimestreamNoOpResultRetriever extends TimestreamResultRetriever {
    TimestreamNoOpResultRetriever() {
      super(null, null, 0, null, null, 0, 0);
    }

    @Override
    public void run() {
      // Do nothing.
    }

    @Override
    synchronized int getBufferSize() {
      return 0;
    }

    @Override
    synchronized void addTerminationMarker() {
      // Do nothing.
    }

    @Override
    TimestreamResultHolder getResult() {
      return new TimestreamResultHolder(TERMINATION_MARKER, -1, null);
    }

    @Override
    void interrupt() {
      // Do nothing.
    }
  }
}
