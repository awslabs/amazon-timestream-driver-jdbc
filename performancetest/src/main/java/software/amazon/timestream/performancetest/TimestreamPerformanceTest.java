/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.timestream.performancetest;

import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Properties;

/**
 * Tests comparing the driver's performance against the SDK's performance.
 * <p>
 * Queries used in these performance tests are time sensitive. Please adjust {@link
 * Constants#START_TIME_60H} and {@link Constants#END_TIME} before running the tests.
 */
public class TimestreamPerformanceTest {
  private static final String SMALL_RESULT_SET_FLAT_MODEL_QUERY = String
    .format("SELECT BIN(time, 1m) AS time_bin, AVG(measure_value::double) AS avg_cpu%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "AND measure_name = 'cpu_user'%n"
        + "AND region = '%s' AND cell = '%s' AND silo = '%s'%n"
        + "AND availability_zone = '%s' AND microservice_name = '%s'%n"
        + "AND instance_type = '%s' AND os_version = '%s' AND instance_name = '%s'%n"
        + "GROUP BY BIN(time, 1m)%n"
        + "ORDER BY time_bin desc",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      Constants.START_TIME_60H,
      Constants.END_TIME,
      Constants.REGION,
      Constants.CELL,
      Constants.SILO,
      Constants.AVAILABILITY_ZONE1,
      Constants.MICROSERVICE_NAME,
      Constants.INSTANCE_TYPE,
      Constants.OS_VERSION,
      Constants.INSTANCE_NAME0);

  private static final String SMALL_RESULT_SET_TIME_SERIES_QUERY = String
    .format("WITH gc_timeseries AS (%n"
        + "SELECT region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_name, process_name, jdk_version,%n"
        + "CREATE_TIME_SERIES(time, measure_value::double) AS gc_reclaimed,%n"
        + "MIN(time) AS min_time, MAX(time) AS max_time%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "AND measure_name = 'gc_reclaimed'%n"
        + "AND region = '%s' AND cell = '%s' AND silo = '%s'%n"
        + "AND availability_zone = '%s' AND microservice_name = '%s'%n"
        + "AND instance_name = '%s' AND process_name = '%s' AND jdk_version = '%s'%n"
        + "GROUP BY region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_name, process_name, jdk_version%n"
        + "), interpolated_ts AS (%n"
        + "SELECT INTERPOLATE_LOCF(gc_reclaimed, SEQUENCE(min_time, max_time, 1s)) AS interpolated_gc_reclaimed%n"
        + "FROM gc_timeseries%n"
        + ")%n"
        + "SELECT * FROM interpolated_ts",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      "ago(72.5h)",
      "ago(70h)",
      Constants.REGION,
      Constants.CELL,
      Constants.SILO,
      Constants.AVAILABILITY_ZONE1,
      Constants.MICROSERVICE_NAME,
      Constants.INSTANCE_NAME0,
      Constants.PROCESS_NAME_SERVER,
      Constants.JDK_11);

  private static final String MEDIUM_RESULT_SET_FLAT_MODEL_QUERY = String
    .format("SELECT *%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "AND measure_name IN (%n"
        + "'cpu_user', 'cpu_system', 'memory_used', 'disk_io_reads', 'disk_io_writes')",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      Constants.START_TIME_60H,
      Constants.END_TIME);

  private static final String LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY = String
    .format("SELECT region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_type, os_version, instance_name, process_name, jdk_version, measure_name%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "GROUP BY region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_type, os_version, instance_name, process_name, jdk_version, measure_name",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      Constants.START_TIME_60H,
      Constants.END_TIME);

  private static final String VERY_LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY = String
    .format("SELECT *%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "ORDER BY time DESC%n"
        + "LIMIT 1010200",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      Constants.START_TIME_48H,
      Constants.END_TIME);

  private static final String MULTIPLE_TIME_SERIES_MODELS_QUERY = String
    .format("SELECT region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_type, os_version, instance_name, process_name, jdk_version, measure_name, INTERPOLATE_LOCF(CREATE_TIME_SERIES(time, measure_value::double), SEQUENCE(MIN(time), MAX(time), 1s))%n"
        + "FROM %s.%s%n"
        + "WHERE time BETWEEN %s AND %s%n"
        + "AND measure_name = 'gc_reclaimed'%n"
        + "AND region = 'us-east-1' AND cell = 'us-east-1-cell-1' AND silo = 'us-east-1-cell-1-silo-1'%n"
        + "GROUP BY region, cell, silo, availability_zone, microservice_name,%n"
        + "instance_type, os_version, instance_name, process_name, jdk_version, measure_name",
      Constants.DATABASE_NAME,
      Constants.TABLE_NAME,
      Constants.START_TIME_48H,
      Constants.END_TIME);

  @ParameterizedTest
  @ValueSource(strings = {"10", "50", "100", "500"})
  void testDriverWithSmallFlatModelResultSet(final String resultSetSize) throws SQLException {
    runTest("testDriverWithSmallResultSet", SMALL_RESULT_SET_FLAT_MODEL_QUERY + " LIMIT " + resultSetSize, ResultSet::getObject,
      102);
  }

  @ParameterizedTest
  @ValueSource(strings = {"10", "50", "100", "500"})
  void testSDKWithSmallFlatModelResultSet(final String resultSetSize) {
    runSDK("testSDKWithSmallResultSet", SMALL_RESULT_SET_FLAT_MODEL_QUERY + " LIMIT " + resultSetSize, 102);
  }

  @Test
  void testDriverWithSmallTimeSeriesResultSet() throws SQLException {
    runTest("testDriverWithSmallTimeSeriesResultSet", SMALL_RESULT_SET_TIME_SERIES_QUERY,
      ResultSet::getObject, 102);
  }

  @Test
  void testSDKWithSmallTimeSeriesResultSet() {
    runSDK("testSDKWithSmallTimeSeriesResultSet", SMALL_RESULT_SET_TIME_SERIES_QUERY, 102);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1000", "3000", "6000", "10000"})
  void testDriverWithMediumFlatModelResultSet(final String size) throws SQLException {
    runTest("testDriverWithMediumFlatModelResultSet", MEDIUM_RESULT_SET_FLAT_MODEL_QUERY + " LIMIT " + size,
      ResultSet::getObject, 102);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1000", "3000", "6000", "10000"})
  void testSDKWithMediumFlatModelResultSet(final String size) {
    runSDK("testSDKWithMediumFlatModelResultSet", MEDIUM_RESULT_SET_FLAT_MODEL_QUERY + " LIMIT " + size, 102);
  }

  @Test
  void testDriverWithLargeFlatModelResultSet() throws SQLException {
    runTest("testDriverWithLargeFlatModelResultSet", LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY,
      ResultSet::getObject, 12);
  }

  @Test
  void testSDKWithLargeFlatModelResultSet() {
    runSDK("testSDKWithLargeFlatModelResultSet", LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY, 12);
  }

  @Test
  void testDriverWithVeryLargeFlatModelResultSet() throws SQLException {
    runTest("testDriverWithVeryLargeFlatModelResultSet",
      VERY_LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY, ResultSet::getObject, 12);
  }

  @Test
  void testSDKWithVeryLargeFlatModelResultSet() {
    runSDK("testSDKWithVeryLargeFlatModelResultSet", VERY_LARGE_RESULT_SET_WITH_FLAT_MODEL_QUERY,
      12);
  }

  @Test
  void testDriverWithMultipleTimeSeriesResultSet() throws SQLException {
    runTest("testDriverWithMultipleTimeSeriesResultSet", MULTIPLE_TIME_SERIES_MODELS_QUERY,
      ResultSet::getObject, 12);
  }

  @Test
  void testSDKWithMultipleTimeSeriesResultSet() {
    runSDK("testSDKWithMultipleTimeSeriesResultSet", MULTIPLE_TIME_SERIES_MODELS_QUERY, 12);
  }

  /**
   * Execute a query and iterate through the result set using the given retrieval method.
   *
   * @param testName        Name of the performance test.
   * @param query           The SQL query to execute.
   * @param retrievalMethod The lambda specifying which method to call on the {@link ResultSet} to
   *                        get the data.
   * @param runs            The number of iterations to run.
   * @throws SQLException If an error occurred while executing queries.
   */
  static void runTest(
      final String testName,
      final String query,
      final TimestreamRetrievalMethod retrievalMethod,
      final int runs) throws SQLException {
    try (Connection connection = DriverManager.getConnection(Constants.URL_PREFIX, new Properties());
      Statement statement = connection.createStatement()) {
      final Metric retrievalMetric = new Metric();
      final Metric executionMetric = new Metric();

      for (int i = 0; i < runs; i++) {
        final long startExecuteTime = System.nanoTime();
        try (ResultSet rs = statement.executeQuery(query)) {
          executionMetric.trackExecutionTime(System.nanoTime() - startExecuteTime);
          final int columns = rs.getMetaData().getColumnCount();
          final long startRetrievalTime = System.nanoTime();
          while (rs.next()) {
            for (int j = 1; j <= columns; j++) {
              retrievalMethod.get(rs, j);
            }
          }
          retrievalMetric.trackExecutionTime(System.nanoTime() - startRetrievalTime);
          if (i == 0) {
            retrievalMetric.setNumberOfRows(rs.getRow());
          }
        }
      }

      handleMetrics(testName, new AbstractMap.SimpleEntry<>(retrievalMetric, executionMetric));
    }
  }

  /**
   * Execute a query and iterate through the result set using the SDK.
   *
   * @param testName The name of the performance test.
   * @param query    The SQL query to execute.
   * @param runs     Number of iterations to run.
   */
  static void runSDK(final String testName, final String query, final int runs) {
    final AmazonTimestreamQuery queryClient = AmazonTimestreamQueryClient.builder()
      .withRegion("us-east-1").build();
    final Metric retrievalMetric = new Metric();
    final Metric executionMetric = new Metric();

    for (int i = 0; i < runs; i++) {
      final QueryRequest queryRequest = new QueryRequest();
      queryRequest.setQueryString(query);
      final long startExecuteTime = System.nanoTime();
      QueryResult queryResult = queryClient.query(queryRequest);
      int rowsCount = queryResult.getRows().size();
      executionMetric.trackExecutionTime(System.nanoTime() - startExecuteTime);

      final long startRetrievalTime = System.nanoTime();
      while (true) {
        TimestreamSDK.parseQueryResult(queryResult);
        if (queryResult.getNextToken() == null) {
          break;
        }
        queryRequest.setNextToken(queryResult.getNextToken());
        queryResult = queryClient.query(queryRequest);
        rowsCount += queryResult.getRows().size();
      }
      retrievalMetric.trackExecutionTime(System.nanoTime() - startRetrievalTime);
      if (i == 0) {
        retrievalMetric.setNumberOfRows(rowsCount);
      }
    }

    handleMetrics(testName, new AbstractMap.SimpleEntry<>(retrievalMetric, executionMetric));
  }

  /**
   * Handle the performance test metrics.
   *
   * @param testName Name of the performance test.
   * @param metrics  The metric for executing the query and the metric for data retrieval.
   */
  static void handleMetrics(final String testName,
    final AbstractMap.SimpleEntry<Metric, Metric> metrics) {
    final Metric retrievalMetric = metrics.getKey();
    final Metric executionMetric = metrics.getValue();
    try {
      TimestreamPerformanceTestUtils.writeToCsv(executionMetric, retrievalMetric, testName);
    } catch (IOException e) {
      System.out.println("Unable to save metrics to a csv file: " + e.getMessage());
    }
  }
}
