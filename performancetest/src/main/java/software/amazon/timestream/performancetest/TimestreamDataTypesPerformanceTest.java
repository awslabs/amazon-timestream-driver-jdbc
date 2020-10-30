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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Benchmark the driver's performance retrieving different data types.
 */
public class TimestreamDataTypesPerformanceTest {
  private static final String PERFORMANCE_TEST_TABLE = "devops.host_metrics";
  private static final int RUNS = 12;

  @Test
  @DisplayName("Test array with just one value.")
  void testArray() throws SQLException {
    final String arraySQL = "SELECT ARRAY[measure_value::double] FROM " + PERFORMANCE_TEST_TABLE;
    TimestreamPerformanceTest.runTest("testSimpleArray", arraySQL,
      (resultSet, index) -> resultSet.getArray(index).getArray(), RUNS);
    TimestreamPerformanceTest.runSDK("testSimpleArrayAgainstSDK", arraySQL, RUNS);
  }

  @Test
  @DisplayName("Test a nested array with double.")
  void testNestedArray() throws SQLException {
    final String arraySQL =
      "SELECT ARRAY[ARRAY[ARRAY[ARRAY[measure_value::double, measure_value::double], ARRAY[measure_value::double, measure_value::double]]], ARRAY[ARRAY[ARRAY[measure_value::double, measure_value::double], ARRAY[measure_value::double, measure_value::double]]]] FROM "
        + PERFORMANCE_TEST_TABLE;
    TimestreamPerformanceTest.runTest("testNestedArray", arraySQL,
      (resultSet, index) -> resultSet.getArray(index).getArray(), RUNS);
    TimestreamPerformanceTest.runSDK("testNestedArrayAgainstSDK", arraySQL, RUNS);
  }

  @Test
  @DisplayName("Test array containing a row with all types of scalar values.")
  void testArrayWithRow() throws SQLException {
    final String arraySQL = String.format(
      "SELECT ARRAY[ROW(measure_value::double, INTEGER '1', BIGINT '10', measure_name, true, parse_duration('30d'), 5y, time, cast(\"time\" as time), cast(time as date), CREATE_TIME_SERIES(BIN(time, 30s), parse_duration('1d')))]%n"
        + "FROM %s%n"
        + "GROUP BY measure_value::double, measure_name, time", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testArrayWithRow", arraySQL,
      (resultSet, index) -> resultSet.getArray(index).getArray(), RUNS);
    TimestreamPerformanceTest.runSDK("testArrayWithRowAgainstSDK", arraySQL, RUNS);
  }

  @Test
  @DisplayName("Test a row containing all types of scalar values supported by Timestream.")
  void testRow() throws SQLException {
    final String rowSQL = String.format(
      "SELECT ROW(measure_value::double, INTEGER '1', BIGINT '10', measure_name, true, parse_duration('30d'), 5y, time, cast(\"time\" as time), cast(time as date), CREATE_TIME_SERIES(BIN(time, 30s), parse_duration('1d')))%n"
        + "FROM %s%n"
        + "GROUP BY measure_value::double, measure_name, time", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testRow", rowSQL, ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK("testRowAgainstSDK", rowSQL, RUNS);
  }

  @Test
  @DisplayName("Test a nested row containing all types of scalar values supported by Timestream.")
  void testNestedRow() throws SQLException {
    final String rowSQL = String.format(
      "SELECT ROW(ROW(ROW(measure_value::double, INTEGER '03', BIGINT '10', measure_name, true, parse_duration('30d'), 5y, time, cast(\"time\" as time), cast(time as date), CREATE_TIME_SERIES(BIN(time, 30s), parse_duration('1d')))), ROW(ROW(measure_value::double, INTEGER '03', BIGINT '10', measure_name, true, parse_duration('30d'), 5y, time, cast(time as date), cast(time as date), CREATE_TIME_SERIES(BIN(time, 30s), parse_duration('1d')))), ROW(ROW(measure_value::double, INTEGER '03', BIGINT '10', measure_name, true, parse_duration('30d'), 5y, time, cast(time as date), cast(time as date), CREATE_TIME_SERIES(BIN(time, 30s), parse_duration('1d')))))%n"
        + "FROM %s%n"
        + "GROUP BY measure_value::double, measure_name, time", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testNestedRow", rowSQL, ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK("testNestedRowAgainstSDK", rowSQL, RUNS);
  }

  @Test
  @DisplayName("Test retrieving VARCHAR from Timestream.")
  void testScalarVarchar() throws SQLException {
    final String sql = String.format(
      "SELECT measure_name FROM %s", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testScalarVarchar", sql, ResultSet::getString, RUNS);
    TimestreamPerformanceTest.runSDK("testScalarVarcharAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving DOUBLE from Timestream.")
  void testScalarDouble() throws SQLException {
    final String sql = String.format(
      "SELECT measure_value::double FROM %s", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testScalarDouble", sql, ResultSet::getDouble, RUNS);
    TimestreamPerformanceTest.runSDK("testScalarDoubleAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving BOOLEAN from Timestream.")
  void testScalarBoolean() throws SQLException {
    final String sql = String.format(
      "SELECT measure_value::double > 50 FROM %s", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testScalarBoolean", sql, ResultSet::getBoolean, RUNS);
    TimestreamPerformanceTest.runSDK("testScalarBooleanAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving INTEGER from Timestream.")
  void testScalarInteger() throws SQLException {
    final String sql = String.format(
      "SELECT CAST(measure_value::double AS INTEGER) FROM %s", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testScalarInteger", sql, ResultSet::getInt, RUNS);
    TimestreamPerformanceTest.runSDK("testScalarIntegerAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving BIGINT from Timestream.")
  void testScalarBigInt() throws SQLException {
    final String sql = String.format(
      "SELECT YEAR(time) FROM %s", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testScalarBigInt", sql, ResultSet::getLong, RUNS);
    TimestreamPerformanceTest.runSDK("testScalarBigIntAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving time series data from Timestream.")
  void testTimeSeriesWithScalar() throws SQLException {
    final String sql = String.format(
      "WITH binned_timeseries AS (%n"
        + "    SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization%n"
        + "    FROM %s%n"
        + "    GROUP BY BIN(time, 30s)%n"
        + "), interpolated_timeseries AS (%n"
        + "    SELECT CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization) AS ts%n"
        + "    FROM binned_timeseries%n"
        + ")%n"
        + "SELECT ts%n"
        + "FROM interpolated_timeseries", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testTimeSeriesWithScalar", sql, ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK("testTimeSeriesWithScalarAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving time series data containing an array from Timestream.")
  void testTimeSeriesWithArray() throws SQLException {
    final String sql = String.format(
      "WITH binned_timeseries AS (%n"
        + "    SELECT BIN(time, 30s) AS binned_timestamp, ARRAY[AVG(measure_value::double), AVG(measure_value::double), AVG(measure_value::double)] AS avg_cpu_utilization%n"
        + "    FROM %s%n"
        + "    GROUP BY BIN(time, 30s)%n"
        + "), interpolated_timeseries AS (%n"
        + "    SELECT CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization) AS ts%n"
        + "    FROM binned_timeseries%n"
        + ")%n"
        + "SELECT ts%n"
        + "FROM interpolated_timeseries", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testTimeSeriesWithArray", sql, ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK("testTimeSeriesWithArrayAgainstSDK", sql, RUNS);
  }

  @Test
  @DisplayName("Test retrieving time series data containing a row from Timestream.")
  void testTimeSeriesWithRow() throws SQLException {
    final String sql = String.format(
      "WITH binned_timeseries AS (%n"
        + "    SELECT BIN(time, 30s) AS binned_timestamp, ROW(0.0, 0) AS avg_cpu_utilization%n"
        + "    FROM %s%n"
        + "    GROUP BY BIN(time, 30s)%n"
        + "), interpolated_timeseries AS (%n"
        + "   SELECT CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization)%n"
        + "   FROM binned_timeseries%n"
        + ")%n"
        + "SELECT *%n"
        + "FROM interpolated_timeseries", PERFORMANCE_TEST_TABLE);
    TimestreamPerformanceTest.runTest("testTimeSeriesWithRow", sql, ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK("testTimeSeriesWithRowAgainstSDK", sql, RUNS);
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.performancetest.TimestreamPerformanceTestUtils#intervalArguments")
  @DisplayName("Test retrieving INTERVAL_YEAR_TO_MONTH and INTERVAL_DAY_TO_SECONDS from Timestream.")
  void testScalarInterval(final String testName, final String sql) throws SQLException {
    TimestreamPerformanceTest.runTest(testName, String.format(sql, PERFORMANCE_TEST_TABLE), ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK(testName + "AgainstSDK", String.format(sql, PERFORMANCE_TEST_TABLE), RUNS);
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.performancetest.TimestreamPerformanceTestUtils#dateTimeArguments")
  @DisplayName("Test retrieving DateTime data from Timestream.")
  void testScalarDateTime(final String testName, final String sql) throws SQLException {
    TimestreamPerformanceTest
      .runTest(testName, String.format(sql, PERFORMANCE_TEST_TABLE), ResultSet::getObject, RUNS);
    TimestreamPerformanceTest.runSDK(testName + "AgainstSDK", String.format(sql, PERFORMANCE_TEST_TABLE), RUNS);
  }
}
