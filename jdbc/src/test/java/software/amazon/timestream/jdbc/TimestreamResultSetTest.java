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

import com.amazonaws.http.timers.client.ClientExecutionTimeoutException;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.amazonaws.services.timestreamquery.model.Type;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

class TimestreamResultSetTest {

  @FunctionalInterface
  interface ResultGetter<T> {
    T get() throws SQLException;
  }

  private static final int MOCK_FETCH_SIZE = 1;
  private static final String MOCK_COLUMN_NAME = "foo";
  private static final String INVALID_ARGUMENT = LocalTime.now().toString();

  private TimestreamResultSet resultSet;

  static {
    // Override the JVM timezone for unit tests.
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Paris"));
  }

  @Mock
  private TimestreamStatement mockStatement;

  @Mock
  private AmazonTimestreamQuery mockQueryClient;

  @Mock
  private QueryResult mockResult;

  @Mock
  private ColumnInfo mockColumnInfo;

  @Mock
  private Row mockRow;

  @Mock
  private Datum mockData;

  @BeforeEach
  void init() throws SQLException {
    MockitoAnnotations.initMocks(this);

    final List<Row> mockRowList = new ArrayList<>();
    final List<Datum> mockDataList = new ArrayList<>();

    Mockito.when(mockData.getArrayValue()).thenReturn(null);
    Mockito.when(mockData.getRowValue()).thenReturn(null);
    Mockito.when(mockData.getTimeSeriesValue()).thenReturn(null);
    mockDataList.add(mockData);
    mockRow.setData(mockDataList);
    mockRowList.add(mockRow);

    final List<ColumnInfo> columnInfoList = new ArrayList<>();
    columnInfoList.add(mockColumnInfo);

    Mockito.when(mockRow.getData()).thenReturn(mockDataList);
    Mockito.when(mockStatement.getFetchSize()).thenReturn(MOCK_FETCH_SIZE);
    Mockito.when(mockStatement.getClient()).thenReturn(mockQueryClient);
    Mockito.when(mockResult.getRows()).thenReturn(mockRowList);
    Mockito.when(mockResult.getColumnInfo()).thenReturn(columnInfoList);
  }

  /**
   * Initialize the {@link TimestreamResultSet} with the specified {@link TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType} of the data.
   * @throws SQLException if failed to instantiate the a {@link TimestreamResultSet}
   */
  private void initializeResult(TimestreamDataType type) throws SQLException {
    final Type resultType;

    switch (type) {
      case ROW: {
        resultType = TimestreamTestUtils.createRowType(type);
        break;
      }

      case ARRAY: {
        resultType = TimestreamTestUtils.createArrayType(TimestreamDataType.DOUBLE);
        break;
      }

      case TIMESERIES: {
        resultType = TimestreamTestUtils.createTimeSeriesType(type);
        break;
      }

      default: {
        resultType = TimestreamTestUtils.createScalarType(type);
        break;
      }
    }

    Mockito
      .when(mockColumnInfo.getType())
      .thenReturn(resultType);

    resultSet = new TimestreamResultSet(mockStatement, "", mockResult);
  }

  /**
   * Initialize the {@link TimestreamResultSet} with the specified {@link TimestreamDataType} and
   * the specified return value.
   *
   * @param type        The {@link TimestreamDataType} of the data.
   * @param returnValue The value to return when retrieving data from the mock result set.
   * @throws SQLException if failed to instantiate the a {@link TimestreamResultSet}
   */
  private void initializeResult(
    final TimestreamDataType type,
    final String returnValue)
    throws SQLException {
    initializeResult(type);
    Mockito.when(mockData.getScalarValue()).thenReturn(returnValue);
    resultSet.next();
  }

  @Test
  void testAbsoluteBeforeFirst() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions
      .assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.absolute(-1));
  }

  @Test
  void testAbsoluteWithInvalidIndex() throws SQLException {
    final QueryResult firstPage = new QueryResult()
      .withColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("VARCHAR")))
      .withRows(
        new Row().withData(new Datum(), new Datum()),
        new Row().withData(new Datum(), new Datum())
      );

    this.resultSet = new TimestreamResultSet(mockStatement, "", firstPage, new HashMap<>(), 1, 0);
    resultSet.next();
    resultSet.next();
    Assertions
      .assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.absolute(1));
  }

  @Test
  void testAbsoluteAfterLast() throws SQLException {
    Mockito.when(mockResult.getRows()).thenReturn(Collections.emptyList());
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertFalse(resultSet.absolute(1));
  }

  @Test
  void testCloseTwice() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.close();
    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
  }

  @Test
  void testCloseWithMoreRows() throws SQLException {
    Mockito.when(mockResult.getNextToken()).thenReturn("More result");
    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
  }

  @Test
  void testNextWithTerminationMarker() throws SQLException {
    Mockito.when(mockResult.getNextToken()).thenReturn("More result");
    Mockito.when(mockResult.getRows()).thenReturn(new ArrayList<>());

    Mockito
      .when(mockQueryClient.query(Mockito.any(QueryRequest.class)))
      .thenReturn(TimestreamResultSet.TERMINATION_MARKER);

    initializeResult(TimestreamDataType.VARCHAR);

    Assertions.assertFalse(resultSet.next());

    Mockito.verify(mockQueryClient, Mockito.atLeastOnce()).query(Mockito.any());
  }

  @Test
  void testNextThrowsException() throws SQLException {
    Mockito.when(mockResult.getNextToken()).thenReturn("More result");
    Mockito.when(mockResult.getRows()).thenReturn(new ArrayList<>());

    Mockito
      .when(mockQueryClient.query(Mockito.any(QueryRequest.class)))
      .thenThrow(ClientExecutionTimeoutException.class);

    initializeResult(TimestreamDataType.VARCHAR);

    Assertions.assertThrows(SQLException.class, () -> resultSet.next());

    Mockito.verify(mockQueryClient, Mockito.atLeastOnce()).query(Mockito.any());
  }

  @RepeatedTest(100)
  @DisplayName("Test closing a result set with a full buffer. ")
  void testCloseWithFullBuffer() throws SQLException, InterruptedException {
    Mockito.when(mockResult.getNextToken()).thenReturn("More result");

    Mockito
      .when(mockQueryClient.query(Mockito.any(QueryRequest.class)))
      .thenReturn(new QueryResult()
        .withNextToken("More result")
        .withRows(ImmutableList.of(new Row())));
    initializeResult(TimestreamDataType.VARCHAR);

    // Busy wait so producer has time to fill up the buffer.
    while (resultSet.getBufferSize() < 2) {
      Thread.sleep(50);
    }
    Assertions.assertEquals(2, resultSet.getBufferSize());

    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
    Assertions.assertEquals(1, resultSet.getBufferSize());
    Assertions.assertTrue(resultSet.isTerminated());
    Mockito.verify(mockQueryClient, Mockito.atLeast(2)).query(Mockito.any());
  }

  @Test
  void testNextOnClosedResult() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.close();

    Assertions.assertThrows(SQLException.class, () -> resultSet.next());
  }

  @Test
  void testNextWithNoResult() throws SQLException {
    final QueryResult emptyResult = new QueryResult()
      .withColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("VARCHAR")))
      .withRows(Collections.emptyList());
    resultSet = new TimestreamResultSet(mockStatement, "", emptyResult);
    Assertions.assertFalse(resultSet.next());
  }

  @Test
  void testNextWithMaxRowLimit() throws SQLException {
    final QueryResult firstPage = new QueryResult()
      .withColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("VARCHAR")))
      .withRows()
      .withNextToken("token");

    final List<Row> rows = new ArrayList<>();
    rows.add(new Row());
    rows.add(new Row());

    this.resultSet = new TimestreamResultSet(mockStatement, "", firstPage, new HashMap<>(), 1, 0);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(new QueryResult().withRows(rows).withNextToken("foo"));
    Assertions.assertTrue(this.resultSet.next());
    Assertions.assertFalse(this.resultSet.next());
  }

  @Test
  void testNextWithNoMaxRowLimit() throws SQLException {
    final QueryResult firstPage = new QueryResult()
      .withColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("VARCHAR")))
      .withRows()
      .withNextToken("token");

    final List<Row> rows = new ArrayList<>();
    rows.add(new Row());
    rows.add(new Row());

    this.resultSet = new TimestreamResultSet(mockStatement, "", firstPage, new HashMap<>(), 0, 0);
    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(new QueryResult().withRows(rows));
    Assertions.assertTrue(this.resultSet.next());
    Assertions.assertTrue(this.resultSet.next());
    Mockito.verify(mockQueryClient).query(Mockito.any());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 1000})
  void testNextWithFetchSizeLimit(final int fetchSize) throws SQLException {
    Mockito.when(mockStatement.getFetchSize()).thenReturn(fetchSize);

    final QueryResult firstPage = new QueryResult()
      .withColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("VARCHAR")))
      .withRows()
      .withNextToken("token");

    final ArgumentCaptor<QueryRequest> requestArgumentCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    Mockito.when(mockQueryClient.query(requestArgumentCaptor.capture())).thenReturn(new QueryResult().withRows(ImmutableList.of(new Row())));

    final TimestreamResultSet result = new TimestreamResultSet(mockStatement, "", firstPage, new HashMap<>(), 0, 0);
    result.next();

    final QueryRequest actualRequest = requestArgumentCaptor.getValue();
    if (fetchSize == 0) {
      Assertions.assertNull(actualRequest.getMaxRows());
    } else {
      Assertions.assertEquals(Math.min(fetchSize, 1000), actualRequest.getMaxRows());
    }
    Assertions.assertNotNull(actualRequest.getNextToken());
  }

  @Test
  void testFindColumn() throws SQLException {
    Mockito.when(mockColumnInfo.getName()).thenReturn(MOCK_COLUMN_NAME);

    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertEquals(1, resultSet.findColumn(MOCK_COLUMN_NAME));
  }

  @Test
  void testFindColumnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.TIMESTAMP);
    testMethodOnClosedResult(() -> resultSet.findColumn(MOCK_COLUMN_NAME));
  }

  @Test
  void testFindColumnWithInvalidLabel() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertThrows(SQLException.class, () -> resultSet.findColumn(MOCK_COLUMN_NAME));
  }

  @Test
  void testGetArrayFromArray() throws SQLException {
    final List<Datum> array = new ArrayList<>();
    final Datum data = new Datum().withScalarValue("3.14159");
    final Object[] expectation = new Object[] {3.14159};
    array.add(data);

    initializeResult(TimestreamDataType.ARRAY);
    resultSet.next();
    Mockito.when(mockData.getArrayValue()).thenReturn(array);
    Assertions.assertArrayEquals(expectation,
      (Object[]) resultSet.getArray(1).getArray());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timeSeriesArguments")
  void testGetArrayFromTimeSeries(
    final List<TimeSeriesDataPoint> input,
    final List<TimeSeriesDataPoint> expected) throws SQLException {
    initializeResult(TimestreamDataType.TIMESERIES);
    resultSet.next();
    Mockito.when(mockData.getTimeSeriesValue()).thenReturn(input);
    Assertions.assertArrayEquals(
      ImmutableList.of(expected).toArray(),
      (Object[]) resultSet.getArray(1).getArray());
  }

  @Test
  void testGetArrayWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.ARRAY, () -> resultSet.getArray(1), null);
  }

  @Test
  void testGetArrayOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.ARRAY);
    testMethodOnClosedResult(() -> resultSet.getArray(1));
  }

  @Test
  void testGetRowOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.ROW);
    testMethodOnClosedResult(() -> resultSet.getRow());
  }

  @Test
  void testGetTimeSeriesWithInvalidIndex() throws SQLException {
    initializeResult(TimestreamDataType.TIMESERIES);
    resultSet.next();

    Assertions.assertThrows(SQLException.class, () -> resultSet.getObject(0));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timeSeriesArguments")
  void testGetObjectWithTimeSeries(
    final List<TimeSeriesDataPoint> input,
    final List<TimeSeriesDataPoint> expected) throws SQLException {
    initializeResult(TimestreamDataType.TIMESERIES);
    resultSet.next();
    Mockito.when(mockData.getTimeSeriesValue()).thenReturn(input);
    Assertions.assertEquals(expected, resultSet.getObject(1));
  }

  @Test
  void testGetObjectWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.TIMESERIES, () -> resultSet.getObject(1), null);
  }

  @Test
  void testGetObjectWithInvalidIndex() throws SQLException {
    initializeResult(TimestreamDataType.ROW);
    resultSet.next();

    Assertions.assertThrows(SQLException.class, () -> resultSet.getObject(2));
  }

  @Test
  void testGetObjectOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.TIMESERIES);
    testMethodOnClosedResult(() -> resultSet.getObject(1));
  }

  @Test
  void testGetObjectWithEmptyConversionMap() throws SQLException {
    final Time expectation = Time.valueOf("00:40:14");
    final Map<String, Class<?>> map = new HashMap<>();
    Mockito.when(mockData.getScalarValue()).thenReturn("00:40:14.706000000");
    initializeResult(TimestreamDataType.TIME);
    resultSet.next();

    Assertions.assertEquals(expectation, resultSet.getObject(1, map));
  }

  @Test
  void testGetObjectWithUnsupportedType() throws SQLException {
    Mockito.when(mockData.getRowValue()).thenReturn(null);
    initializeResult(TimestreamDataType.ROW);
    resultSet.next();

    // Float is not a supported Java class that can be mapped to a Timestream supported data type.
    Assertions.assertThrows(SQLException.class, () -> resultSet.getObject(1, Float.class));
  }

  @Test
  void testGetObjectWithConversionMapOfScalarValues() throws SQLException {
    final Timestamp expectation = Timestamp.valueOf("1970-01-01 00:40:14.706000000");
    final Map<String, Class<?>> map = new HashMap<>();
    map.put(JdbcType.TIME.name(), Timestamp.class);
    Mockito.when(mockData.getScalarValue()).thenReturn("00:40:14.706000000");
    initializeResult(TimestreamDataType.TIME);
    resultSet.next();

    Assertions.assertEquals(expectation, resultSet.getObject(1, map));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#objectMapOnArrayArguments")
  void testGetObjectWithConversionMapOnArray(
    final List<Object> input,
    final Object expected,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    initializeResult(TimestreamDataType.ARRAY);

    Mockito
      .when(mockData.getArrayValue())
      .thenReturn(TimestreamTestUtils.createInputDatumList(input));

    resultSet.next();

    final Object result = resultSet.getObject(1, conversionMap);

    if (result instanceof Array) {
      Assertions.assertArrayEquals((Object[]) expected, (Object[]) ((Array) result).getArray());
    } else {
      Assertions.assertEquals(expected, result);
    }
  }

  @Test
  void testGetObjectOnEmptyRow() throws SQLException {
    Mockito.when(mockColumnInfo.getType())
      .thenReturn(new Type().withRowColumnInfo(new ColumnInfo()));
    Mockito.when(mockData.getNullValue()).thenReturn(true);
    resultSet = new TimestreamResultSet(mockStatement, "", mockResult);
    resultSet.next();

    Assertions.assertNull(resultSet.getObject(1));
  }

  @Test
  void testGetObjectWithoutMapOnRow() throws SQLException {
    initializeResultSetWithNestedRow("2", TimestreamDataType.INTEGER);
    resultSet.next();

    final Object[] expectation = new Object[]{2};
    final Object resultWithIntMap = resultSet.getObject(1);

    final Object externalStruct = ((Struct) resultWithIntMap).getAttributes()[0];
    Assertions.assertTrue(externalStruct instanceof TimestreamStruct);
    Assertions
      .assertArrayEquals(expectation, ((Struct) externalStruct).getAttributes());
  }

  @Test
  void testGetObjectWithRowConversionMapOnRow() throws SQLException {
    final Map<String, Class<?>> map = new ImmutableMap.Builder<String, Class<?>>()
      .put(TimestreamDataType.ROW.name(), String.class)
      .build();
    initializeResultSetWithNestedRow("2", TimestreamDataType.INTEGER);
    resultSet.next();

    final Object resultWithRowMap = resultSet.getObject(1, map);
    Assertions.assertEquals("(2)", ((Struct) resultWithRowMap).getAttributes()[0]);
  }

  @Test
  void testGetObjectWithIntConversionMapOnRow() throws SQLException {
    final Map<String, Class<?>> map = new ImmutableMap.Builder<String, Class<?>>()
      .put(TimestreamDataType.INTEGER.name(), Double.class)
      .build();
    initializeResultSetWithNestedRow("2", TimestreamDataType.INTEGER);
    resultSet.next();

    final Object[] expectation = new Object[]{2.0};
    final Object resultWithIntMap = resultSet.getObject(1, map);
    final Object externalStruct = ((Struct) resultWithIntMap).getAttributes()[0];

    Assertions.assertTrue(externalStruct instanceof TimestreamStruct);
    Assertions
      .assertArrayEquals(expectation, ((Struct) externalStruct).getAttributes());
  }

  @Test
  void testGetObjectWithConversionMapOnClosedResultSet() throws SQLException {
    final Map<String, Class<?>> map = new HashMap<>();
    initializeResult(TimestreamDataType.TIMESERIES);
    testMethodOnClosedResult(() -> resultSet.getObject(1, map));
  }

  @Test
  void testGetJavaObjectFromArray() throws SQLException {
    final List<Datum> array = new ArrayList<>();
    final Datum data = new Datum().withScalarValue("3.14159");
    array.add(data);

    initializeResult(TimestreamDataType.ARRAY);
    resultSet.next();
    Mockito.when(mockData.getArrayValue()).thenReturn(array);
    Assertions.assertEquals(array, resultSet.getObject(1, List.class));
  }

  @Test
  void testGetJavaObjectFromRow() throws SQLException {
    final Row row = new Row();
    final Row rowWithDouble = new Row().withData(new Datum().withScalarValue("3.14159"));
    final Row rowWithDoubleAndInt = new Row()
      .withData(new Datum().withScalarValue("3.14159"), new Datum().withScalarValue("1"));

    initializeResult(TimestreamDataType.ROW);
    resultSet.next();

    Mockito.when(mockData.getRowValue()).thenReturn(row);
    Assertions.assertEquals(ImmutableList.of(row),
      resultSet.getObject(1, List.class));
    Mockito.when(mockData.getRowValue()).thenReturn(rowWithDouble);
    Assertions.assertEquals(ImmutableList.of(rowWithDouble),
      resultSet.getObject(1, List.class));
    Mockito.when(mockData.getRowValue()).thenReturn(rowWithDoubleAndInt);
    Assertions.assertEquals(ImmutableList.of(rowWithDoubleAndInt),
      resultSet.getObject(1, List.class));
  }

  @Test
  void testGetAsciiStreamWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.DOUBLE, () -> resultSet.getAsciiStream(1), null);
  }

  @Test
  void testGetAsciiStreamWithString() throws SQLException, IOException {
    final String stringWithUTF = "3.14159@Œ§";
    final ByteArrayInputStream expectation = new ByteArrayInputStream(stringWithUTF.getBytes(
      StandardCharsets.US_ASCII));

    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.next();
    Mockito.when(mockData.getScalarValue()).thenReturn(stringWithUTF);
    Assertions.assertEquals(IOUtils.toString(expectation),
      IOUtils.toString(resultSet.getAsciiStream(1)));
  }

  @Test
  void testGetAsciiStreamOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    testMethodOnClosedResult(() -> resultSet.getAsciiStream(1));
  }

  @Test
  void testGetCharacterStreamWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.VARCHAR, () -> resultSet.getCharacterStream(1), null);
  }

  @Test
  void testGetCharacterStreamWithString() throws SQLException, IOException {
    final String stringWithUTF = "3.14159@Œ§";

    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.next();
    Mockito.when(mockData.getScalarValue()).thenReturn(stringWithUTF);
    Assertions
      .assertEquals(stringWithUTF, CharStreams.toString(resultSet.getCharacterStream(1)));
  }

  @Test
  void testGetCharacterStreamOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    testMethodOnClosedResult(() -> resultSet.getCharacterStream(1));
  }

  @Test
  void testGetUnicodeStreamWithString() throws SQLException, IOException {
    final String piDouble = "3.14159";

    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.next();
    Mockito.when(mockData.getScalarValue()).thenReturn(piDouble);
    Assertions.assertEquals(piDouble, IOUtils.toString(resultSet.getUnicodeStream(1)));
  }

  @Test
  void testGetUnicodeStreamOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    testMethodOnClosedResult(() -> resultSet.getUnicodeStream(1));
  }

  @Test
  void testRelativeWithNegativeArgument() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    Assertions.assertThrows(SQLException.class, () -> resultSet.relative(-1));
  }

  @Test
  void testRelativeWithPositiveArgument() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    Assertions.assertTrue(resultSet.relative(1));
  }

  @Test
  void testRelativeWithPositiveArgumentWithNoMoreRows() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    resultSet.next();
    Assertions.assertFalse(resultSet.relative(1));
  }

  @Test
  void testGetBigDecimalWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    testGetWithInvalidArgument(
      () -> resultSet.getBigDecimal(1),
      INVALID_ARGUMENT,
      TimestreamDataType.DOUBLE);
  }

  @Test
  void testGetBigDecimalWithScale() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    final BigDecimal bigDecimal = new BigDecimal("3.14").setScale(1, RoundingMode.HALF_UP);
    Assertions.assertEquals(bigDecimal, resultSet.getBigDecimal(1, 1));
  }

  @Test
  void testGetBigDecimalFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(1));
  }

  @Test
  void testGetBigDecimalFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));
  }

  @Test
  void testGetBigDecimalFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "3000000");
    Assertions.assertEquals(new BigDecimal("3000000"), resultSet.getBigDecimal(1));
  }

  @Test
  void testGetBigDecimalFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(new BigDecimal(0), resultSet.getBigDecimal(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(new BigDecimal(1), resultSet.getBigDecimal(1));
  }

  @Test
  void testGetBigDecimalWithScaleAndNullData() throws SQLException {
    Mockito.when(mockData.getNullValue()).thenReturn(Boolean.TRUE);
    initializeResult(TimestreamDataType.DOUBLE);
    resultSet.next();

    Assertions.assertNull(resultSet.getBigDecimal(1, 2));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  void testGetBigDecimalWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.DOUBLE, () -> resultSet.getBigDecimal(1), null);
  }

  @Test
  void testGetBigDecimalOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    testMethodOnClosedResult(() -> resultSet.getBigDecimal(1));
  }

  @Test
  void testGetBooleanFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertTrue(resultSet.getBoolean(1));
  }

  @Test
  void testGetBooleanFromString() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR, Boolean.TRUE.toString());
    Assertions.assertTrue(resultSet.getBoolean(1));
    initializeResult(TimestreamDataType.VARCHAR, "foo");
    Assertions.assertFalse(resultSet.getBoolean(1));
    initializeResult(TimestreamDataType.VARCHAR, "12.3");
    Assertions.assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void testGetBooleanFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertTrue(resultSet.getBoolean(1));

    initializeResult(TimestreamDataType.INTEGER, "0");
    Assertions.assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void testGetBooleanFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "1");
    Assertions.assertTrue(resultSet.getBoolean(1));

    initializeResult(TimestreamDataType.BIGINT, "0");
    Assertions.assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void testGetBooleanFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "1.3");
    Assertions.assertTrue(resultSet.getBoolean(1));

    initializeResult(TimestreamDataType.DOUBLE, "0.0");
    Assertions.assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void testGetBooleanWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.BOOLEAN, () -> resultSet.getBoolean(1), false);
  }

  @Test
  void testGetBooleanOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN);
    testMethodOnClosedResult(() -> resultSet.getBoolean(1));
  }

  @Test
  void testGetByteFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals((byte) 1, resultSet.getByte(1));
  }

  @Test
  void testGetByteFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "1");
    Assertions.assertEquals((byte) 1, resultSet.getByte(1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-129", "130", "-9223372036854775809", "9223372036854775809"})
  void testGetByteFromOutOfRangeValues(final String input) throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, input);

    final SQLException exception = Assertions.assertThrows(
      SQLException.class,
      () -> resultSet.getByte(1));

    TimestreamTestUtils.validateException(exception, Error.lookup(
      Error.VALUE_OUT_OF_RANGE,
      input,
      Byte.MIN_VALUE,
      Byte.MAX_VALUE,
      JDBCType.TINYINT));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.00, 1.0, 11.0})
  void testGetByteFromDoubleWithoutWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((byte) input, resultSet.getByte(1));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.14, 1.000001, 11.1111})
  void testGetByteFromDoubleWithWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((byte) input, resultSet.getByte(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.TINYINT));
  }

  @Test
  void testGetByteFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(1, resultSet.getByte(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(0, resultSet.getByte(1));
  }

  @Test
  void testGetByteWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testGetWithInvalidArgument(
      () -> resultSet.getByte(1),
      INVALID_ARGUMENT,
      TimestreamDataType.INTEGER);
  }

  @Test
  void testGetByteWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.INTEGER, () -> resultSet.getByte(1), (byte) 0);
  }

  @Test
  void testGetByteOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testMethodOnClosedResult(() -> resultSet.getByte(1));
  }

  @Test
  void testGetBytesOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    testMethodOnClosedResult(() -> resultSet.getBytes(1));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#dateParameters")
  void testGetDateFromDifferentValues(
    final TimestreamDataType inputType,
    final String input,
    final Date expected) throws SQLException {
    initializeResult(inputType, input);
    Assertions.assertEquals(expected, resultSet.getDate(1));
    Assertions.assertEquals(expected, resultSet.getDate(1, null));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#dateParametersWithTimezone")
  void testGetDateFromDifferentTimeZones(
    final Calendar inputCalendar,
    final TimestreamDataType inputType,
    final String input,
    final Date expected) throws SQLException {
    initializeResult(inputType, input);
    Assertions.assertEquals(expected, resultSet.getDate(1, inputCalendar));
  }

  @Test
  void testGetDateWithInvalidArgument() throws SQLException {
    final String dateString = "12:00:00";
    initializeResult(TimestreamDataType.DATE);
    testGetWithInvalidArgument(
      () -> resultSet.getDate(1),
      dateString,
      TimestreamDataType.DATE);
  }

  @Test
  void testGetDateWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.DATE, () -> resultSet.getDate(1), null);
  }

  @Test
  void testGetDateOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.DATE);
    testMethodOnClosedResult(() -> resultSet.getDate(1));
  }

  @Test
  void testGetDoubleFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals(3.14, resultSet.getDouble(1));
  }

  @Test
  void testGetDoubleFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(1.0, resultSet.getDouble(1));
  }

  @Test
  void testGetDoubleFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "3000000");
    Assertions.assertEquals(3000000.0, resultSet.getDouble(1));
  }

  @Test
  void testGetDoubleFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(0.0, resultSet.getDouble(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(1.0, resultSet.getDouble(1));
  }

  @Test
  void testGetDoubleWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    testGetWithInvalidArgument(
      () -> resultSet.getDouble(1),
      INVALID_ARGUMENT,
      TimestreamDataType.DOUBLE);
  }

  @Test
  void testGetDoubleWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.DOUBLE, () -> resultSet.getDouble(1), 0.0);
  }

  @Test
  void testGetDoubleOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    testMethodOnClosedResult(() -> resultSet.getDouble(1));
  }

  @Test
  void testGetFetchSize() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE);
    Assertions.assertEquals(MOCK_FETCH_SIZE, resultSet.getFetchSize());
  }

  @Test
  void testSetFetchSizeExceedsLimit() throws SQLException {
    Mockito.when(mockStatement.getFetchSize()).thenReturn(0);
    initializeResult(TimestreamDataType.DOUBLE);

    Assertions.assertEquals(0, resultSet.getFetchSize());
    resultSet.setFetchSize(20000);
    Assertions.assertEquals(1000, resultSet.getFetchSize());
  }

  @Test
  void testGetFetchSizeOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    testMethodOnClosedResult(() -> resultSet.getFetchSize());
  }

  @Test
  void testGetFloatFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals(3.14f, resultSet.getFloat(1));
  }

  @Test
  void testGetFloatFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "3000000");
    Assertions.assertEquals(3000000, resultSet.getFloat(1));
  }

  @Test
  void testGetFloatFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(1f, resultSet.getFloat(1));
  }

  @Test
  void testGetFloatFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(0, resultSet.getFloat(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(1, resultSet.getFloat(1));
  }

  @Test
  void testGetFloatWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testGetWithInvalidArgument(
      () -> resultSet.getFloat(1),
      INVALID_ARGUMENT,
      TimestreamDataType.INTEGER);
  }

  @Test
  void testGetFloatWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.DOUBLE, () -> resultSet.getFloat(1), 0.0f);
  }

  @Test
  void testGetFloatOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testMethodOnClosedResult(() -> resultSet.getFloat(1));
  }

  @Test
  void testGetIntFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(1, resultSet.getInt(1));
  }

  @Test
  void testGetIntFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals(3, resultSet.getInt(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.INTEGER));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-2147483649.0", "2147483649.0", "-1.7976931348623157E+308",
    "1.7976931348623157E+308"})
  void testGetIntFromOutOfRangeValues(final String input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, input);

    final SQLException exception = Assertions.assertThrows(
      SQLException.class,
      () -> resultSet.getInt(1));

    TimestreamTestUtils.validateException(exception, Error.lookup(
      Error.VALUE_OUT_OF_RANGE,
      input,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE,
      JDBCType.INTEGER));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.00, 1.0, 11.0})
  void testGetIntFromDoubleWithoutWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((int) input, resultSet.getInt(1));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.14, 1.000001, 11.1111})
  void testGetIntFromDoubleWithWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((int) input, resultSet.getInt(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.INTEGER));
  }

  @Test
  void testGetIntFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "3000000");
    Assertions.assertEquals(3000000, resultSet.getInt(1));
  }

  @Test
  void testGetIntFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN);
    Mockito.when(mockData.getScalarValue()).thenReturn("false");
    resultSet.next();

    Assertions.assertEquals(0, resultSet.getInt(1));

    Mockito.when(mockData.getScalarValue()).thenReturn("true");
    resultSet.next();

    Assertions.assertEquals(1, resultSet.getInt(1));
  }

  @Test
  void testGetIntWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.INTEGER, () -> resultSet.getInt(1), 0);
  }

  @Test
  void testGetIntWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testGetWithInvalidArgument(
      () -> resultSet.getInt(1),
      INVALID_ARGUMENT,
      TimestreamDataType.INTEGER);
  }

  @Test
  void testGetIntOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testMethodOnClosedResult(() -> resultSet.getInt(1));
  }

  @Test
  void testGetLongFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "3000000");
    Assertions.assertEquals(3000000L, resultSet.getLong(1));
  }

  @Test
  void testGetLongFromDouble() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals(3L, resultSet.getLong(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.BIGINT));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-9223372036854775809.00", "9223372036854775809.00",
    "-1.7976931348623157E+308", "1.7976931348623157E+308"})
  void testGetLongFromOutOfRangeValues(final String input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, input);

    final SQLException exception = Assertions
      .assertThrows(SQLException.class, () -> resultSet.getLong(1));

    TimestreamTestUtils.validateException(exception, Error.lookup(
      Error.VALUE_OUT_OF_RANGE,
      input,
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      JDBCType.BIGINT));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.00, 1.0, 11.0})
  void testGetLongFromDoubleWithoutWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((long) input, resultSet.getLong(1));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.14, 1.000001, 11.1111})
  void testGetLongFromDoubleWithWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((long) input, resultSet.getLong(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.BIGINT));
  }

  @Test
  void testGetLongFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(1, resultSet.getLong(1));
  }

  @Test
  void testGetLongFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(0, resultSet.getLong(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(1, resultSet.getLong(1));
  }

  @Test
  void testGetLongWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT);
    testGetWithInvalidArgument(
      () -> resultSet.getLong(1),
      INVALID_ARGUMENT,
      TimestreamDataType.BIGINT);
  }

  @Test
  void testGetLongWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.BIGINT, () -> resultSet.getLong(1), 0L);
  }

  @Test
  void testGetLongOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testMethodOnClosedResult(() -> resultSet.getLong(1));
  }

  @Test
  void testGetMetaDataOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.close();
    Assertions.assertThrows(SQLException.class, () -> resultSet.getMetaData());
  }

  @Test
  void testGetShortFromInt() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER, "1");
    Assertions.assertEquals(1, resultSet.getShort(1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-32769", "32768", "-9223372036854775809", "9223372036854775809"})
  void testGetShortFromOutOfRangeValues(final String input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, input);

    final SQLException exception = Assertions
      .assertThrows(SQLException.class, () -> resultSet.getShort(1));

    TimestreamTestUtils.validateException(exception, Error.lookup(
      Error.VALUE_OUT_OF_RANGE,
      input,
      Short.MIN_VALUE,
      Short.MAX_VALUE,
      JDBCType.SMALLINT));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.00, 1.0, 11.0})
  void testGetShortFromDoubleWithoutWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((short) input, resultSet.getShort(1));
  }

  @ParameterizedTest
  @ValueSource(doubles = {3.14, 1.000001, 11.1111})
  void testGetShortFromDoubleWithWarning(final double input) throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, String.valueOf(input));
    Assertions.assertEquals((short) input, resultSet.getShort(1));
    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(
        Warning.VALUE_TRUNCATED,
        TimestreamDataType.DOUBLE,
        JdbcType.SMALLINT));
  }

  @Test
  void testGetShortFromLong() throws SQLException {
    initializeResult(TimestreamDataType.BIGINT, "30000");
    Assertions.assertEquals((short) 30000, resultSet.getShort(1));
  }

  @Test
  void testGetShortFromBoolean() throws SQLException {
    initializeResult(TimestreamDataType.BOOLEAN, Boolean.FALSE.toString());
    Assertions.assertEquals(0, resultSet.getShort(1));

    initializeResult(TimestreamDataType.BOOLEAN, Boolean.TRUE.toString());
    Assertions.assertEquals(1, resultSet.getShort(1));
  }

  @Test
  void testGetShortWithInvalidArgument() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    testGetWithInvalidArgument(
      () -> resultSet.getShort(1),
      INVALID_ARGUMENT,
      TimestreamDataType.INTEGER);
  }

  @Test
  void testGetShortWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.INTEGER, () -> resultSet.getShort(1), (short) 0);
  }

  @Test
  void testGetVarcharWithMaxFieldSize() throws SQLException, IOException {
    final String testString = "3.14159@Œ§";
    Mockito
      .when(mockColumnInfo.getType())
      .thenReturn(new Type().withScalarType(TimestreamDataType.VARCHAR.toString()));
    Mockito.when(mockData.getScalarValue()).thenReturn(testString);

    // Test for getString with max field size that is larger than string size.
    resultSet = new TimestreamResultSet(mockStatement, "", mockResult, new HashMap<>(), 0, 12);
    resultSet.next();
    Assertions.assertEquals(testString, resultSet.getString(1));

    // Test for getString with max field size that is smaller than string size.
    resultSet = new TimestreamResultSet(mockStatement, "", mockResult, new HashMap<>(), 0, 3);
    resultSet.next();
    Assertions.assertEquals("3.1", resultSet.getString(1));

    resultSet = new TimestreamResultSet(
      mockStatement,
      "",
      mockResult,
      new HashMap<>(),
      0,
      testString.length() - 1);
    resultSet.next();

    final ByteArrayInputStream expectation = new ByteArrayInputStream(
      "3.14159@Œ".getBytes(StandardCharsets.US_ASCII));
    Assertions.assertEquals(
      IOUtils.toString(expectation),
      IOUtils.toString(resultSet.getAsciiStream(1)));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#stringParameters")
  void testGetStringFromDifferentValues(
    final TimestreamDataType inputType,
    final String string,
    final String expectedString) throws SQLException {
    initializeResult(inputType, string);
    Assertions.assertEquals(expectedString, resultSet.getString(1));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timeSeriesStringArguments")
  void testGetStringFromTimeSeries(
    final List<TimeSeriesDataPoint> input,
    final String expected) throws SQLException {
    initializeResult(TimestreamDataType.TIMESERIES);
    resultSet.next();
    Mockito.when(mockData.getTimeSeriesValue()).thenReturn(input);

    Assertions.assertEquals(expected, resultSet.getString(1));
  }

  @Test
  void testGetStringWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.VARCHAR, () -> resultSet.getString(1), null);
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timeParameters")
  void testGetTimeFromDifferentValues(
    final TimestreamDataType inputType,
    final String timeString,
    final Time expectedTime) throws SQLException {
    initializeResult(inputType, timeString);
    Assertions.assertEquals(expectedTime, resultSet.getTime(1));
    Assertions.assertEquals(expectedTime, resultSet.getTime(1, null));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timeParametersWithTimezone")
  void testGetTimeFromDifferentValuesAndTimeZones(
    final Calendar calendar,
    final TimestreamDataType inputType,
    final String timeString,
    final long expectedTime) throws SQLException {
    initializeResult(inputType, timeString);
    Assertions.assertEquals(expectedTime, resultSet.getTime(1, calendar).getTime());
  }

  @Test
  void testGetTimeWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.TIME, () -> resultSet.getTime(1), null);
  }

  @Test
  void testGetTimeWithInvalidArgument() throws SQLException {
    final String timeString = "12-02-01";
    initializeResult(TimestreamDataType.TIME);
    testGetWithInvalidArgument(
        () -> resultSet.getTime(1),
        timeString,
        TimestreamDataType.TIME);
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timestampParameters")
  void testGetTimestampFromDifferentValues(
    final TimestreamDataType inputType,
    final String input,
    final Timestamp expected) throws SQLException {
    initializeResult(inputType, input);
    Assertions.assertEquals(expected, resultSet.getTimestamp(1));
    Assertions.assertEquals(expected, resultSet.getTimestamp(1, null));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#timestampParametersWithTimezone")
  void testGetTimestampFromDifferentValuesAndTimeZones(
    final Calendar inputCalendar,
    final TimestreamDataType inputType,
    final String input,
    final long expected) throws SQLException {
    initializeResult(inputType, input);
    Assertions.assertEquals(expected, resultSet.getTimestamp(1, inputCalendar).getTime());
  }

  @Test
  void testGetTimestampWithInvalidArgument() throws SQLException {
    final String timestampString = "22:21:55";
    initializeResult(TimestreamDataType.TIMESTAMP);
    testGetWithInvalidArgument(
      () -> resultSet.getTimestamp(1),
      timestampString,
      TimestreamDataType.TIMESTAMP);
  }

  @Test
  void testGetTimestampWithNullData() throws SQLException {
    testGetNull(TimestreamDataType.TIMESTAMP, () -> resultSet.getTimestamp(1), null);
  }

  @Test
  void testGetTimestampOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.TIMESTAMP);
    testMethodOnClosedResult(() -> resultSet.getTimestamp(1));
  }

  @Test
  void testIsClosed() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertFalse(resultSet.isClosed());
    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
  }

  @Test
  void testSetFetchSize() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    final int fetchSize = 2;
    resultSet.setFetchSize(fetchSize);
    Assertions.assertEquals(fetchSize, resultSet.getFetchSize());
  }

  @Test
  void testSetFetchSizeWithNegativeSize() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertThrows(SQLException.class, () -> resultSet.setFetchSize(-1));
  }

  @Test
  void testWasNull() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    Assertions.assertFalse(resultSet.wasNull());
  }

  @Test
  void testWasNullOnClosedResultSet() throws SQLException {
    initializeResult(TimestreamDataType.VARCHAR);
    resultSet.close();

    final Exception exception = Assertions.assertThrows(
      SQLException.class,
      () -> resultSet.wasNull());

    TimestreamTestUtils.validateException(exception, Error.lookup(Error.RESULT_SET_CLOSED));
  }

  @Test
  void testGetObjectFromUnsupportedSourceType() throws SQLException {
    initializeResult(TimestreamDataType.UNKNOWN);
    Mockito.when(mockData.getScalarValue()).thenReturn("");
    resultSet.next();

    final SQLException exception = Assertions.assertThrows(
      SQLException.class,
      () -> resultSet.getInt(1));

    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(
        Error.UNSUPPORTED_CONVERSION,
        TimestreamDataType.UNKNOWN,
        JdbcType.INTEGER));
  }

  @Test
  void testGetObjectFromUnsupportedTargetType() throws SQLException {
    initializeResult(TimestreamDataType.INTEGER);
    Mockito.when(mockData.getScalarValue()).thenReturn("");
    resultSet.next();

    final SQLException exception = Assertions.assertThrows(
      SQLException.class,
      () -> resultSet.getArray(1));

    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(
        Error.UNSUPPORTED_CONVERSION,
        TimestreamDataType.INTEGER,
        JdbcType.ARRAY));
  }

  @Test
  void testGetMethodsWithMultipleWarnings() throws SQLException {
    initializeResult(TimestreamDataType.DOUBLE, "3.14");
    Assertions.assertEquals((short) 3, resultSet.getShort(1));
    Assertions.assertEquals((byte) 3, resultSet.getByte(1));

    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings(),
      Warning.lookup(Warning.VALUE_TRUNCATED, TimestreamDataType.DOUBLE, JdbcType.SMALLINT));

    TimestreamTestUtils.validateWarning(
      resultSet.getWarnings().getNextWarning(),
      Warning.lookup(Warning.VALUE_TRUNCATED, TimestreamDataType.DOUBLE, JdbcType.TINYINT));

    Assertions.assertNull(resultSet.getWarnings().getNextWarning().getNextWarning());
  }

  /**
   * Test calling a method on a closed {@link TimestreamResultSet}.
   *
   * @param getter The method executable.
   * @throws SQLException If an error occurs while closing the {@link TimestreamResultSet}.
   */
  private void testMethodOnClosedResult(final Executable getter) throws SQLException {
    resultSet.close();

    final Exception exception = Assertions.assertThrows(SQLException.class, getter);
    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(Error.RESULT_SET_CLOSED, 1));
  }

  /**
   * Test a scalar getter when the Timestream value is null.
   *
   * @param <T>       The data type of the column.
   * @param colType   The Timestream type of the column to test.
   * @param supplier  The function to invoke to test the null retrieval.
   * @param nullValue The value that should be returned if the result is null.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private <T> void testGetNull(TimestreamDataType colType, ResultGetter<T> supplier, T nullValue)
    throws SQLException {
    Mockito.when(mockData.getNullValue()).thenReturn(Boolean.TRUE);
    initializeResult(colType);
    resultSet.next();

    Assertions.assertEquals(nullValue, supplier.get());
    Assertions.assertTrue(resultSet.wasNull());
  }

  /**
   * Test calling a getter on invalid data that does not conform to the expected source type.
   *
   * @param getter      The getter executable.
   * @param invalidData The data that does not conform to the expected source type.
   * @param sourceType  The expected {@link TimestreamDataType} of the data.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void testGetWithInvalidArgument(
    final Executable getter,
    final String invalidData,
    final TimestreamDataType sourceType) throws SQLException {
    Mockito.when(mockData.getScalarValue()).thenReturn(invalidData);
    resultSet.next();

    final Exception exception = Assertions.assertThrows(SQLException.class, getter);
    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType));
  }

  /**
   * Initialize {@link TimestreamResultSet} with a nestedRow value.
   *
   * @param scalarValue The scalar value in the row.
   * @param type        The Timestream data of the scalar value.
   * @throws SQLException If an error occurs while retrieving the value.
   */
  private void initializeResultSetWithNestedRow(String scalarValue, TimestreamDataType type)
    throws SQLException {
    final Row rowValue = new Row().withData(new Datum().withScalarValue(scalarValue));
    final Row row = new Row().withData(new Datum().withRowValue(rowValue));
    final ColumnInfo columnInfo = new ColumnInfo().withType(new Type().withRowColumnInfo(
      new ColumnInfo().withType(new Type().withRowColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(type.name()))))));

    Mockito.when(mockData.getRowValue()).thenReturn(row);
    Mockito.when(mockColumnInfo.getType()).thenReturn(columnInfo.getType());
    resultSet = new TimestreamResultSet(mockStatement, "", mockResult);
  }
}
