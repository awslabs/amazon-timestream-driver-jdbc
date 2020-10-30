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

package software.amazon.timestream.jdbc;

import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TimeZone;

class TimestreamArrayTest {
  @Mock
  TimestreamBaseResultSet mockResultSet;

  private TimestreamArray array;

  static {
    // Override the JVM timezone for unit tests.
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Paris"));
  }

  @BeforeEach
  void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  void testGetBaseTypeName() throws SQLException {
    Mockito.when(mockResultSet.isClosed()).thenReturn(false);
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    Assertions.assertEquals("DOUBLE", array.getBaseTypeName());
  }

  @Test
  void testGetBaseTypeNameWithClosedResultSet() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    testMethodWithClosedParentResultSet(() -> array.getBaseTypeName());
  }

  @Test
  void testGetBaseType() throws SQLException {
    Mockito.when(mockResultSet.isClosed()).thenReturn(false);
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    Assertions.assertEquals(8, array.getBaseType());
  }

  @Test
  void testGetBaseTypeWithClosedResultSet() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    testMethodWithClosedParentResultSet(() -> array.getBaseType());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayArguments")
  void testGetArray(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateArray(expected, array.getArray());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#indexArguments")
  void testGetArrayWithIndex(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final int startingIndex,
    final int count) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateArray(expected, array.getArray(startingIndex, count));
  }

  @Test
  void testGetArrayWithInvalidIndex() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DATE, "2020-05-16");
    Assertions.assertThrows(SQLException.class, () -> array.getArray(-1, 1));
    Assertions.assertThrows(SQLException.class, () -> array.getArray(3, 1));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayDateTimeArguments")
  void testGetArrayWithDateTime(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    Assertions.assertArrayEquals(expected, (Object[]) array.getArray());
  }

  @Test
  void testGetArrayWithEmptyArray() throws SQLException {
    final List<Object> emptyList = new ArrayList<>();
    final Object[] expectedArray = new Object[]{};
    array = new TimestreamArray(emptyList, new Type().withScalarType("DOUBLE"), mockResultSet);
    Assertions.assertArrayEquals(expectedArray, (Object[]) array.getArray());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayArguments")
  void testGetArrayWithEmptyMap(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateArray(expected, array.getArray(new HashMap<>()));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayMapArguments")
  void testGetArrayWithMap(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateArray(expected, array.getArray(conversionMap));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayToStringArguments")
  void testGetArrayToString(
      final String expected,
      final Type baseType,
      final List<Object> inputList,
      final Map<String, Class<?>> conversionMap) throws SQLException {
    initTimestreamArrayWithList(baseType, inputList);
    Assertions.assertEquals(expected, ((Object[]) array.getArray(conversionMap))[0]);
  }

  @Test
  void testGetArrayWithNestedArray() throws SQLException {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);
    final Type inputType = new Type().withArrayColumnInfo(
        new ColumnInfo().withType(new Type().withArrayColumnInfo(
            new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())))));
    final List<Object> inputList = ImmutableList.of(
        new Datum().withArrayValue(
            new Datum().withArrayValue(
                new Datum().withScalarValue("1.1"),
                new Datum().withScalarValue("2.2")))
    );
    final Object[] expected = new Object[]{1, 2};
    initTimestreamArrayWithList(inputType, inputList);
    array.getArray(conversionMap);
    int depth = 0;
    Object[] arr = (Object[]) array.getArray(conversionMap);
    while (arr[0] instanceof TimestreamArray) {
      // Keep calling `getArray` until we have reached the inner most array.
      arr = (Object[]) ((TimestreamArray) arr[0]).getArray();
      depth++;
    }
    Assertions.assertEquals(2, depth);
    Assertions.assertArrayEquals(expected, arr);
  }

  @Test
  void testGetArrayWithRow() throws SQLException {
    final Object[] expectation = initializeArrayWithRow();
    final Object[] actual = ((TimestreamStruct) ((Object[]) array.getArray(new HashMap<>()))[0]).getAttributes();
    Assertions.assertArrayEquals(expectation, actual);
  }

  @Test
  void testGetArrayWithInvalidColumnMetadata() {
    final Datum data = new Datum().withTimeSeriesValue(new TimeSeriesDataPoint()
      .withTime("2020-05-05 16:51:30.000000000")
      .withValue(new Datum().withScalarValue("82")));

    initTimestreamArrayWithList(
      TimestreamTestUtils.createArrayType(TimestreamDataType.TIMESERIES),
      ImmutableList.of(data));
    Assertions.assertThrows(RuntimeException.class, () -> array.getArray());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#indexMapArguments")
  void testGetArrayWithMapAndIndex(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final int startingIndex,
    final int count,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateArray(expected, array.getArray(startingIndex, count, conversionMap));
  }

  @Test
  void testGetArrayWithUnsupportedConversion() {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(JdbcType.DATE.name(), Time.class);
    initTimestreamArrayWithScalarValue(TimestreamDataType.DATE, "2020-05-11");
    Assertions.assertThrows(SQLException.class, () -> array.getArray(conversionMap));
  }

  @Test
  void testGetArrayWithClosedResultSet() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    testMethodWithClosedParentResultSet(() -> array.getArray());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayArguments")
  void testGetResultSet(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet());
  }

  @Test
  void testGetResultSetWithNestedTimeSeriesArray() throws SQLException {
    // Setting up data for a nested Array with TimeSeries.
    // Represents an array like so: [[ [{<TimeSeriesDataPoint>}] ]]
    final List<TimeSeriesDataPoint> timeSeriesArrayWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));

    final Object[][] expectedNestedTimeSeries = new Object[][] {new Object[]{timeSeriesArrayWithTimeAndValue}};

    final Type inputType = new Type().withArrayColumnInfo(
      new ColumnInfo().withType(new Type().withArrayColumnInfo(
        new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
          new ColumnInfo().withType(new Type().withScalarType("DOUBLE")))))));

    final List<Object> nestedTimeSeriesInputList = ImmutableList
      .of(new Datum().withArrayValue(new Datum().withArrayValue(new Datum().withTimeSeriesValue(timeSeriesArrayWithTimeAndValue))));

    initTimestreamArrayWithList(inputType, nestedTimeSeriesInputList);
    final ResultSet rs = array.getResultSet();
    while (rs.next()) {
      Assertions.assertTrue(rs.getObject(2) instanceof TimestreamArray);
      final TimestreamArray outerArray = (TimestreamArray) rs.getObject(2);
      final TimestreamArray nestedArray = (TimestreamArray) ((Object[]) outerArray.getArray())[0];
      Assertions.assertArrayEquals(expectedNestedTimeSeries[0], (Object[]) nestedArray.getArray());
    }
  }

  @Test
  void testGetResultSetOnTimeSeries() throws SQLException {
    // Setting up data for a nested Array with TimeSeries.
    // Represents an array like so: [[ [{<TimeSeriesDataPoint>}] ]]
    final List<TimeSeriesDataPoint> timeSeriesWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));

    final Type inputType = new Type().withTimeSeriesMeasureValueColumnInfo(
      new ColumnInfo().withType(new Type().withScalarType("DOUBLE")));

    final List<Object> nestedTimeSeriesInputList = ImmutableList
      .of(new Datum().withTimeSeriesValue(timeSeriesWithTimeAndValue));

    initTimestreamArrayWithList(inputType, nestedTimeSeriesInputList);
    validateResultSet(new Object[] {timeSeriesWithTimeAndValue}, array.getResultSet());
  }

  @Test
  void testGetResultSetWithNestedTimeseries() throws SQLException {
    // Setting up data for a nested Array with TimeSeries.
    // Represents an array like so: [[ [{<TimeSeriesDataPoint>}] ]]
    final List<TimeSeriesDataPoint> timeSeriesWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));

    final Object[] expectedTimeSeriesArray = new Object[]{timeSeriesWithTimeAndValue};

    final Type inputType = new Type().withArrayColumnInfo(
      new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType("DOUBLE")))
      )
    );

    final List<Object> nestedTimeSeriesInputList = ImmutableList
      .of(new Datum().withArrayValue(new Datum().withTimeSeriesValue(timeSeriesWithTimeAndValue)));

    initTimestreamArrayWithList(inputType, nestedTimeSeriesInputList);
    validateResultSet(new Object[][]{expectedTimeSeriesArray}, array.getResultSet());
  }

  @Test
  void testGetResultSetWithNestedRow() throws SQLException {
    // Setting up data for a nested Array with Row.
    final Row row = new Row()
      .withData(new Datum().withScalarValue("3.14"), new Datum().withScalarValue("2"));

    final List<Object[]> expected = ImmutableList.of(new Object[]{3.14, 2});

    final List<Object> nestedRowValues = new ArrayList<>();
    final Datum rowValues = new Datum().withRowValue(row);
    nestedRowValues.add(new Datum().withArrayValue(rowValues));

    final Type nestedRowType = new Type().withArrayColumnInfo(new ColumnInfo().withType(
      new Type().withRowColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())),
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())
        ))
    ));

    initTimestreamArrayWithList(nestedRowType, nestedRowValues);

    final ResultSet rs = array.getResultSet();
    int i = 0;
    while (rs.next()) {
      Assertions.assertArrayEquals(
        expected.get(i++),
        ((TimestreamStruct) ((Object[]) rs.getArray(2).getArray())[0]).getAttributes());
    }
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#indexArguments")
  void testGetResultSetWithIndex(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final int startingIndex,
    final int count) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet(startingIndex, count));
  }

  @Test
  void testGetResultSetWithRow() throws SQLException {
    final Object[] expected = initializeArrayWithRow();

    final ResultSet rs = array.getResultSet();
    while (rs.next()) {
      Assertions.assertArrayEquals(expected, ((Struct) rs.getObject(2)).getAttributes());
    }
  }

  @Test
  void testGetResultSetWithRowAndEmptyMap() throws SQLException {
    final Object[] expected = initializeArrayWithRow();
    final ResultSet rs = array.getResultSet(new HashMap<>());
    while (rs.next()) {
      Assertions.assertArrayEquals(expected, ((Struct) rs.getObject(2)).getAttributes());
    }
  }

  @Test
  void testGetResultSetWithRowWithMapToString() throws SQLException {
    final Map<String, Class<?>> map = new HashMap<>();
    map.put(TimestreamDataType.ROW.name(), String.class);

    final Object[] expected = initializeArrayWithRow();
    final StringJoiner joiner = new StringJoiner(", ", "(", ")");
    for (final Object o : expected) {
      joiner.add(o.toString());
    }

    final ResultSet rs = array.getResultSet(map);
    while (rs.next()) {
      Assertions.assertEquals(joiner.toString(), rs.getObject(2));
    }
  }

  @Test
  void testGetResultSetWithNestedRowWithMapToString() throws SQLException {
    final Map<String, Class<?>> map = new HashMap<>();
    map.put(TimestreamDataType.ROW.name(), String.class);

    final Object[] expected = initializeArrayWithNestedRow();
    final StringJoiner joiner = new StringJoiner(", ", "(", ")");
    for (final Object o : expected) {
      joiner.add(o.toString());
    }

    final ResultSet rs = array.getResultSet(map);
    while (rs.next()) {
      Assertions.assertEquals(joiner.toString(), rs.getObject(2));
    }
  }

  @Test
  void testGetResultSetWithUnsupportedConversion() {
    final Map<String, Class<?>> unsupportedConversionMap = new HashMap<>();
    unsupportedConversionMap.put(TimestreamDataType.ROW.name(), List.class);
    initializeArrayWithNestedRow();
    Assertions.assertThrows(SQLException.class, () -> array.getResultSet(unsupportedConversionMap));

    // Float is not a supported Java class that can be mapped to a Timestream supported data type.
    unsupportedConversionMap.put(TimestreamDataType.ROW.name(), Float.class);
    Assertions.assertThrows(SQLException.class, () -> array.getResultSet(unsupportedConversionMap));
  }

  @Test
  void testGetResultSetWithInvalidIndex() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DATE, "2020-05-16");
    Assertions.assertThrows(SQLException.class, () -> array.getResultSet(-1, 1));
    Assertions.assertThrows(SQLException.class, () -> array.getResultSet(3, 1));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayDateTimeArguments")
  void testGetResultSetWithDateTime(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayMapArguments")
  void testGetResultSetWithMap(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet(conversionMap));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#arrayArguments")
  void testGetResultSetWithEmptyMap(
    final Type inputType,
    final List<Object> input,
    final Object[] expected) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet(new HashMap<>()));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#indexMapArguments")
  void testGetResultSetWithMapAndIndex(
    final Type inputType,
    final List<Object> input,
    final Object[] expected,
    final int startingIndex,
    final int count,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    initTimestreamArrayWithList(inputType, input);
    validateResultSet(expected, array.getResultSet(startingIndex, count, conversionMap));
  }

  @Test
  void testGetResultSetWithClosedParentResultSet() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    testMethodWithClosedParentResultSet(() -> array.getResultSet());
  }

  @Test
  void testToString() {
    initTimestreamArrayWithScalarValue(TimestreamDataType.DOUBLE, "3.14159");
    Assertions.assertEquals("[3.14159]", array.toString());
  }

  @Test
  void testToStringWithNestedRow() {
    initializeArrayWithRow();
    Assertions.assertEquals("[(3.14, 2)]", array.toString());
  }

  /**
   * Initialize a {@link TimestreamArray} with the given base data type and the given scalar value.
   *
   * @param baseType The data type of the value.
   * @param value    String representing the scalar value in the array.
   */
  private void initTimestreamArrayWithScalarValue(
    final TimestreamDataType baseType,
    final String value) {
    final List<Object> arrayList = new ArrayList<>();
    final Datum data = new Datum().withScalarValue(value);
    arrayList.add(data);

    array = new TimestreamArray(arrayList, new Type().withScalarType(baseType.name()),
      mockResultSet);
  }

  /**
   * Initialize a {@link TimestreamArray} with the given base data type and the give list of
   * values.
   *
   * @param baseType  The data type of the value.
   * @param valueList The list of values contained in the array.
   */
  private void initTimestreamArrayWithList(
    final Type baseType,
    final List<Object> valueList) {
    array = new TimestreamArray(valueList, baseType, mockResultSet);
  }

  /**
   * Test calling a method on a closed {@link TimestreamResultSet}.
   *
   * @param method The method executable.
   */
  private void testMethodWithClosedParentResultSet(final Executable method) {
    Mockito.when(mockResultSet.isClosed()).thenReturn(true);

    final Exception exception = Assertions.assertThrows(SQLException.class, method);
    TimestreamTestUtils.validateException(
      exception,
      Error.lookup(Error.RESULT_SET_CLOSED, 1));
  }

  /**
   * Validate the {@link TimestreamArrayResultSet} generated by {@link TimestreamArray}.
   *
   * @param expected The expected contain in the result set.
   * @param rs       The actual result set produced by {@link TimestreamArray}.
   * @throws SQLException if an error occurred while retrieving the result.
   */
  private void validateResultSet(final Object[] expected, final ResultSet rs)
    throws SQLException {
    int i = 0;
    while (rs.next()) {
      final int type = rs.getMetaData().getColumnType(2);
      if (JdbcType.ARRAY.jdbcCode == type) {
        validateArray((Object[]) expected[i++], rs.getArray(2).getArray());
      } else {
        Assertions.assertEquals(expected[i++], rs.getObject(2));
      }
    }
  }

  /**
   * Initialize the array with a row.
   *
   * @return the values in the row.
   */
  private Object[] initializeArrayWithRow() {
    final Row row = new Row()
      .withData(new Datum().withScalarValue("3.14"), new Datum().withScalarValue("2"));

    final Object[] expected = new Object[]{3.14, 2};

    final List<Object> nestedRowValues = new ArrayList<>();
    final Datum rowValues = new Datum().withRowValue(row);
    nestedRowValues.add(rowValues);

    final Type type = new Type().withRowColumnInfo(
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())),
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())
      ));
    initTimestreamArrayWithList(type, nestedRowValues);
    return expected;
  }

  /**
   * Initialize the array with a nested row.
   *
   * @return the values in the row.
   */
  private Object[] initializeArrayWithNestedRow() {
    final Row nestedRow = new Row().withData(new Datum().withScalarValue("1.0"));
    final Row row = new Row().withData(
            new Datum().withScalarValue("3.14"),
            new Datum().withScalarValue("2"),
            new Datum().withRowValue(nestedRow));
    final TimestreamStruct expectRow = new TimestreamStruct(
        ImmutableList.of(new Datum().withScalarValue("1.0")),
        ImmutableList.of(new ColumnInfo().withType(
            new Type().withRowColumnInfo(
            new ColumnInfo()
                .withType(new Type()
                    .withScalarType(TimestreamDataType.DOUBLE.name()))))), mockResultSet);
    final Object[] expected = new Object[]{3.14, 2, expectRow};

    final List<Object> nestedRowValues = new ArrayList<>();
    nestedRowValues.add(new Datum().withRowValue(row));

    final Type type = new Type().withRowColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())),
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())),
        new ColumnInfo().withType(new Type().withRowColumnInfo(
            new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name()))))
    );

    initTimestreamArrayWithList(type, nestedRowValues);
    return expected;
  }

  /**
   * Validate the content in the given array.
   *
   * @param expected    An array containing the expected values.
   * @param resultArray The actual array from TimestreamArray.
   * @throws SQLException if an error occurred while parsing nested values in TimestreamArray.
   */
  private void validateArray(final Object[] expected, final Object resultArray)
    throws SQLException {
    for (int i = 0; i < expected.length; i++) {
      if (resultArray instanceof String) {
        // The resultArray is a string.
        Assertions.assertEquals(expected[i], resultArray);
      } else if (resultArray instanceof TimeSeriesDataPoint[]) {
        // The resultArray is a TimeSeries array.
        Assertions.assertEquals(expected[i], (((Object[]) resultArray)[i]));
      } else if (resultArray instanceof TimestreamArray[]) {
        // The resultArray is a nested array.
        Assertions.assertArrayEquals((Object[]) expected[i],
          (Object[]) ((TimestreamArray[]) resultArray)[i].getArray());
      } else {
        // The resultArray is a nested TimeSeries array.
        Assertions.assertArrayEquals(expected, ((Object[]) resultArray));
      }
    }
  }
}
