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

import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TimestreamStructTest {

  private TimestreamStruct timestreamStruct;
  private static final Map<String, Class<?>> CONVERSION_MAP = new ImmutableMap.Builder<String, Class<?>>()
    .put(TimestreamDataType.INTEGER.name(), Double.class)
    .put(TimestreamDataType.ARRAY.name(), String.class)
    .put(TimestreamDataType.DOUBLE.name(), String.class)
    .put(TimestreamDataType.VARCHAR.name(), Double.class)
    .put(TimestreamDataType.TIMESERIES.name(), String.class)
    .put(TimestreamDataType.TIME.name(), Timestamp.class)
    .put(TimestreamDataType.DATE.name(), String.class)
    .put(TimestreamDataType.TIMESTAMP.name(), Time.class)
    .put(TimestreamDataType.ROW.name(), String.class)
    .build();

  @Mock
  private TimestreamBaseResultSet mockParentResultSet;

  @BeforeEach
  void init() {
    MockitoAnnotations.initMocks(this);
    timestreamStruct = new TimestreamStruct(new ArrayList<>(), new ArrayList<>(), mockParentResultSet);
  }

  @Test
  void testGetSQLTypeNameOnClosedParentResultSet() {
    Mockito.when(mockParentResultSet.isClosed()).thenReturn(Boolean.TRUE);
    Assertions.assertThrows(SQLException.class, () -> timestreamStruct.getSQLTypeName());
  }

  @Test
  void testGetSQLTypeName() throws SQLException {
    Assertions
      .assertEquals("com.amazonaws.services.timestreamquery.model.Row",
        timestreamStruct.getSQLTypeName());
  }

  @Test
  void testGetAttributesOnClosedParentResultSet() {
    Mockito.when(mockParentResultSet.isClosed()).thenReturn(Boolean.TRUE);
    Assertions.assertThrows(SQLException.class, () -> timestreamStruct.getAttributes());
    Assertions
      .assertThrows(SQLException.class, () -> timestreamStruct.getAttributes(CONVERSION_MAP));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#structScalarValuesArguments")
  void testGetAttributesWithScalarValue(
    final TimestreamDataType inputType,
    final String input,
    final Object expected) throws SQLException {
    initStructWithScalarValue(input, inputType);
    Assertions.assertEquals(expected, timestreamStruct.getAttributes()[0]);
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#structScalarValuesConversionMapArguments")
  void testGetAttributesWithScalarValueAndConversionMap(
    final TimestreamDataType inputType,
    final String input,
    final Object expected) throws SQLException {
    initStructWithScalarValue(input, inputType);
    Assertions.assertEquals(expected, timestreamStruct.getAttributes(CONVERSION_MAP)[0]);
  }

  @Test
  void testGetAttributesWithVarchar() throws SQLException {
    initStructWithScalarValue("1.2", TimestreamDataType.VARCHAR);

    Assertions.assertEquals("1.2", timestreamStruct.getAttributes()[0]);
    Assertions.assertEquals(1.2, timestreamStruct.getAttributes(CONVERSION_MAP)[0]);
  }

  @Test
  void testGetAttributesWithRow() throws SQLException {
    final Object[] expectation = new Object[]{2, 3};

    final Row row = new Row().withData(new Datum().withScalarValue("2"))
      .withData(new Datum().withScalarValue("3"));
    final List<Datum> structDatum = ImmutableList.of(new Datum().withRowValue(row));
    final List<ColumnInfo> baseTypeList = ImmutableList.of(new ColumnInfo().withType(new Type().withRowColumnInfo(
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())))));

    final TimestreamStruct struct = new TimestreamStruct(structDatum, baseTypeList, mockParentResultSet);
    Assertions.assertArrayEquals(expectation, ((Struct) struct.getAttributes()[0]).getAttributes());
    Assertions.assertEquals("(2.0, 3.0)", struct.getAttributes(CONVERSION_MAP)[0]);
  }

  @Test
  void testGetAttributesWithArray() throws SQLException {
    final List<Datum> array = new ArrayList<>();
    array.add(new Datum().withScalarValue("2"));
    array.add(new Datum().withScalarValue("3"));
    final List<Object> expectation = new ArrayList<>();
    expectation.add(2);
    expectation.add(3);

    initStructWithArray(array, TimestreamDataType.INTEGER);
    Assertions.assertArrayEquals(expectation.toArray(),
      (Object[]) ((TimestreamArray) timestreamStruct.getAttributes()[0]).getArray());
    Assertions
      .assertEquals("[2.0, 3.0]", timestreamStruct.getAttributes(CONVERSION_MAP)[0]);
  }

  @Test
  void testGetAttributesWithTimeSeries() throws SQLException {
    initStructWithTimeSeries("2020-05-05 16:51:30.000000000", TimestreamDataType.VARCHAR);

    final List<TimeSeriesDataPoint> expectation = new ArrayList<>();
    expectation.add(new TimeSeriesDataPoint().withTime("2020-05-05 16:51:30.000000000")
      .withValue(new Datum().withScalarValue("2")));
    final String expectedString = "[{time: 2020-05-05 16:51:30.000000000, value: 2}]";
    Assertions.assertEquals(expectation, timestreamStruct.getAttributes()[0]);
    Assertions.assertEquals(expectedString, timestreamStruct.getAttributes(CONVERSION_MAP)[0]);
  }

  @Test
  void testGetAttributesWithMultiDataType() throws SQLException {
    final Object[] expectation = new Object[3];
    final List<Object> array = new ArrayList<>();
    final List<TimeSeriesDataPoint> time = new ArrayList<>();
    time.add(new TimeSeriesDataPoint().withTime("2020-05-05 16:51:30.000000000"));
    array.add(2);
    array.add(3);
    expectation[0] = 1.0;
    expectation[1] = array.toArray();
    expectation[2] = time;

    initStructWithMultiDataType();
    for (int i = 0; i < expectation.length; i++) {
      if (i == 1) {
        Assertions.assertArrayEquals(
          (Object[]) expectation[i],
          (Object[]) ((TimestreamArray) timestreamStruct.getAttributes()[i]).getArray());
      } else {
        Assertions.assertEquals(expectation[i], timestreamStruct.getAttributes()[i]);
      }
    }
  }

  @Test
  void testGetAttributesWithNestedArray() throws SQLException {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);

    final List<Datum> inputDatum = ImmutableList.of(
        new Datum().withRowValue(new Row()
            .withData(new Datum().withArrayValue(
                new Datum().withScalarValue("1.1"),
                new Datum().withScalarValue("2.2"))))
    );
    final List<ColumnInfo> baseTypeList = ImmutableList.of(
        new ColumnInfo().withType(new Type().withRowColumnInfo(
            new ColumnInfo().withType(new Type().withArrayColumnInfo(
                new ColumnInfo().withType(new Type()
                    .withScalarType(TimestreamDataType.DOUBLE.name()))))))
    );

    final Object[] expected = new Object[]{1, 2};
    final TimestreamStruct struct = new TimestreamStruct(inputDatum, baseTypeList, mockParentResultSet);

    int depth = 0;
    Object[] arr = struct.getAttributes(conversionMap);
    while (true) {
      if (arr[0] instanceof TimestreamStruct) {
        arr = ((TimestreamStruct) arr[0]).getAttributes();
        depth++;
      } else if (arr[0] instanceof TimestreamArray) {
        arr = (Object[]) ((TimestreamArray) arr[0]).getArray();
        depth++;
      } else {
        break;
      }
    }

    Assertions.assertEquals(2, depth);
    Assertions.assertArrayEquals(expected, arr);
  }

  @Test
  void testGetAttributesWithNestedTimeSeries() throws SQLException {
    final Datum datum = new Datum().withTimeSeriesValue(
        new TimeSeriesDataPoint()
            .withTime("2020-05-05 16:51:30.000000000")
            .withValue(new Datum().withScalarValue("82")));
    final List<Datum> inputDatum = ImmutableList
        .of(new Datum().withRowValue(new Row().withData(datum)));
    final List<ColumnInfo> baseTypeList = ImmutableList.of(
        new ColumnInfo().withType(new Type().withRowColumnInfo(
            new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
                new ColumnInfo().withType(new Type()
                    .withScalarType(TimestreamDataType.INTEGER.name()))))))
    );
    final TimestreamStruct struct = new TimestreamStruct(inputDatum, baseTypeList,
        mockParentResultSet);

    int depth = 0;
    Object[] nestedStruct = struct.getAttributes();
    while (nestedStruct[0] instanceof TimestreamStruct) {
      // Keep calling `getAttributes` until we have reached the inner most struct.
      nestedStruct = ((TimestreamStruct) nestedStruct[0]).getAttributes();
      depth++;
    }
    Assertions.assertEquals(1, depth);
    Assertions.assertEquals(datum.getTimeSeriesValue(), nestedStruct[0]);
  }

  @Test
  void testGetAttributesWithNestedTimeSeriesArray() throws SQLException {
    final Datum datum = new Datum().withTimeSeriesValue(
        new TimeSeriesDataPoint()
            .withTime("2020-05-05 16:51:30.000000000")
            .withValue(new Datum().withScalarValue("82")));
    final List<Datum> inputDatum = ImmutableList.of(
        new Datum().withRowValue(new Row()
            .withData(new Datum().withArrayValue(datum)))
    );
    final List<ColumnInfo> baseTypeList = ImmutableList.of(
        new ColumnInfo().withType(new Type().withRowColumnInfo(
            new ColumnInfo().withType(new Type().withArrayColumnInfo(
                new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
                    new ColumnInfo().withType(new Type()
                        .withScalarType(TimestreamDataType.INTEGER.name()))))))))
    );

    final TimestreamStruct struct = new TimestreamStruct(inputDatum, baseTypeList,
        mockParentResultSet);

    int depth = 0;
    Object[] nestedStruct = struct.getAttributes();
    while (true) {
      if (nestedStruct[0] instanceof TimestreamStruct) {
        nestedStruct = ((TimestreamStruct) nestedStruct[0]).getAttributes();
        depth++;
      } else if (nestedStruct[0] instanceof TimestreamArray) {
        nestedStruct = (Object[]) ((TimestreamArray) nestedStruct[0]).getArray();
        depth++;
      } else {
        break;
      }
    }
    Assertions.assertEquals(2, depth);
    Assertions.assertEquals(datum.getTimeSeriesValue(), nestedStruct[0]);
  }

  @Test
  void testToString() {
    initStructWithTimeSeries("2020-05-05 16:51:30.000000000", TimestreamDataType.VARCHAR);
    Assertions.assertEquals("([{time: 2020-05-05 16:51:30.000000000, value: 2}])", timestreamStruct.toString());
  }

  @Test
  void testGetAttributesWithString() throws SQLException {
    // This test is to ensure that no attributes are truncated in a row containing VARCHAR.
    final Object[] expectedAttributes = new Object[]{
      3.14,
      3,
      "string",
      true,
      "30 00:00:00.000000000"};

    final List<ColumnInfo> columnInfos = ImmutableList.of(
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())),
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())),
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.VARCHAR.name())),
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.BOOLEAN.name())),
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTERVAL_DAY_TO_SECOND.name()))
    );

    final List<Datum> structValues = Arrays
      .stream(expectedAttributes)
      .map(val -> new Datum().withScalarValue(String.valueOf(val)))
      .collect(Collectors.toList());

    final TimestreamStruct struct = new TimestreamStruct(
      structValues,
      columnInfos,
      mockParentResultSet);
    Assertions.assertArrayEquals(expectedAttributes, struct.getAttributes());
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#structToStringArguments")
  void testGetAttributesToString(
      final String expected,
      final List<Datum> structDatum,
      final List<ColumnInfo> baseTypeList,
      final Map<String, Class<?>> conversionMap) throws SQLException {
    final TimestreamStruct struct = new TimestreamStruct(structDatum, baseTypeList, mockParentResultSet);
    Assertions.assertEquals(expected, ((Object[]) struct.getAttributes(conversionMap))[0]);
  }

  /**
   * Initialize the {@link TimestreamStruct} with the scalarValue.
   *
   * @param value String representing the scalar value in the array.
   * @param type  The Timestream data type of the value.
   */
  private void initStructWithScalarValue(final String value, final TimestreamDataType type) {
    timestreamStruct = new TimestreamStruct(
      ImmutableList.of(new Datum().withScalarValue(value)),
      ImmutableList.of(new ColumnInfo().withType(new Type().withScalarType(type.name()))),
      mockParentResultSet);
  }

  /**
   * Initialize the {@link TimestreamStruct} with the scalarValue.
   *
   * @param array A list of Datum contained in the struct.
   * @param type  The Timestream data type of the value.
   */
  private void initStructWithArray(final List<Datum> array, final TimestreamDataType type) {
    timestreamStruct = new TimestreamStruct(
      ImmutableList.of(new Datum().withArrayValue(array)),
      ImmutableList.of(new ColumnInfo().withType(new Type().withArrayColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(type.name()))))),
      mockParentResultSet);
  }

  /**
   * Initialize the {@link TimestreamStruct} with the scalarValue.
   *
   * @param time String representing the time in the TimeSeries.
   * @param type The Timestream data type of the value.
   */
  private void initStructWithTimeSeries(final String time, final TimestreamDataType type) {
    timestreamStruct = new TimestreamStruct(
      ImmutableList.of(new Datum().withTimeSeriesValue(new TimeSeriesDataPoint()
        .withTime(time).withValue(new Datum().withScalarValue("2")))),
      ImmutableList.of(new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(type.name()))))),
      mockParentResultSet);
  }

  /**
   * Initialize the {@link TimestreamStruct} with the scalarValue, ARRAY, TimeSeries.
   */
  private void initStructWithMultiDataType() {
    final List<Datum> struct = ImmutableList.of(
      new Datum().withScalarValue("1"),
      new Datum().withArrayValue(
        ImmutableList.of(
          new Datum().withScalarValue("2"),
          new Datum().withScalarValue("3"))),
      new Datum().withTimeSeriesValue(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000"))
    );

    final List<ColumnInfo> columnInfos = ImmutableList.of(
      new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.DOUBLE.name())),
      new ColumnInfo().withType(new Type().withArrayColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())))),
      new ColumnInfo().withType(new Type().withTimeSeriesMeasureValueColumnInfo(
        new ColumnInfo()
          .withType(new Type().withScalarType(TimestreamDataType.VARCHAR.name()))))
    );

    timestreamStruct = new TimestreamStruct(
      struct,
      columnInfos,
      mockParentResultSet);
  }
}
