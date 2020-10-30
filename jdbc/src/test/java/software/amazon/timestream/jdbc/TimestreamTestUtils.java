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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.provider.Arguments;

import java.sql.Date;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class containing helper methods for Timestream unit tests.
 */
public class TimestreamTestUtils {
  /**
   * Assert that the given exception is thrown by the expected error.
   *
   * @param exception     The {@link java.sql.SQLException} thrown by the unit test.
   * @param expectedError String representing the expected {@link Error} causing the {@link
   *                      java.sql.SQLException}.
   */
  public static void validateException(final Exception exception, final String expectedError) {
    final String actualMessage = exception.getMessage();
    final boolean containsMessage = actualMessage.contains(expectedError);
    if (!containsMessage) {
      // Print the actual message if it is not the expected error message.
      System.out.println(actualMessage);
    }
    Assertions.assertTrue(containsMessage);
  }

  /**
   * Assert that the a {@link java.sql.SQLWarning} has been recorded.
   *
   * @param warning         The {@link java.sql.SQLWarning} recorded by the unit test.
   * @param expectedWarning The String representing the expected {@link Warning}.
   */
  public static void validateWarning(final SQLWarning warning, final String expectedWarning) {
    final String actualMessage = warning.getMessage();

    Assertions.assertTrue(actualMessage.contains(expectedWarning));
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetTimeFromDifferentValues(TimestreamDataType,
   * String, Time)}
   *
   * @return the stream of arguments containing the {@link TimestreamDataType} of the input value,
   * the input value, and their expected return value.
   */
  static Stream<Arguments> objectMapOnArrayArguments() {
    final List<Double> doubleList = ImmutableList
      .of(211479.92608786508, 1200.0, 176.23327173988756);
    final List<Integer> intList = ImmutableList.of(211479, 1200, 176);
    final String expectedArrayString = doubleList.toString();

    final Map<String, Class<?>> doubleToIntConversionMap = new HashMap<>();
    doubleToIntConversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);

    final Map<String, Class<?>> arrayToStringConversionMap = new HashMap<>();
    arrayToStringConversionMap.put(TimestreamDataType.ARRAY.name(), String.class);

    return Stream.of(
      Arguments.of(doubleList, intList.toArray(), doubleToIntConversionMap),
      Arguments.of(doubleList, expectedArrayString, arrayToStringConversionMap)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetStringFromDifferentValues(TimestreamDataType,
   * String, String)}
   *
   * @return the stream of arguments containing the {@link TimestreamDataType} of the input value,
   * the input scalar value, and their expected return string value.
   */
  static Stream<Arguments> stringParameters() {
    return Stream.of(
        Arguments.of(TimestreamDataType.VARCHAR, "Foo", "Foo"),
        Arguments.of(TimestreamDataType.BOOLEAN, "true", "true"),
        Arguments.of(TimestreamDataType.TIME, "2020-05-05", "2020-05-05"),
        Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-05", "2020-05-05"),
        Arguments.of(TimestreamDataType.INTERVAL_DAY_TO_SECOND, "1 00:00:00.000000000",
            "1 00:00:00.000000000")
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetTimeFromDifferentValues(TimestreamDataType,
   * String, Time)}
   *
   * @return the stream of arguments containing the {@link TimestreamDataType} of the input value,
   * the input value, and their expected return value.
   */
  static Stream<Arguments> timeParameters() {
    final Time expectedTime = Time.valueOf("22:21:55");
    final Time expectedTimeFromTimestamp = new Time(76915000L);

    return Stream.of(
      Arguments.of(TimestreamDataType.VARCHAR, "22:21:55.186000000", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.186000000", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.186123123", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.18612312", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.1861231", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.186123", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.18612", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.1861", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.18", expectedTime),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.1", expectedTime),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186123123",
        expectedTimeFromTimestamp),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.18612",
        expectedTimeFromTimestamp),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.18",
        expectedTimeFromTimestamp)
      );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetTimeFromDifferentValuesAndTimeZones(Calendar,
   * TimestreamDataType, String, long)}
   *
   * @return the stream of arguments containing the time zone, {@link TimestreamDataType} of the
   * input value, the input value, and their expected return value.
   */
  static Stream<Arguments> timeParametersWithTimezone() {
    final String input = "11:40:31.511000000";
    final long expectedTimeFromTimestamp =
      Timestamp.valueOf("2020-05-11 03:10:31.511000000").getTime();

    return Stream.of(
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("America/New_York")),
        TimestreamDataType.TIME,
        input,
        Time.valueOf("17:40:31").getTime(),
        Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo")),
          TimestreamDataType.TIME,
          input,
          Time.valueOf("03:40:31").getTime()),
        Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Africa/Cairo")),
          TimestreamDataType.TIME,
          input,
          Time.valueOf("10:40:31").getTime()),
        Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Australia/Darwin")),
          TimestreamDataType.TIMESTAMP,
          "2020-05-11 11:40:31.511000000",
          expectedTimeFromTimestamp))
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetTimestampFromDifferentValues(TimestreamDataType,
   * String, Timestamp)} String, Time)}
   *
   * @return the stream of arguments containing the {@link TimestreamDataType} of the input value,
   * the input value, and their expected return value.
   */
  static Stream<Arguments> timestampParameters() {
    final Timestamp expectedTimestampFullPrecision = Timestamp.valueOf("2020-05-19 22:21:55");
    expectedTimestampFullPrecision.setNanos(186123123);

    return Stream.of(
      Arguments.of(TimestreamDataType.VARCHAR, "2020-05-19 22:21:55.186123123",
        expectedTimestampFullPrecision),
      Arguments.of(TimestreamDataType.TIME, "22:21:55.186000",
        Timestamp.valueOf("1970-01-01 22:21:55.186")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.000000000",
        Timestamp.valueOf("2020-05-19 22:21:55.0")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186000000",
        Timestamp.valueOf("2020-05-19 22:21:55.186")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186123123",
        expectedTimestampFullPrecision),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.1861",
        Timestamp.valueOf("2020-05-19 22:21:55.1861")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.18",
        Timestamp.valueOf("2020-05-19 22:21:55.18")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55",
        Timestamp.valueOf("2020-05-19 22:21:55")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186123",
        Timestamp.valueOf("2020-05-19 22:21:55.186123")),
      Arguments.of(TimestreamDataType.DATE, "2020-05-19",
        Timestamp.valueOf("2020-05-19 00:00:00.0"))
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetTimestampFromDifferentValuesAndTimeZones(Calendar,
   * TimestreamDataType, String, long)}
   *
   * @return the stream of arguments containing the time zone, {@link TimestreamDataType} of the
   * input value, the input value, and their expected return value.
   */
  static Stream<Arguments> timestampParametersWithTimezone() {
    final String input = "2020-05-11 11:40:31.511000000";

    return Stream.of(
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("America/New_York")),
        TimestreamDataType.TIMESTAMP,
        input,
        Timestamp.valueOf("2020-05-11 17:40:31.511000000").getTime()),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo")),
        TimestreamDataType.TIMESTAMP,
        input,
        Timestamp.valueOf("2020-05-11 04:40:31.511000000").getTime()),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Africa/Cairo")),
        TimestreamDataType.TIMESTAMP,
        input,
        Timestamp.valueOf("2020-05-11 11:40:31.511000000").getTime()),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Australia/Darwin")),
        TimestreamDataType.TIMESTAMP,
        input,
        Timestamp.valueOf("2020-05-11 04:10:31.511000000").getTime())
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetDateFromDifferentValues(TimestreamDataType,
   * String, Date)}
   *
   * @return the stream of arguments containing the {@link TimestreamDataType} of the input value,
   * the input value, and their expected return value.
   */
  static Stream<Arguments> dateParameters() {
    final Date expectedDate = Date.valueOf("2020-05-19");
    return Stream.of(
        Arguments.of(TimestreamDataType.VARCHAR, "2020-05-19", expectedDate),
        Arguments.of(TimestreamDataType.VARCHAR, "2020-05-19 22:21:55.186123123", expectedDate),
        Arguments.of(TimestreamDataType.DATE, "2020-05-19", expectedDate),
        Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186000000",
            expectedDate),
        Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-19 22:21:55.186123123",
            expectedDate)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetDateFromDifferentTimeZones(Calendar,
   * TimestreamDataType, String, Date)}
   *
   * @return the stream of arguments containing the time zone, {@link TimestreamDataType} of the
   * input value, the input value, and their expected return value.
   */
  static Stream<Arguments> dateParametersWithTimezone() {
    return Stream.of(
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("America/New_York")),
        TimestreamDataType.DATE, "2020-05-11", new Date(1589169600000L)),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo")),
        TimestreamDataType.DATE, "2020-05-11", new Date(1589122800000L)),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Africa/Cairo")),
        TimestreamDataType.DATE, "2020-05-11", new Date(1589148000000L)),
      Arguments.of(Calendar.getInstance(TimeZone.getTimeZone("Australia/Darwin")),
        TimestreamDataType.DATE, "2020-05-11", new Date(1589121000000L))
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetObjectWithTimeSeries(List,
   * List)} and {@link TimestreamResultSetTest#testGetArrayFromTimeSeries(List, List)}.
   *
   * @return the stream of arguments containing the input value, and their expected return value.
   */
  static Stream<Arguments> timeSeriesArguments() {
    final List<TimeSeriesDataPoint> data = ImmutableList.of(new TimeSeriesDataPoint());
    final List<TimeSeriesDataPoint> dataWithTime = ImmutableList.of(new TimeSeriesDataPoint()
      .withTime("2020-05-05 16:51:30.000000000"));
    final List<TimeSeriesDataPoint> dataWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));
    final List<TimeSeriesDataPoint> dataWithTimeAndArray = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withArrayValue(new Datum().withScalarValue("82.77"))));

    final List<TimeSeriesDataPoint> dataWithTimeAndRow = ImmutableList.of(new TimeSeriesDataPoint()
      .withTime("2020-05-05 16:51:30.000000000")
      .withValue(
        new Datum().withRowValue(new Row().withData(new Datum().withScalarValue("82.77")))));
    final List<TimeSeriesDataPoint> dataWithTimeAndTimeseries = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withTimeSeriesValue(new TimeSeriesDataPoint()
          .withTime("2020-05-05 16:51:30.000000000")
          .withValue(new Datum().withNullValue(Boolean.TRUE)))));
    final List<TimeSeriesDataPoint> dataWithTimeAndNullValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withNullValue(Boolean.TRUE)));

    return Stream.of(
      Arguments.of(data, data),
      Arguments.of(dataWithTime, dataWithTime),
      Arguments.of(dataWithTimeAndValue, dataWithTimeAndValue),
      Arguments.of(dataWithTimeAndArray, dataWithTimeAndArray),
      Arguments.of(dataWithTimeAndRow, dataWithTimeAndRow),
      Arguments.of(dataWithTimeAndTimeseries, dataWithTimeAndTimeseries),
      Arguments.of(dataWithTimeAndNullValue, dataWithTimeAndNullValue)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamResultSetTest#testGetStringFromTimeSeries(List,
   * String)}.
   *
   * @return the stream of arguments containing the input value, and their expected return value.
   */
  static Stream<Arguments> timeSeriesStringArguments() {
    final List<TimeSeriesDataPoint> dataWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));
    final List<TimeSeriesDataPoint> dataWithTimeAndArray = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withArrayValue(new Datum().withScalarValue("82.77"))));

    final List<TimeSeriesDataPoint> dataWithTimeAndRow = ImmutableList.of(new TimeSeriesDataPoint()
      .withTime("2020-05-05 16:51:30.000000000")
      .withValue(
        new Datum().withRowValue(new Row().withData(new Datum().withScalarValue("82.77")))));
    final List<TimeSeriesDataPoint> dataWithTimeAndTimeSeries = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withTimeSeriesValue(new TimeSeriesDataPoint()
          .withTime("2020-05-05 16:51:30.000000000")
          .withValue(new Datum().withNullValue(Boolean.TRUE)))));
    final List<TimeSeriesDataPoint> dataWithTimeAndNullValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withNullValue(Boolean.TRUE)));

    return Stream.of(
      Arguments.of(
        dataWithTimeAndValue,
        "[{time: 2020-05-05 16:51:30.000000000, value: 82.77}]"),
      Arguments.of(
        dataWithTimeAndArray,
        "[{time: 2020-05-05 16:51:30.000000000, value: [82.77]}]"),
      Arguments.of(
        dataWithTimeAndRow,
        "[{time: 2020-05-05 16:51:30.000000000, value: (82.77)}]"),
      Arguments.of(
        dataWithTimeAndTimeSeries,
        "[{time: 2020-05-05 16:51:30.000000000, value: [{time: 2020-05-05 16:51:30.000000000, value: null}]}]"),
      Arguments.of(
        dataWithTimeAndNullValue,
        "[{time: 2020-05-05 16:51:30.000000000, value: null}]")
    );
  }

  /**
   * A stream of arguments and their expected results.
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> arrayArguments() {
    final List<Double> doubles = ImmutableList
      .of(211479.92608786508, 1200.0, 176.23327173988756);
    final Object[] expectedDoubles = doubles.toArray();
    final Object[][] expectedNestedDoubles = new Object[][]{expectedDoubles};

    final List<Datum> doubleValues = createInputDatumList(doubles);

    final Datum arrayValues = new Datum().withArrayValue(doubleValues);

    final List<Object> nestedDoubleValues = new ArrayList<>();
    nestedDoubleValues.add(arrayValues);

    final List<TimeSeriesDataPoint> timeSeries = ImmutableList.of(new TimeSeriesDataPoint());
    final List<TimeSeriesDataPoint> timeSeriesWithTime = ImmutableList.of(new TimeSeriesDataPoint()
      .withTime("2020-05-05 16:51:30.000000000"));
    final List<TimeSeriesDataPoint> timeSeriesWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.77")));

    return Stream.of(
      Arguments.of(
        createScalarType(TimestreamDataType.DOUBLE),
        doubleValues,
        expectedDoubles),
      Arguments.of(
        createArrayType(TimestreamDataType.DOUBLE),
        nestedDoubleValues,
        expectedNestedDoubles),
      Arguments.of(
        createTimeSeriesType(TimestreamDataType.DOUBLE),
        timeSeries,
        ImmutableList.of(timeSeries).toArray()),
      Arguments.of(
        createTimeSeriesType(TimestreamDataType.DOUBLE),
        timeSeriesWithTime,
        ImmutableList.of(timeSeriesWithTime).toArray()),
      Arguments.of(
        createTimeSeriesType(TimestreamDataType.DOUBLE),
        timeSeriesWithTimeAndValue,
        ImmutableList.of(timeSeriesWithTimeAndValue).toArray())
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamArrayTest#testGetArrayWithMap(Type,
   * List, Object[], Map)}.
   *
   * @return the stream of arguments containing the input data type, the input value, their expected
   * return value, and a conversion map.
   */
  static Stream<Arguments> arrayMapArguments() {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);
    conversionMap.put(TimestreamDataType.TIMESERIES.name(), String.class);

    final List<Double> doubleList = ImmutableList
      .of(211479.92608786508, 1200.0, 176.23327173988756);
    final List<Datum> doubleValues = createInputDatumList(doubleList);

    final Datum arrayValues = new Datum().withArrayValue(doubleValues);

    final List<Object> nestedDoubleValues = new ArrayList<>();
    nestedDoubleValues.add(arrayValues);

    final List<Integer> intList = ImmutableList.of(211479, 1200, 176);
    final Object[][] expectedNestIntArray = new Object[][]{intList.toArray()};

    final List<TimeSeriesDataPoint> timeSeriesWithTimeAndValue = ImmutableList
      .of(new TimeSeriesDataPoint()
        .withTime("2020-05-05 16:51:30.000000000")
        .withValue(new Datum().withScalarValue("82.70")));

    final List<String> stringList = ImmutableList
      .of("[{time: 2020-05-05 16:51:30.000000000, value: 82.70}]");

    return Stream.of(
      Arguments.of(
        createScalarType(TimestreamDataType.DOUBLE),
        createInputDatumList(doubleList),
        intList.toArray(),
        conversionMap),
      Arguments.of(
        createTimeSeriesType(TimestreamDataType.INTEGER),
        timeSeriesWithTimeAndValue,
        stringList.toArray(),
        conversionMap),
      Arguments.of(
        createArrayType(TimestreamDataType.DOUBLE),
        nestedDoubleValues,
        expectedNestIntArray,
        conversionMap)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamArrayTest#testGetArrayWithIndex(Type,
   * List, Object[], int, int)})}.
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> indexArguments() {
    final List<Double> expectedDoubles = ImmutableList
      .of(211479.92608786508, 1200.0, 176.23327173988756);
    final List<Object> doubleValues = expectedDoubles
      .stream()
      .map(val -> new Datum().withScalarValue(String.valueOf(val)))
      .collect(Collectors.toList());

    final Type expectedType = createScalarType(TimestreamDataType.DOUBLE);

    return Stream.of(
      Arguments.of(expectedType, doubleValues, expectedDoubles.subList(0, 1).toArray(), 1, 0),
      Arguments.of(expectedType, doubleValues, expectedDoubles.subList(0, 2).toArray(), 1, 1),
      Arguments.of(expectedType, doubleValues, expectedDoubles.subList(0, 3).toArray(), 1, 2),
      Arguments.of(expectedType, doubleValues, expectedDoubles.subList(1, 2).toArray(), 2, 0),
      Arguments.of(expectedType, doubleValues, expectedDoubles.subList(2, 2).toArray(), 3, -1)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamArrayTest#testGetArrayToString(String,
   * Type, List, Map)}, which includes cases with nestedArray, row in array and timeSeries object in
   * array.
   *
   * @return the stream of arguments containing the expected output string, the input type, the
   * input list and the corresponding conversion map.
   */
  static Stream<Arguments> arrayToStringArguments() {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(TimestreamDataType.ARRAY.name(), String.class);
    conversionMap.put(TimestreamDataType.INTEGER.name(), Double.class);
    conversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);

    final List<Object> nestedArrayList = ImmutableList.of(
        new Datum().withArrayValue(
            new Datum().withArrayValue(
                new Datum().withScalarValue("1"),
                new Datum().withScalarValue("2")))
    );
    final Type nestedArrayType =
        new Type().withArrayColumnInfo(
            new ColumnInfo().withType(new Type().withArrayColumnInfo(
                new ColumnInfo()
                    .withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())))));

    final List<Object> nestedRowList = ImmutableList.of(
        new Datum()
            .withArrayValue(new Datum()
                .withRowValue(new Row()
                    .withData(
                        new Datum().withScalarValue("1.1"),
                        new Datum().withScalarValue("2.2"))))
    );
    final Type nestedRowType = new Type()
        .withArrayColumnInfo(new ColumnInfo()
        .withType(new Type()
            .withRowColumnInfo(new ColumnInfo()
                .withType(new Type()
                    .withScalarType(TimestreamDataType.DOUBLE.name())))));

    final List<Object> nestedTimeSeriesList = ImmutableList.of(
        new Datum()
            .withArrayValue(new Datum()
                .withTimeSeriesValue(new TimeSeriesDataPoint()
                    .withTime("2020-05-05 16:51:30.000000000")
                    .withValue(new Datum().withScalarValue("82"))))
    );
    final Type nestedTimeSeriesType = new Type()
        .withArrayColumnInfo(new ColumnInfo()
            .withType(new Type().withTimeSeriesMeasureValueColumnInfo(new ColumnInfo()
                .withType(new Type().withScalarType(TimestreamDataType.INTEGER.name())))));

    final String nestedArrayExpected = "[[1.0, 2.0]]";
    final String nestedRowExpected = "[(1, 2)]";
    final String nestedTimeSeriesExpected = "[[{Time: 2020-05-05 16:51:30.000000000,Value: {ScalarValue: 82,}}]]";

    return Stream.of(
        Arguments.of(nestedArrayExpected, nestedArrayType, nestedArrayList, conversionMap),
        Arguments.of(nestedRowExpected, nestedRowType, nestedRowList, conversionMap),
        Arguments.of(nestedTimeSeriesExpected, nestedTimeSeriesType, nestedTimeSeriesList, conversionMap)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamStructTest#testGetAttributesToString(String,
   * List, List, Map)}, which includes cases with nestedRow, array in row.
   *
   * @return the stream of arguments containing the expected output string, the input type, the
   * input list and the corresponding conversion map.
   */
  static Stream<Arguments> structToStringArguments() {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(TimestreamDataType.ROW.name(), String.class);
    conversionMap.put(TimestreamDataType.INTEGER.name(), Double.class);
    conversionMap.put(TimestreamDataType.DOUBLE.name(), Integer.class);

    final List<Datum> nestedRowDatum = ImmutableList.of(
        new Datum().withRowValue(new Row()
            .withData(
                new Datum().withScalarValue("1"),
                new Datum().withScalarValue("2")))
    );
    final List<ColumnInfo> nestedRowColInfo = ImmutableList.of(
        new ColumnInfo().withType(new Type().withRowColumnInfo(
        new ColumnInfo().withType(new Type().withScalarType(TimestreamDataType.INTEGER.name()))))
    );
    final String nestedRowExpected = "(1.0, 2.0)";

    final List<Datum> nestedArrayDatum = ImmutableList.of(
        new Datum().withRowValue(new Row()
            .withData(new Datum()
                .withArrayValue(
                    new Datum().withScalarValue("1.2"),
                    new Datum().withScalarValue("2.3"))))
    );
    final List<ColumnInfo> nestedArrayColInfo = ImmutableList.of(
        new ColumnInfo().withType(new Type().withRowColumnInfo(
            new ColumnInfo().withType(new Type().withArrayColumnInfo(
                new ColumnInfo().withType(new Type()
                    .withScalarType(TimestreamDataType.DOUBLE.name()))))))
    );
    final String nestedArrayExpected = "([1, 2])";

    return Stream.of(
        Arguments.of(nestedRowExpected, nestedRowDatum, nestedRowColInfo, conversionMap),
        Arguments.of(nestedArrayExpected, nestedArrayDatum, nestedArrayColInfo, conversionMap)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamArrayTest#testGetArrayWithMapAndIndex(Type,
   * List, Object[], int, int, Map)})} and {@link TimestreamArrayTest#testGetResultSetWithMapAndIndex(Type,
   * List, Object[], int, int, Map)}.
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> indexMapArguments() {
    final Map<String, Class<?>> conversionMap = new HashMap<>();
    conversionMap.put(JdbcType.DOUBLE.name(), Integer.class);

    final List<Double> inputDouble = ImmutableList
      .of(211479.92608786508, 1200.0, 176.23327173988756);

    final List<Integer> intList = ImmutableList.of(211479, 1200, 176);

    final List<Object> doubleValues = inputDouble
      .stream()
      .map(val -> new Datum().withScalarValue(String.valueOf(val)))
      .collect(Collectors.toList());

    final Type expectedType = createScalarType(TimestreamDataType.DOUBLE);

    return Stream.of(
      Arguments.of(expectedType, doubleValues, intList.subList(0, 1).toArray(), 1, 0, conversionMap),
      Arguments.of(expectedType, doubleValues, intList.subList(0, 2).toArray(), 1, 1, conversionMap),
      Arguments.of(expectedType, doubleValues, intList.subList(0, 3).toArray(), 1, 2, conversionMap),
      Arguments.of(expectedType, doubleValues, intList.subList(1, 2).toArray(), 2, 0, conversionMap),
      Arguments.of(expectedType, doubleValues, intList.subList(2, 2).toArray(), 3, -1, conversionMap)
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamArrayTest#testGetArrayWithDateTime(Type,
   * List, Object[])}.
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> arrayDateTimeArguments() {
    final List<String> inputDate = ImmutableList.of("2020-05-11");
    final List<Date> expectedDate = ImmutableList.of(Date.valueOf("2020-05-11"));
    final List<Time> expectedTime = ImmutableList.of(
      Time.valueOf("16:51:30"),
      Time.valueOf("16:51:30"),
      Time.valueOf("16:51:30"));
    final List<Timestamp> expectedTimestamp = ImmutableList.of(
      Timestamp.valueOf("2020-05-05 16:51:30.123"),
      Timestamp.valueOf("2020-05-05 16:51:30.123"),
      Timestamp.valueOf("2020-05-05 16:51:30.123"));

    return Stream.of(
      Arguments.of(createScalarType(TimestreamDataType.DATE), createInputDatumList(inputDate),
        expectedDate.toArray()),
      Arguments.of(createScalarType(TimestreamDataType.TIME), createInputDatumList(expectedTime),
        expectedTime.toArray()),
      Arguments
        .of(createScalarType(TimestreamDataType.TIMESTAMP), createInputDatumList(expectedTimestamp),
          expectedTimestamp.toArray())
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamStructTest#testGetAttributesWithScalarValue(TimestreamDataType,
   * String, Object)}
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> structScalarValuesArguments() {
    return Stream.of(
      Arguments.of(TimestreamDataType.DOUBLE, "3.14", 3.14),
      Arguments.of(TimestreamDataType.VARCHAR, "1.2", "1.2"),
      Arguments.of(TimestreamDataType.DATE, "2020-05-11", Date.valueOf("2020-05-11")),
      Arguments.of(TimestreamDataType.TIME, "16:51:30", Time.valueOf("16:51:30")),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-05 16:51:30.123",
        Timestamp.valueOf("2020-05-05 16:51:30.123"))
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamStructTest#testGetAttributesWithScalarValueAndConversionMap(TimestreamDataType,
   * String, Object)}
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> structScalarValuesConversionMapArguments() {
    return Stream.of(
      Arguments.of(TimestreamDataType.DOUBLE, "3.14", "3.14"),
      Arguments.of(TimestreamDataType.VARCHAR, "1.2", 1.2),
      Arguments.of(TimestreamDataType.DATE, "2020-05-11", "2020-05-11"),
      Arguments
        .of(TimestreamDataType.TIME, "16:51:30", new Timestamp(Time.valueOf("16:51:30").getTime())),
      Arguments.of(TimestreamDataType.TIMESTAMP, "2020-05-05 16:51:30.123",
        Time.valueOf(LocalDateTime.parse("2020-05-05T16:51:30.123").toLocalTime()))
    );
  }

  /**
   * A stream of arguments and their expected results for {@link TimestreamDatabaseMetaDataTest#testGetURL(Properties,
   * String)}
   *
   * @return the stream of arguments containing the input data type, the input value, and their
   * expected return value.
   */
  static Stream<Arguments> urlArguments() {
    final Properties inputWithIamCredentialsProperties = new Properties();
    inputWithIamCredentialsProperties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    inputWithIamCredentialsProperties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "foo");
    inputWithIamCredentialsProperties.put(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(), "foo");

    final Properties inputWithOktaProperties = new Properties();
    inputWithOktaProperties.put(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(), "Okta");
    inputWithOktaProperties.put(TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(), "aws_role");
    inputWithOktaProperties.put(TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(), "idp_arn");
    inputWithOktaProperties.put(TimestreamConnectionProperty.OKTA_APP_ID.getConnectionProperty(), "okta_app_id");
    inputWithOktaProperties.put(TimestreamConnectionProperty.IDP_HOST.getConnectionProperty(), "okta_host");
    inputWithOktaProperties.put(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(), "idp_user_name");
    inputWithOktaProperties.put(TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(), "pwd");

    final Properties inputWithAadProperties = new Properties();
    inputWithAadProperties.put(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(), "AzureAD");
    inputWithAadProperties.put(TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(), "aws_role");
    inputWithAadProperties.put(TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(), "idp_arn");
    inputWithAadProperties.put(TimestreamConnectionProperty.AAD_TENANT_ID.getConnectionProperty(), "tenant_id");
    inputWithAadProperties.put(TimestreamConnectionProperty.AAD_APP_ID.getConnectionProperty(), "app_id");
    inputWithAadProperties.put(TimestreamConnectionProperty.AAD_CLIENT_SECRET.getConnectionProperty(), "secret");
    inputWithAadProperties.put(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(), "idp_user_name");
    inputWithAadProperties.put(TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(), "pwd");

    final Properties inputWithEmptyProperties = new Properties();

    final Properties inputWithRegionProperties = new Properties();
    inputWithRegionProperties.put(TimestreamConnectionProperty.REGION.getConnectionProperty(), "ca-central-1");

    return Stream.of(
      Arguments.of(
        inputWithIamCredentialsProperties,
        "jdbc:timestream://AccessKeyId=***Sensitive Data Redacted***;SecretAccessKey=***Sensitive Data Redacted***;SessionToken=***Sensitive Data Redacted***;Region=us-east-1"
      ),
      Arguments.of(
        inputWithOktaProperties,
        "jdbc:timestream://IdpName=Okta;IdpHost=okta_host;IdpUserName=***Sensitive Data Redacted***;IdpPassword=***Sensitive Data Redacted***;OktaApplicationID=okta_app_id;RoleARN=***Sensitive Data Redacted***;IdpARN=***Sensitive Data Redacted***;Region=us-east-1"
      ),
      Arguments.of(
        inputWithAadProperties,
        "jdbc:timestream://IdpName=AzureAD;IdpUserName=***Sensitive Data Redacted***;IdpPassword=***Sensitive Data Redacted***;RoleARN=***Sensitive Data Redacted***;IdpARN=***Sensitive Data Redacted***;AADApplicationID=app_id;AADClientSecret=***Sensitive Data Redacted***;AADTenant=***Sensitive Data Redacted***;Region=us-east-1"
      ),
      Arguments.of(inputWithEmptyProperties, "jdbc:timestream://Region=us-east-1"),
      Arguments.of(inputWithRegionProperties, "jdbc:timestream://Region=ca-central-1")
    );
  }

  /**
   * A stream of {@link Properties} containing invalid SDK options for {@link
   * TimestreamConnectionTest#testConnectionWithInvalidSDKOptions(Properties)} that will cause
   * exceptions.
   *
   * @return a stream of {@link Properties} containing invalid SDK options.
   */
  static Stream<Properties> invalidSdkOptions() {
    final Properties invalidSocketTimeout = new Properties();
    invalidSocketTimeout.put(
      TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(),
      "-1");

    final Properties invalidMaxConnections = new Properties();
    invalidMaxConnections.put(
      TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(),
      "-1");

    final Properties invalidMaxRetryCount = new Properties();
    invalidMaxRetryCount.put(
      TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(),
      "-1");

    return Stream.of(
      invalidSocketTimeout,
      invalidMaxConnections,
      invalidMaxRetryCount);
  }

  /**
   * A stream of {@link Properties} containing invalid SDK options for {@link
   * TimestreamConnectionTest#testConnectionWithNonNumericSDKOptions(Properties)} that will cause
   * exceptions.
   *
   * @return a stream of {@link Properties} containing invalid SDK options.
   */
  static Stream<Properties> nonNumericSdkOptions() {
    final Properties invalidSocketTimeout = new Properties();
    invalidSocketTimeout.put(
      TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(),
      "foo");

    final Properties invalidMaxConnections = new Properties();
    invalidMaxConnections.put(
      TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(),
      "foo");

    final Properties invalidMaxRetryCount = new Properties();
    invalidMaxRetryCount.put(
      TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(),
      "foo");


    final Properties invalidRequestTimeout = new Properties();
    invalidRequestTimeout.put(
      TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(),
      "foo");

    return Stream.of(
      invalidSocketTimeout,
      invalidMaxConnections,
      invalidMaxRetryCount,
      invalidRequestTimeout);
  }

  /**
   * Creates a {@link Type} containing a scalar value from the given {@link TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType} to convert into a {@link Type}.
   * @return The {@link Type}.
   */
  static Type createScalarType(final TimestreamDataType type) {
    return new Type().withScalarType(type.name());
  }

  /**
   * Creates a row {@link Type} containing a scalar value from the given {@link
   * TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType} to convert into a {@link Type}.
   * @return The {@link Type}.
   */
  static Type createRowType(final TimestreamDataType type) {
    return new Type().withRowColumnInfo(
      new ColumnInfo().withType(new Type().withScalarType(type.name())));
  }

  /**
   * Creates a TimeSeries {@link Type} containing a scalar value of the given {@link
   * TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType} to convert into a {@link Type}.
   * @return The {@link Type}.
   */
  static Type createTimeSeriesType(final TimestreamDataType type) {
    return new Type().withTimeSeriesMeasureValueColumnInfo(
      new ColumnInfo().withType(new Type().withScalarType(type.name())));
  }

  /**
   * Creates am array {@link Type} containing a scalar value from the given {@link
   * TimestreamDataType}.
   *
   * @param type The {@link TimestreamDataType} to convert into a {@link Type}.
   * @return The {@link Type}.
   */
  static Type createArrayType(final TimestreamDataType type) {
    return new Type()
      .withArrayColumnInfo(new ColumnInfo().withType(new Type().withScalarType(type.name())));
  }

  /**
   * Creates a list of {@link Datum} containing values from the given list of expected output.
   *
   * @param expectedList A list containing expected output for a unit test.
   * @return a list of {@link Datum}.
   */
  static List<Datum> createInputDatumList(List<?> expectedList) {
    return expectedList
      .stream()
      .map(val -> new Datum().withScalarValue(String.valueOf(val)))
      .collect(Collectors.toList());
  }
}
