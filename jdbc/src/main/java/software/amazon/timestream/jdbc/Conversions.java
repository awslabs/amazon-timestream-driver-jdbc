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

import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.EnumMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * Class containing conversions from Timestream data types to Java data types.
 */
class Conversions {
  private static final Logger LOGGER = LoggerFactory.getLogger(Conversions.class);
  static final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> CONVERSIONS =
    Conversions.populateConversionMap();

  private static final BigDecimal BYTE_MIN = new BigDecimal(Byte.MIN_VALUE);
  private static final BigDecimal BYTE_MAX = new BigDecimal(Byte.MAX_VALUE);

  private static final BigDecimal SHORT_MIN = new BigDecimal(Short.MIN_VALUE);
  private static final BigDecimal SHORT_MAX = new BigDecimal(Short.MAX_VALUE);

  private static final BigDecimal INT_MIN = new BigDecimal(Integer.MIN_VALUE);
  private static final BigDecimal INT_MAX = new BigDecimal(Integer.MAX_VALUE);

  private static final BigDecimal LONG_MIN = new BigDecimal(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX = new BigDecimal(Long.MAX_VALUE);

  /**
   * Populate a {@link EnumMap} with lambdas that converts data at a cell from a source data type to
   * a target data type.
   *
   * @return a {@link EnumMap} populated with conversion lambdas.
   */
  static Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> populateConversionMap() {
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> conversionMap =
      new EnumMap<>(TimestreamDataType.class);

    populateIntConversions(conversionMap);
    populateDoubleConversions(conversionMap);
    populateBooleanConversions(conversionMap);
    populateLongConversions(conversionMap);
    populateDateConversions(conversionMap);
    populateTimeConversions(conversionMap);
    populateTimestampConversions(conversionMap);
    populateStringConversions(conversionMap);
    populateArrayConversions(conversionMap);
    populateRowConversions(conversionMap);
    populateTimeSeriesConversions(conversionMap);
    populateIntervalConversions(conversionMap);

    return conversionMap;
  }

  /**
   * Add common numeric conversions to the given set of conversions.
   *
   * @param conversions The existing set of conversions to add to.
   */
  private static void addCommonNumericConversions(
    Map<JdbcType, TimestreamConvertFunction<?>> conversions) {
    conversions.put(JdbcType.TINYINT, (data, callback) -> Byte.valueOf(data.getScalarValue()));
    conversions
      .put(JdbcType.SMALLINT, (data, callback) -> Short.valueOf(data.getScalarValue()));
    conversions
      .put(JdbcType.INTEGER, (data, callback) -> Integer.valueOf(data.getScalarValue()));
    conversions.put(JdbcType.BIGINT, (data, callback) -> Long.valueOf(data.getScalarValue()));
    conversions.put(JdbcType.DOUBLE, (data, callback) -> Double.valueOf(data.getScalarValue()));
    conversions.put(JdbcType.FLOAT, (data, callback) -> Float.valueOf(data.getScalarValue()));
    conversions
      .put(JdbcType.DECIMAL, (data, callback) -> new BigDecimal(data.getScalarValue()));
    conversions.put(JdbcType.VARCHAR, (data, callback) -> data.getScalarValue());
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#INTEGER} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateIntConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> intConversions = new EnumMap<>(
      JdbcType.class);

    addCommonNumericConversions(intConversions);
    intConversions.put(JdbcType.BOOLEAN,
      (data, callback) -> Integer.parseInt(data.getScalarValue()) != 0);

    map.put(TimestreamDataType.INTEGER, intConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#DOUBLE} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateDoubleConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> doubleConversions = new EnumMap<>(
      JdbcType.class);

    addCommonNumericConversions(doubleConversions);
    doubleConversions.put(JdbcType.BOOLEAN,
      (data, callback) -> Double.parseDouble(data.getScalarValue()) != 0.0);
    doubleConversions.put(JdbcType.TINYINT, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        BYTE_MIN,
        BYTE_MAX,
        JDBCType.TINYINT);

      if (value.stripTrailingZeros().scale() > 0) {
        callback.accept(new SQLWarning(Warning.lookup(
          Warning.VALUE_TRUNCATED,
          TimestreamDataType.DOUBLE,
          JdbcType.TINYINT)));
      }

      return value.byteValue();
    });
    doubleConversions.put(JdbcType.SMALLINT, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        SHORT_MIN,
        SHORT_MAX,
        JDBCType.SMALLINT);

      if (value.stripTrailingZeros().scale() > 0) {
        callback.accept(new SQLWarning(Warning.lookup(
          Warning.VALUE_TRUNCATED,
          TimestreamDataType.DOUBLE,
          JdbcType.SMALLINT)));
      }
      return value.shortValue();
    });
    doubleConversions.put(JdbcType.INTEGER, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        INT_MIN,
        INT_MAX,
        JDBCType.INTEGER);

      if (value.stripTrailingZeros().scale() > 0) {
        callback.accept(new SQLWarning(Warning.lookup(
          Warning.VALUE_TRUNCATED,
          TimestreamDataType.DOUBLE,
          JdbcType.INTEGER)));
      }

      return value.intValue();
    });
    doubleConversions.put(JdbcType.BIGINT, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        LONG_MIN,
        LONG_MAX,
        JDBCType.BIGINT);

      if (value.stripTrailingZeros().scale() > 0) {
        callback.accept(new SQLWarning(Warning.lookup(
          Warning.VALUE_TRUNCATED,
          TimestreamDataType.DOUBLE,
          JdbcType.BIGINT)));
      }
      return value.longValue();
    });
    map.put(TimestreamDataType.DOUBLE, doubleConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#BOOLEAN} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateBooleanConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> conversions = new EnumMap<>(
      JdbcType.class);

    conversions
      .put(JdbcType.BOOLEAN, (data, callback) -> Boolean.valueOf(data.getScalarValue()));
    conversions.put(JdbcType.TINYINT,
      (data, callback) -> (byte) (Boolean.parseBoolean(data.getScalarValue()) ? 1 : 0));
    conversions.put(JdbcType.SMALLINT,
      (data, callback) -> (short) (Boolean.parseBoolean(data.getScalarValue()) ? 1 : 0));
    conversions.put(JdbcType.INTEGER,
      (data, callback) -> Boolean.parseBoolean(data.getScalarValue()) ? 1 : 0);
    conversions.put(JdbcType.BIGINT,
      (data, callback) -> Boolean.parseBoolean(data.getScalarValue()) ? 1L : 0L);
    conversions.put(JdbcType.FLOAT,
      (data, callback) -> Boolean.parseBoolean(data.getScalarValue()) ? 1f : 0f);
    conversions.put(JdbcType.DOUBLE,
      (data, callback) -> Boolean.parseBoolean(data.getScalarValue()) ? 1d : 0d);
    conversions.put(JdbcType.DECIMAL,
      (data, callback) -> new BigDecimal(
        Boolean.parseBoolean(data.getScalarValue()) ? 1 : 0));
    conversions.put(JdbcType.VARCHAR, (data, callback) -> data.getScalarValue());

    map.put(TimestreamDataType.BOOLEAN, conversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#BIGINT} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateLongConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> longConversions = new EnumMap<>(
      JdbcType.class);

    addCommonNumericConversions(longConversions);

    longConversions.put(JdbcType.TINYINT, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        BYTE_MIN,
        BYTE_MAX,
        JDBCType.TINYINT);

      return value.byteValue();
    });
    longConversions.put(JdbcType.SMALLINT, (data, callback) -> {
      final BigDecimal value = validateValueRange(
        data.getScalarValue(),
        SHORT_MIN,
        SHORT_MAX,
        JDBCType.SMALLINT);

      return value.shortValue();
    });
    longConversions
      .put(JdbcType.BOOLEAN, (data, callback) -> Long.parseLong(data.getScalarValue()) != 0);

    map.put(TimestreamDataType.BIGINT, longConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#DATE} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateDateConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> dateConversions = new EnumMap<>(
      JdbcType.class);

    dateConversions
      .put(JdbcType.DATE, (data, callback) -> Date.valueOf(data.getScalarValue()));
    dateConversions.put(JdbcType.TIMESTAMP,
      (data, callback) -> Timestamp.valueOf(LocalDate
        .parse(data.getScalarValue())
        .atTime(0, 0, 0)));
    dateConversions.put(JdbcType.VARCHAR, (data, callback) -> data.getScalarValue());

    map.put(TimestreamDataType.DATE, dateConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#TIME} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateTimeConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> timeConversions = new EnumMap<>(
      JdbcType.class);

    timeConversions
      .put(JdbcType.TIME, (data, callback) -> Time.valueOf(LocalTime.parse(data.getScalarValue())));
    timeConversions.put(JdbcType.TIMESTAMP,
      (data, callback) -> Timestamp.valueOf(
        LocalDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.parse(data.getScalarValue()))));
    timeConversions.put(JdbcType.VARCHAR, (data, callback) -> data.getScalarValue());

    map.put(TimestreamDataType.TIME, timeConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#TIMESTAMP} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateTimestampConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> timestampConversions = new EnumMap<>(
      JdbcType.class);

    timestampConversions.put(JdbcType.DATE,
      (data, callback) -> Date.valueOf(LocalDateTime
        .parse(data.getScalarValue(), Constants.DATE_TIME_FORMATTER)
        .toLocalDate()));
    timestampConversions.put(JdbcType.TIME,
      (data, callback) -> Time.valueOf(LocalDateTime
        .parse(data.getScalarValue(), Constants.DATE_TIME_FORMATTER)
        .toLocalTime()));
    timestampConversions
      .put(JdbcType.TIMESTAMP, ((data, callback) -> Timestamp.valueOf(
        LocalDateTime.parse(data.getScalarValue(), Constants.DATE_TIME_FORMATTER))));
    timestampConversions.put(JdbcType.VARCHAR, (data, callback) -> data.getScalarValue());

    map.put(TimestreamDataType.TIMESTAMP, timestampConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#VARCHAR} to a supported data type.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateStringConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> stringConversions = new EnumMap<>(
      JdbcType.class);

    addCommonNumericConversions(stringConversions);
    stringConversions
        .put(JdbcType.BOOLEAN, (data, callback) -> Boolean.parseBoolean(data.getScalarValue()));
    stringConversions
        .put(JdbcType.DATE, (data, callback) -> Date.valueOf(LocalDate.parse(data.getScalarValue(), Constants.DATE_FORMATTER)));
    stringConversions
        .put(JdbcType.TIME, (data, callback) -> Time.valueOf(LocalTime.parse(data.getScalarValue(), Constants.TIME_FORMATTER)));
    stringConversions
        .put(JdbcType.TIMESTAMP, (data, callback) -> Timestamp.valueOf(LocalDateTime.parse(data.getScalarValue(), Constants.DATE_TIME_FORMATTER)));

    map.put(TimestreamDataType.VARCHAR, stringConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that converts data at a cell from a supported
   * data type to a {@link TimestreamDataType#ARRAY}.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateArrayConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> arrayConversions = new EnumMap<>(
      JdbcType.class);
    arrayConversions.put(JdbcType.ARRAY, (data, callback) -> data.getArrayValue());
    arrayConversions.put(JdbcType.VARCHAR, (data, callback) -> {
      final StringJoiner result = new StringJoiner(Constants.DELIMITER, "[", "]");
      for (final Datum datum : data.getArrayValue()) {
        result.add(String.valueOf(Conversions.convert(
          getDatumSourceType(datum),
          JdbcType.VARCHAR,
          datum,
          callback)));
      }
      return result.toString();
    });
    arrayConversions.put(JdbcType.JAVA_OBJECT, (data, callback) -> data.getArrayValue());
    map.put(TimestreamDataType.ARRAY, arrayConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that convert data at a cell from a {@link
   * TimestreamDataType#ROW}.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateRowConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {

    final Map<JdbcType, TimestreamConvertFunction<?>> rowConversions = new EnumMap<>(
      JdbcType.class);
    rowConversions.put(JdbcType.VARCHAR, (data, callback) -> {
      final StringJoiner result = new StringJoiner(Constants.DELIMITER, "(", ")");
      for (final Datum datum : data.getRowValue().getData()) {
        result.add(String.valueOf(Conversions.convert(
          getDatumSourceType(datum),
          JdbcType.VARCHAR,
          datum,
          callback)));
      }

      return result.toString();
    });
    rowConversions.put(JdbcType.STRUCT, (data, callback) -> data.getRowValue());
    rowConversions
      .put(JdbcType.JAVA_OBJECT, (data, callback) -> ImmutableList.of(data.getRowValue()));
    map.put(TimestreamDataType.ROW, rowConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that converts data at a cell from a supported
   * data type to a {@link TimestreamDataType#TIMESERIES}.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateTimeSeriesConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> timeSeriesConversions = new EnumMap<>(
      JdbcType.class);
    timeSeriesConversions.put(JdbcType.VARCHAR, (data, callback) -> {
      final StringJoiner result = new StringJoiner(Constants.DELIMITER, "[", "]");
      for (final TimeSeriesDataPoint timeSeriesDataPoint : data.getTimeSeriesValue()) {
        final StringJoiner stringJoiner = new StringJoiner(Constants.DELIMITER, "{", "}");
        stringJoiner.add("time: " + timeSeriesDataPoint.getTime());

        final Datum value = timeSeriesDataPoint.getValue();
        stringJoiner.add("value: " + ((value == null) ? null : Conversions.convert(
          getDatumSourceType(value),
          JdbcType.VARCHAR,
          value,
          callback)));
        result.add(stringJoiner.toString());
      }

      return result.toString();
    });
    timeSeriesConversions.put(JdbcType.JAVA_OBJECT, (data, callback) -> data.getTimeSeriesValue());
    timeSeriesConversions.put(JdbcType.ARRAY, (data, callback) -> data.getTimeSeriesValue());
    map.put(TimestreamDataType.TIMESERIES, timeSeriesConversions);
  }

  /**
   * Populate the given {@link EnumMap} with lambdas that converts data at a cell from a supported
   * data type to a {@link TimestreamDataType#INTERVAL_DAY_TO_SECOND} or a {@link
   * TimestreamDataType#INTERVAL_YEAR_TO_MONTH}.
   *
   * @param map The {@link EnumMap} to populate with conversion lambdas.
   */
  private static void populateIntervalConversions(
    final Map<TimestreamDataType, Map<JdbcType, TimestreamConvertFunction<?>>> map) {
    final Map<JdbcType, TimestreamConvertFunction<?>> intervalConversion = new EnumMap<>(
      JdbcType.class);
    intervalConversion
      .put(JdbcType.VARCHAR, (data, callback) -> String.valueOf(data.getScalarValue()));

    map.put(TimestreamDataType.INTERVAL_DAY_TO_SECOND, intervalConversion);
    map.put(TimestreamDataType.INTERVAL_YEAR_TO_MONTH, intervalConversion);
  }

  /**
   * Validate whether the value can be represented within the range of the target data type.
   *
   * @param data       The data at a cell containing the value to convert.
   * @param minValue   The minimum value within the range of the target type.
   * @param maxValue   The maximum value within the range of the target type.
   * @param targetType The target {@link JdbcType}.
   * @return the {@link BigDecimal} value of the data at cell to prevent loss of precision.
   * @throws SQLException if the value at cell cannot be be represented within the range of the
   *                      target data type.
   */
  private static BigDecimal validateValueRange(
    final String data,
    final BigDecimal minValue,
    final BigDecimal maxValue,
    final JDBCType targetType) throws SQLException {

    final BigDecimal decimalValue = new BigDecimal(data);
    final BigInteger value = decimalValue.toBigInteger();
    if ((value.compareTo(maxValue.toBigInteger()) > 0) ||
      (value.compareTo(minValue.toBigInteger()) < 0)) {
      throw Error.createSQLException(
          LOGGER,
          Error.VALUE_OUT_OF_RANGE,
          data,
          minValue,
          maxValue,
          targetType);
    }
    return decimalValue;
  }

  private static TimestreamDataType getDatumSourceType(final Datum datum) {
    if (datum.getArrayValue() != null) {
      return TimestreamDataType.ARRAY;
    }

    if (datum.getTimeSeriesValue() != null) {
      return TimestreamDataType.TIMESERIES;
    }

    if (datum.getRowValue() != null) {
      return TimestreamDataType.ROW;
    }

    return TimestreamDataType.VARCHAR;
  }

  /**
   * Retrieves the conversion lambda if the conversion is supported.
   *
   * @param sourceType The Timestream data type of the provided data.
   * @param targetType The JDBC data type to convert the given data into.
   * @return the lambda converting the data from the source type to a target type.
   * @throws SQLException if the conversion between source type and target type is not supported.
   */
  static TimestreamConvertFunction<?> retrieveAndValidateConversion(
    final TimestreamDataType sourceType,
    final JdbcType targetType)
    throws SQLException {
    final Map<JdbcType, TimestreamConvertFunction<?>> conversion = CONVERSIONS.get(sourceType);

    if (conversion == null) {
      throw Error.createSQLException(
          LOGGER,
          Error.UNSUPPORTED_CONVERSION,
          sourceType.toString(),
          String.valueOf(targetType));
    }

    final TimestreamConvertFunction<?> fn = conversion.get(targetType);
    if (fn == null) {
      throw Error.createSQLException(
          LOGGER,
          Error.UNSUPPORTED_CONVERSION,
          sourceType.toString(),
          String.valueOf(targetType));
    }

    return fn;
  }

  /**
   * Converts the given data from the given source data type to a target type.
   *
   * @param sourceType The Timestream data type of the provided data.
   * @param targetType The JDBC data type to convert the given data into.
   * @param data       A cell of data from the current row.
   * @param callback   The callback to post a {@link SQLWarning} to the parent result set.
   * @return the data that has been converted to the target type.
   * @throws SQLException if the conversion between source type and target type is not supported.
   */
  static Object convert(
    final TimestreamDataType sourceType,
    final JdbcType targetType,
    final Datum data,
    final Consumer<SQLWarning> callback) throws SQLException {

    final TimestreamConvertFunction<?> fn = Conversions
      .retrieveAndValidateConversion(sourceType, targetType);

    try {
      LOGGER.debug("Converting data {} from source type {} to target type {}", data, sourceType, targetType);
      return fn.convert(data, callback);
    } catch (final IllegalArgumentException | DateTimeParseException e) {
      LOGGER.warn("Cannot parse data as {}: {}", sourceType, e.getMessage());
      throw new SQLException(
        Error.lookup(Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType), e);
    }
  }
}
