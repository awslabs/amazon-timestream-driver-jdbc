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
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enums representing Timestream data types and their equivalent JDBC and Java data type. Unknown
 * Timestream data types are mapped to {@link Types#VARCHAR} and {@link String} to provide more
 * useful information.
 */
enum TimestreamDataType {
  BIGINT(JdbcType.BIGINT, Long.class, 19),
  INTEGER(JdbcType.INTEGER, Integer.class, 11),
  BOOLEAN(JdbcType.BOOLEAN, Boolean.class, 5),
  DOUBLE(JdbcType.DOUBLE, Double.class, 53),
  VARCHAR(JdbcType.VARCHAR, String.class, 2147483647),
  DATE(JdbcType.DATE, Date.class, 10),
  TIME(JdbcType.TIME, Time.class, 18),
  TIMESTAMP(JdbcType.TIMESTAMP, Timestamp.class, 29),
  INTERVAL_YEAR_TO_MONTH(JdbcType.VARCHAR, String.class, 0),
  INTERVAL_DAY_TO_SECOND(JdbcType.VARCHAR, String.class, 0),
  ARRAY(JdbcType.ARRAY, Array.class, 0),
  ROW(JdbcType.STRUCT, Struct.class, 0),
  TIMESERIES(JdbcType.JAVA_OBJECT, List.class, 0),
  UNKNOWN(JdbcType.VARCHAR, String.class, 0);

  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamDataType.class);

  private static final Map<String, JdbcType> CLASS_NAME_TO_JDBC_TYPE = new ImmutableMap.Builder<String, JdbcType>()
    .put(Long.class.getName(), JdbcType.BIGINT)
    .put(Integer.class.getName(), JdbcType.INTEGER)
    .put(Boolean.class.getName(), JdbcType.BOOLEAN)
    .put(Double.class.getName(), JdbcType.DOUBLE)
    .put(String.class.getName(), JdbcType.VARCHAR)
    .put(Date.class.getName(), JdbcType.DATE)
    .put(Time.class.getName(), JdbcType.TIME)
    .put(Timestamp.class.getName(), JdbcType.TIMESTAMP)
    .put(Array.class.getName(), JdbcType.ARRAY)
    .put(Struct.class.getName(), JdbcType.STRUCT)
    .put(List.class.getName(), JdbcType.JAVA_OBJECT)
    .build();

  private static final Map<String, TimestreamDataType> CLASS_NAME_TO_TIMESTREAM_TYPE = new ImmutableMap.Builder<String, TimestreamDataType>()
    .put(Long.class.getName(), TimestreamDataType.BIGINT)
    .put(Integer.class.getName(), TimestreamDataType.INTEGER)
    .put(Boolean.class.getName(), TimestreamDataType.BOOLEAN)
    .put(Double.class.getName(), TimestreamDataType.DOUBLE)
    .put(String.class.getName(), TimestreamDataType.VARCHAR)
    .put(Date.class.getName(), TimestreamDataType.DATE)
    .put(Time.class.getName(), TimestreamDataType.TIME)
    .put(Timestamp.class.getName(), TimestreamDataType.TIMESTAMP)
    .put(Array.class.getName(), TimestreamDataType.ARRAY)
    .put(Struct.class.getName(), TimestreamDataType.ROW)
    .put(List.class.getName(), TimestreamDataType.TIMESERIES)
    .build();

  private final JdbcType jdbcType;
  private final Class<?> javaClass;
  private final int precision;

  /**
   * Constructor for {@link TimestreamDataType}.
   *
   * @param jdbcType  Equivalent JDBC data type.
   * @param javaClass Equivalent Java class.
   * @param precision Timestream data type's precision.
   */
  TimestreamDataType(final JdbcType jdbcType, final Class<?> javaClass, final int precision) {
    this.jdbcType = jdbcType;
    this.javaClass = javaClass;
    this.precision = precision;
  }

  /**
   * Getter for Timestream data type's equivalent JDBC data type.
   *
   * @return the equivalent JDBC data type.
   */
  JdbcType getJdbcType() {
    return jdbcType;
  }

  /**
   * Getter the Timestream data type's equivalent Java data type class.
   *
   * @return the equivalent Java data type.
   */
  Class<?> getJavaClass() {
    return javaClass;
  }

  /**
   * Gets the Timestream data type's equivalent Java data type class name.
   *
   * @return the String representing a Java data type.
   */
  String getClassName() {
    return javaClass.getName();
  }

  /**
   * Getter for Timestream data type's precision.
   *
   * @return the data type's precision.
   */
  int getPrecision() {
    return precision;
  }

  /**
   * Create a {@link ColumnInfo} object for a result set using the given {@link
   * TimestreamDataType}.
   *
   * @param type the {@link TimestreamDataType} of the column.
   * @param name the name of the column.
   * @return the ColumnInfo representing the column.
   */
  static ColumnInfo createColumnInfo(TimestreamDataType type, String name) {
    return new ColumnInfo().withName(name).withType(new Type().withScalarType(type.toString()));
  }

  /**
   * Creates a new {@link Type} object for a result set using the given conversion map.
   *
   * @param type          The {@link Type} containing new metadata for the column.
   * @param originalType  The original metadata of the column.
   * @param conversionMap The map specifying the conversion from a {@link TimestreamDataType} source
   *                      type to a target Java class.
   * @return the new {@link Type} describing the column.
   * @throws SQLException if there is an unsupported conversion in the conversion map.
   */
  static Type createTypeWithMap(
    final Type type,
    final Type originalType,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    // This method recursively construct a new Type object from the original Type using the given conversion map.
    //
    // The method looks up the original type in the conversion map, and if there a conversion
    // exists and the conversion is supported by the driver, the method will construct the new
    // Type object as the target type.
    //
    // For instance, if the original type is a TimeSeries and it is to be converted to an Array.
    // The method will use `type.withArrayColumnInfo` containing column information from the
    // TimeSeries object.

    final String originalScalarType = originalType.getScalarType();
    if (originalScalarType != null) {
      final TimestreamDataType targetType = retrieveTargetType(
        TimestreamDataType.valueOf(originalScalarType),
        conversionMap);
      return type.withScalarType(targetType.name());
    }

    final List<ColumnInfo> originalRow = originalType.getRowColumnInfo();
    if (originalRow != null) {
      final TimestreamDataType targetType = retrieveTargetType(TimestreamDataType.ROW,
        conversionMap);

      switch (targetType) {
        case VARCHAR: {
          return type.withScalarType(targetType.name());
        }

        case TIMESERIES: {
          throw Error.createSQLException(
              LOGGER,
              Error.UNSUPPORTED_CONVERSION,
              TimestreamDataType.ROW.name(),
              targetType);
        }

        default: {
          final List<ColumnInfo> targetRow = new ArrayList<>();
          for (final ColumnInfo columnInfo : originalRow) {
            targetRow.add(new ColumnInfo().withType(createTypeWithMap(
              new Type(),
              columnInfo.getType(),
              conversionMap)));
          }

          return type.withRowColumnInfo(targetRow);
        }
      }
    }

    final ColumnInfo originalTimeSeries = originalType.getTimeSeriesMeasureValueColumnInfo();
    if (originalTimeSeries != null) {
      return createType(type, originalTimeSeries, TimestreamDataType.TIMESERIES, conversionMap);
    }

    final ColumnInfo originalArray = originalType.getArrayColumnInfo();
    if (originalArray != null) {
      return createType(type, originalArray, TimestreamDataType.ARRAY, conversionMap);
    }

    throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.INVALID_TYPE, type));
  }

  /**
   * Create a new {@link Type} object from the given {@link ColumnInfo} and conversion map.
   *
   * @param type           The {@link Type} containing new metadata for the column.
   * @param originalColumn The original metadata of the column.
   * @param sourceType     The source type of the data.
   * @param conversionMap  The map specifying the conversion from a {@link TimestreamDataType}
   *                       source type to a target Java class.
   * @return the new {@link Type} describing the column.
   * @throws SQLException if there is an unsupported conversion in the conversion map.
   */
  private static Type createType(
    final Type type,
    final ColumnInfo originalColumn,
    final TimestreamDataType sourceType,
    final Map<String, Class<?>> conversionMap)
    throws SQLException {
    final TimestreamDataType targetType = retrieveTargetType(sourceType, conversionMap);
    final Type newType = createTypeWithMap(
      new Type(),
      originalColumn.getType(),
      conversionMap);

    switch (targetType) {
      case ARRAY: {
        return type.withArrayColumnInfo(new ColumnInfo().withType(newType));
      }

      case TIMESERIES: {
        return type.withTimeSeriesMeasureValueColumnInfo(
          new ColumnInfo().withType(newType));
      }

      default: {
        return type.withScalarType(targetType.name());
      }
    }
  }

  /**
   * Retrieve the target data type from the conversion map if the conversion is supported.
   *
   * @param sourceType    The source type of the data.
   * @param conversionMap A map specifying the conversions from a source type to a target data
   *                      type.
   * @return the target data type.
   * @throws SQLException if the conversion is not supported.
   */
  static TimestreamDataType retrieveTargetType(
    final TimestreamDataType sourceType,
    final Map<String, Class<?>> conversionMap) throws SQLException {
    final Class<?> targetClass = conversionMap.get(sourceType.name());
    final TimestreamDataType targetType =
      targetClass != null
        ? TimestreamDataType.convertClassNameToTimestreamType(targetClass.getName())
        : sourceType;

    Conversions.retrieveAndValidateConversion(
      sourceType,
      targetType.getJdbcType());

    return targetType;
  }

  /**
   * Constructs the given {@link ColumnInfo} object with the given {@link Type}.
   *
   * @param columnInfo The given {@link ColumnInfo} object.
   * @param type       The {@link Type} of the column.
   * @param name       The name of the column.
   * @return the ColumnInfo representing the column.
   */
  static ColumnInfo createColumnInfo(final ColumnInfo columnInfo, Type type, String name) {
    if (type.getScalarType() != null) {
      return columnInfo
        .withName(type.getScalarType())
        .withType(new Type().withScalarType(type.getScalarType()));
    }

    final ColumnInfo arrayColumnInfo = type.getArrayColumnInfo();
    if (arrayColumnInfo != null) {
      return columnInfo
        .withName(name)
        .withType(new Type().withArrayColumnInfo(createColumnInfo(
          new ColumnInfo(),
          arrayColumnInfo.getType(),
          TimestreamDataType.ARRAY.name())));
    }

    final List<ColumnInfo> rowColumnInfo = type.getRowColumnInfo();
    if (rowColumnInfo != null) {
      return columnInfo
        .withName(name)
        .withType(new Type().withRowColumnInfo(
          rowColumnInfo
            .parallelStream()
            .map(rowCol -> createColumnInfo(
              new ColumnInfo(),
              rowCol.getType(),
              TimestreamDataType.ROW.name()))
            .collect(Collectors.toList())));
    }

    final ColumnInfo timeSeriesColumnInfo = type.getTimeSeriesMeasureValueColumnInfo();
    if (timeSeriesColumnInfo != null) {
      return columnInfo
        .withName(name)
        .withType(new Type().withTimeSeriesMeasureValueColumnInfo(createColumnInfo(
          new ColumnInfo(),
          timeSeriesColumnInfo.getType(),
          TimestreamDataType.TIMESERIES.name())));
    }

    throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.INVALID_TYPE, type));
  }

  /**
   * Constructs the given {@link Datum} object with the given value.
   *
   * @param datum     The Timestream {@link Datum} object to construct.
   * @param value     The value to store in the  {@link Datum} object.
   * @param valueType The data type of the value.
   * @return the constructed  {@link Datum} object.
   * @throws SQLException if an error occurred while parsing nested values in TimestreamArray.
   */
  static Datum createDatum(final Datum datum, Object value, final Type valueType)
    throws SQLException {
    if (valueType.getScalarType() != null) {
      if (value instanceof Datum) {
        return (Datum) value;
      }
      return datum.withScalarValue(value.toString());
    }

    if (valueType.getArrayColumnInfo() != null) {
      // The value is a TimestreamArray. This happens when we are inside a nested array. We need
      // to call `getArrayList` here to retrieve the parsed values in the TimestreamArray.
      value = ((TimestreamArray) value).getArrayList();

      final List<Datum> arrayValueList = new ArrayList<>();
      for (final Object val : (List<Object>) value) {
        arrayValueList.add(createDatum(new Datum(), val, valueType.getArrayColumnInfo().getType()));
      }

      return datum.withArrayValue(arrayValueList);
    }

    if (valueType.getRowColumnInfo() != null) {
      final List<Datum> newRowData = new ArrayList<>();
      final List<Datum> rowData = ((TimestreamStruct) value).getStruct();
      final List<ColumnInfo> columnInfoList = valueType.getRowColumnInfo();
      for (int i = 0; i < rowData.size(); i++) {
        final Datum val = rowData.get(i);
        newRowData.add(createDatum(new Datum(), val, columnInfoList.get(i).getType()));
      }
      return datum.withRowValue(new Row().withData(newRowData));
    }

    if (valueType.getTimeSeriesMeasureValueColumnInfo() != null) {
      if (value instanceof TimeSeriesDataPoint) {
        // This should not happen, a TimeSeries object is a list of TimeSeriesDataPoint,
        // if the value is not within an ArrayList, an error has occurred while converting the data to
        // a TimestreamArray. Since calling `new Datum().withTimeSeriesValue((TimeSeriesDataPoint) value)`
        // will create a new ArrayList with the value, there is no a breaking error.
        LOGGER.warn("The TimeSeries value {} is not wrapped in an array list.", value);
        return new Datum().withTimeSeriesValue((TimeSeriesDataPoint) value);
      }

      return new Datum().withTimeSeriesValue((Collection<TimeSeriesDataPoint>) value);
    }

    throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.INVALID_TYPE, valueType));
  }

  /**
   * Parse a Timestream {@link Type} object to a {@link TimestreamDataType} enum.
   *
   * @param type the Timestream data type object to convert.
   * @return an enum representing the Timestream data type.
   */
  static TimestreamDataType fromType(final Type type) {
    if (type.getArrayColumnInfo() != null) {
      return ARRAY;
    }

    if (type.getRowColumnInfo() != null) {
      return ROW;
    }

    if (type.getTimeSeriesMeasureValueColumnInfo() != null) {
      return TIMESERIES;
    }

    try {
      return TimestreamDataType.valueOf(type.getScalarType().toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Unknown Timestream data type {}", type, e);
      return UNKNOWN;
    }
  }

  /**
   * Get the equivalent JDBC type code for the given Timestream data type.
   *
   * @param type a Timestream data type.
   * @return the JDBC type code.
   */
  static JdbcType getJdbcTypeCode(final Type type) {
    return fromType(type).jdbcType;
  }

  /**
   * Get the fully-qualified Java class name for the given Timestream data type.
   *
   * @param type a Timestream data type.
   * @return the fully-qualified Java class name.
   */
  static String getJavaClassName(final Type type) {
    return fromType(type).getClassName();
  }

  /**
   * Get the equivalent JDBC type from the given fully-qualified Java class name.
   *
   * @param className a fully-qualified Java class name.
   * @return the equivalent JDBC type.
   * @throws SQLException if the given Java class name cannot be mapped to a JDBC data type.
   */
  static JdbcType convertClassNameToJdbcType(final String className) throws SQLException {
    final JdbcType type = CLASS_NAME_TO_JDBC_TYPE.get(className);

    if (type == null) {
      throw Error.createSQLException(LOGGER, Error.UNSUPPORTED_CLASS, className);
    }

    return type;
  }

  /**
   * Get the equivalent JDBC type from the given fully-qualified Java class name.
   *
   * @param className a fully-qualified Java class name.
   * @return the equivalent JDBC type.
   * @throws SQLException if the given Java class name cannot be mapped to a JDBC data type.
   */
  static TimestreamDataType convertClassNameToTimestreamType(final String className)
    throws SQLException {
    final TimestreamDataType type = CLASS_NAME_TO_TIMESTREAM_TYPE.get(className);

    if (type == null) {
      throw Error.createSQLException(LOGGER, Error.UNSUPPORTED_CLASS, className);
    }

    return type;
  }
}
