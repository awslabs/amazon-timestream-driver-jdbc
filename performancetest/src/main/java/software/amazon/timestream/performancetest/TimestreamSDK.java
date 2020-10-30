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

import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.ScalarType;
import com.amazonaws.services.timestreamquery.model.Type;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Code from the sample application that parses the {@link QueryResult} returned by the {@link
 * com.amazonaws.services.timestreamquery.AmazonTimestreamQuery}.
 */
class TimestreamSDK {
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
    .ofPattern("HH:mm:ss.SSSSSSSSS");

  static void parseQueryResult(final QueryResult response) {
    final List<ColumnInfo> columnInfo = response.getColumnInfo();
    final List<Row> rows = response.getRows();

    for (final Row row : rows) {
      parseRow(columnInfo, row);
    }
  }

  /**
   * Parse the given row based on the given column info into Java objects.
   *
   * @param columnInfo The list of metadata describing the given row.
   * @param row        An {@link Row} of data.
   * @return a list of Java objects representing the data in the given row.
   */
  private static List<Object> parseRow(final List<ColumnInfo> columnInfo, final Row row) {
    final List<Datum> data = row.getData();
    final List<Object> rowOutput = new ArrayList<>();
    for (int j = 0; j < data.size(); j++) {
      rowOutput.add(parseDatum(columnInfo.get(j), data.get(j)));
    }
    return rowOutput;
  }

  /**
   * Parse the data in the given {@link Datum} representing a cell of data.
   *
   * @param info  The metadata describing the given {@link Datum}.
   * @param datum The {@link Datum} representing a cell of data.
   * @return the type of the data and the data's Java representation.
   */
  private static Object parseDatum(final ColumnInfo info, final Datum datum) {
    if (datum.isNullValue() != null && datum.isNullValue()) {
      return info.getName() + "=" + "NULL";
    }

    final Type columnType = info.getType();
    if (columnType.getTimeSeriesMeasureValueColumnInfo() != null) {
      return datum.getTimeSeriesValue();
    }

    if (columnType.getArrayColumnInfo() != null) {
      return info.getName() + "=" + parseArray(columnType.getArrayColumnInfo(),
        datum.getArrayValue());
    }

    if (columnType.getRowColumnInfo() != null) {
      return parseRow(columnType.getRowColumnInfo(), datum.getRowValue());
    }

    return parseScalarType(info, datum);
  }

  /**
   * Parse the scalar values into their equivalent Java representation.
   *
   * @param info  The metadata for the given data.
   * @param datum The {@link Datum} containing the scalar value.
   * @return the Java representation of the scalar value.
   * @throws IllegalArgumentException if the data is not a valid scalar type.
   */
  private static Object parseScalarType(final ColumnInfo info, final Datum datum) {
    switch (ScalarType.fromValue(info.getType().getScalarType())) {
      case INTERVAL_DAY_TO_SECOND:
      case INTERVAL_YEAR_TO_MONTH:
      case UNKNOWN:
      case VARCHAR: {
        return datum.getScalarValue();
      }

      case BIGINT: {
        return Long.parseLong(datum.getScalarValue());
      }

      case INTEGER: {
        return Integer.parseInt(datum.getScalarValue());
      }

      case BOOLEAN: {
        return Boolean.parseBoolean(datum.getScalarValue());
      }

      case DOUBLE: {
        return Double.parseDouble(datum.getScalarValue());
      }

      case TIMESTAMP: {
        return Timestamp
          .valueOf(LocalDateTime.parse(datum.getScalarValue(), TIMESTAMP_FORMATTER));
      }

      case DATE: {
        return Date.valueOf(LocalDate.parse(datum.getScalarValue(), DATE_FORMATTER));
      }

      case TIME: {
        return Time.valueOf(LocalTime.parse(datum.getScalarValue(), TIME_FORMATTER));
      }

      default: {
        throw new IllegalArgumentException(
          "Given type is not valid: " + info.getType().getScalarType());
      }
    }
  }

  /**
   * Parse the values int the given list of {@link Datum} into their equivalent Java
   * representation.
   *
   * @param arrayColumnInfo The metadata for the given list of array values.
   * @param arrayValues     A list of {@link Datum} representing an array of values.
   * @return A list of array values.
   */
  private static List<Object> parseArray(
    final ColumnInfo arrayColumnInfo,
    final List<Datum> arrayValues) {
    final List<Object> arrayOutput = new ArrayList<>();
    for (final Datum datum : arrayValues) {
      arrayOutput.add(parseDatum(arrayColumnInfo, datum));
    }
    return arrayOutput;
  }
}
