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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Struct} containing Timestream objects.
 */
public class TimestreamStruct implements Struct {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamStruct.class);
  private final List<Datum> struct;
  private final TimestreamBaseResultSet parentResultSet;
  private final List<TimestreamDataType> baseTypeList;
  private final List<Type> typeList;
  private final Map<String, Class<?>> conversionMap;

  /**
   * Constructor.
   *
   * @param struct          A list of {@link Datum} represents data in the struct.
   * @param columnInfos     A list of {@link ColumnInfo} contains row column info.
   * @param parentResultSet The parent {@link TimestreamBaseResultSet} resultSet.
   */
  TimestreamStruct(
    final List<Datum> struct,
    final List<ColumnInfo> columnInfos,
    final TimestreamBaseResultSet parentResultSet) {
    this(struct, columnInfos, parentResultSet, new HashMap<>());
  }

  /**
   * Constructor with conversion map.
   *
   * @param struct          A list of {@link Datum} represents data in the struct.
   * @param columnInfos     A list of {@link ColumnInfo} contains row column info.
   * @param parentResultSet The parent {@link TimestreamBaseResultSet} resultSet.
   * @param conversionMap   The map specifying the conversion from a {@link TimestreamDataType} source
   *                        type to a target Java class.
   */
  TimestreamStruct(
    final List<Datum> struct,
    final List<ColumnInfo> columnInfos,
    final TimestreamBaseResultSet parentResultSet,
    final Map<String, Class<?>> conversionMap) {
    this.struct = struct;
    this.parentResultSet = parentResultSet;
    this.conversionMap = conversionMap;
    this.baseTypeList = columnInfos
      .parallelStream()
      .map(columnInfo -> TimestreamDataType.fromType(columnInfo.getType()))
      .collect(Collectors.toList());
    this.typeList = columnInfos
      .parallelStream()
      .map(ColumnInfo::getType)
      .collect(Collectors.toList());
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    verifyOpen();
    return Row.class.getName();
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    verifyOpen();
    return parseRow(struct).toArray();
  }

  @Override
  public Object[] getAttributes(final Map<String, Class<?>> map) throws SQLException {
    verifyOpen();
    return parseRow(struct, baseTypeList, map).toArray();
  }

  /**
   * Parse the elements in the given array list as their original data type.
   *
   * @param row An {@link ArrayList} containing data from Timestream.
   * @return an {@link ArrayList} containing parsed elements.
   * @throws SQLException if there is an error retrieving data from the array.
   */
  private List<Object> parseRow(final List<Datum> row) throws SQLException {
    return parseRow(row, this.baseTypeList, this.conversionMap);
  }

  /**
   * Parse the elements in the given array list as the given target data type.
   *
   * @param row          An {@link ArrayList} containing data from Timestream.
   * @param baseTypeList The base TimestreamDataType {@link ArrayList} need to be converted.
   * @param map          The map specifying the conversion from a {@link TimestreamDataType} source
   *                     type to a target Java class.
   * @return an {@link ArrayList} containing parsed elements.
   * @throws SQLException if there is an error retrieving data from the array.
   */
  private List<Object> parseRow(
    final List<Datum> row,
    final List<TimestreamDataType> baseTypeList,
    final Map<String, Class<?>> map) throws SQLException {
    final List<Object> rowList = new ArrayList<>();

    final List<JdbcType> targetTypeList = new ArrayList<>();
    for (TimestreamDataType timestreamDataType : baseTypeList) {
      final Class<?> targetClass = map.get(timestreamDataType.name());
      final JdbcType targetType =
        targetClass != null
          ? TimestreamDataType.convertClassNameToJdbcType(targetClass.getName())
          : timestreamDataType.getJdbcType();
      targetTypeList.add(targetType);
    }

    int i = 0;
    for (final Datum datum : row) {
      final TimestreamDataType baseType;
      final JdbcType targetType;

      if (baseTypeList.size() == 1) {
        baseType = baseTypeList.get(0);
        targetType = targetTypeList.get(0);
      } else {
        baseType = baseTypeList.get(i);
        targetType = targetTypeList.get(i);
      }

      switch (baseType) {
        case ARRAY: {
          final TimestreamArray timestreamArray = new TimestreamArray(
              (List<Object>) (Object) datum.getArrayValue(),
              this.typeList.get(i).getArrayColumnInfo().getType(),
              this.parentResultSet,
              map);

          if (targetType == JdbcType.VARCHAR) {
            rowList.add(String.valueOf(timestreamArray.getArrayList()));
            continue;
          }

          final List<Object> resultList = TimestreamArray.populateArrayListToDatum(
              timestreamArray.getArrayList(), timestreamArray);

          final TimestreamArray convertedArray = new TimestreamArray(
              resultList,
              this.typeList.get(i).getArrayColumnInfo().getType(),
              this.parentResultSet,
              map);

          rowList.add(convertedArray);
          break;
        }

        case ROW: {
          final TimestreamStruct timestreamStruct = new TimestreamStruct(
              datum.getRowValue().getData(),
              this.typeList.get(i).getRowColumnInfo(),
              this.parentResultSet,
              map);

          if (targetType == JdbcType.VARCHAR) {
            final List<Datum> data = new ArrayList<>();
            for (final Object attribute : timestreamStruct.getAttributes()) {
              data.add(new Datum().withScalarValue(attribute.toString()));
            }
            final Object convertedResult = Conversions.convert(
                baseType,
                targetType,
                new Datum().withRowValue(new Row().withData(data)),
                this.parentResultSet::addWarning
            );
            rowList.add(convertedResult);
            continue;
          }

          final List<Datum> convertedDatumList =
              populateObjectToDatum(timestreamStruct.getAttributes());

          final TimestreamStruct convertedStruct = new TimestreamStruct(
              convertedDatumList,
              this.typeList.get(i).getRowColumnInfo(),
              this.parentResultSet,
              map);
          rowList.add(convertedStruct);
          break;
        }

        default: {
          rowList.add(Conversions.convert(
              baseType,
              targetType,
              datum,
              this.parentResultSet::addWarning
          ));
          break;
        }
      }
      i++;
    }
    return rowList;
  }

  /**
   * Populate an object array to a list of Datum.
   *
   * @param attributes An object array that need to be populated.
   * @return A list of Datum represent the object array.
   * @throws SQLException when retrieving values from object.
   */
  static List<Datum> populateObjectToDatum(Object[] attributes) throws SQLException {
    final List<Datum> resultList = new ArrayList<>();
    for (final Object attribute : attributes) {
      if (attribute instanceof TimestreamStruct) {
        resultList.add(new Datum()
            .withRowValue(new Row()
                .withData(((TimestreamStruct) attribute).getStruct())));
        continue;
      }
      if (attribute instanceof TimestreamArray) {
        // Check for situation when there is a nested TimeSeries object in an array.
        if (JdbcType.JAVA_OBJECT.name().equals(((TimestreamArray) attribute).getBaseTypeName())) {
          resultList.add(new Datum()
              .withArrayValue((Datum) ((TimestreamArray) attribute).getTimestreamArray().get(0)));
          continue;
        }
        final List<Object> nestedArray = TimestreamArray.populateArrayListToDatum(
            ((TimestreamArray) attribute).getArrayList(), (TimestreamArray) attribute);
        resultList.add(new Datum().withArrayValue((List<Datum>) (List<?>) nestedArray));
        continue;
      }
      // When there is TimeSeries object in array, it is wrapped up with an array list outside.
      if (attribute instanceof ArrayList) {
        resultList.add(new Datum()
            .withTimeSeriesValue((List<TimeSeriesDataPoint>) attribute));
        continue;
      }
      resultList.add(new Datum().withScalarValue(attribute.toString()));
    }
    return resultList;
  }

  /**
   * Gets the list of {@link Datum} representing a {@link Row} in Timestream.
   *
   * @return the list of {@link Datum}.
   */
  List<Datum> getStruct() {
    return struct;
  }

  /**
   * Verify the parent result set is open.
   *
   * @throws SQLException if the parent result set is closed.
   */
  private void verifyOpen() throws SQLException {
    if (parentResultSet.isClosed()) {
      throw Error.createSQLException(LOGGER, Error.RESULT_SET_CLOSED);
    }
  }

  @Override
  public String toString() {
      String str;
      try {
        str = (String) Conversions.convert(
          TimestreamDataType.ROW,
          JdbcType.VARCHAR,
          new Datum().withRowValue(new Row().withData(this.struct)),
          this.parentResultSet::addWarning);
      } catch (SQLException sqlException) {
        LOGGER.warn("Unable to convert row to a string, {}.", sqlException.getMessage());
        str = this.struct.toString();
      }
      return str;
  }
}
