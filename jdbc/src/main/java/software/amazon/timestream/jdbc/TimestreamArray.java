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

import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.amazonaws.services.timestreamquery.model.Type;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Array} containing Timestream objects.
 */
public class TimestreamArray implements Array {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamArray.class);
  private final TimestreamBaseResultSet parentResultSet;
  private final List<Object> timestreamArray;
  private final Map<String, Class<?>> conversionMap;
  private final Type timestreamBaseType;
  private final TimestreamDataType baseType;
  private List<Object> array;

  /**
   * Base constructor.
   *
   * @param timestreamArray A list of objects represents value in the array.
   * @param baseType        The {@link Type} of the elements in the array.
   * @param parentResultSet The parent {@link TimestreamBaseResultSet} resultSet.
   */
  TimestreamArray(
    final List<Object> timestreamArray,
    final Type baseType,
    final TimestreamBaseResultSet parentResultSet) {
    this(timestreamArray, baseType, parentResultSet, new HashMap<>());
  }

  /**
   * Base constructor.
   *
   * @param timestreamArray A list of objects represents value in the array.
   * @param baseType        The {@link Type} of the elements in the array.
   * @param parentResultSet The parent {@link TimestreamBaseResultSet} resultSet.
   * @param conversionMap   The map specifying the conversion from a {@link TimestreamDataType} source
   *                        type to a target Java class.
   */
  TimestreamArray(
    final List<Object> timestreamArray,
    final Type baseType,
    final TimestreamBaseResultSet parentResultSet,
    final Map<String, Class<?>> conversionMap) {
    this.parentResultSet = parentResultSet;
    this.timestreamBaseType = baseType;
    this.baseType = TimestreamDataType.fromType(baseType);
    this.timestreamArray = timestreamArray;
    this.conversionMap = conversionMap;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    verifyOpen();
    return baseType.getJdbcType().name();
  }

  @Override
  public int getBaseType() throws SQLException {
    verifyOpen();
    return baseType.getJdbcType().jdbcCode;
  }

  @Override
  public Object getArray() throws SQLException {
    final List<Object> list = this.getArrayList();
    return convertToArray(list);
  }

  @Override
  public Object getArray(final long index, final int count) throws SQLException {
    verifyIndex(index);
    return Arrays.copyOfRange((Object[]) this.getArray(), (int) index - 1, (int) index + count);
  }

  @Override
  public Object getArray(final long index, final int count, final Map<String, Class<?>> map)
    throws SQLException {
    verifyIndex(index);
    final Object objectArray = this.getArray(map);

    return Arrays.copyOfRange((Object[]) objectArray, (int) index - 1, (int) index + count);
  }

  @Override
  public Object getArray(final Map<String, Class<?>> map) throws SQLException {
    final List<Object> convertedArray = this.getArrayList(map);
    return convertToArray(convertedArray);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return new TimestreamArrayResultSet(this.getArrayList(), timestreamBaseType);
  }

  @Override
  public ResultSet getResultSet(final Map<String, Class<?>> map) throws SQLException {
    return new TimestreamArrayResultSet(
      getArrayList(map),
      TimestreamDataType.createTypeWithMap(new Type(), timestreamBaseType, map));
  }

  @Override
  public ResultSet getResultSet(final long index, final int count) throws SQLException {
    return new TimestreamArrayResultSet(
      this.getArrayList(index, count),
      this.timestreamBaseType);
  }

  @Override
  public ResultSet getResultSet(final long index, final int count,
    final Map<String, Class<?>> map)
    throws SQLException {
    return new TimestreamArrayResultSet(
      getArrayList(index, count, map),
      TimestreamDataType.createTypeWithMap(new Type(), timestreamBaseType, map));
  }

  @Override
  public void free() throws SQLException {
    verifyOpen();
  }

  @Override
  public String toString() {
    String str;
    try {
      str = (String) Conversions.convert(
        TimestreamDataType.ARRAY,
        JdbcType.VARCHAR,
        new Datum().withArrayValue((List<Datum>) (List<?>) this.timestreamArray),
        this.parentResultSet::addWarning);
    } catch (SQLException sqlException) {
      LOGGER.warn("Unable to convert array to a string, {}", sqlException.getMessage());
      str = this.timestreamArray.toString();
    }
    return str;
  }

  /**
   * Verify the parent result set is open.
   *
   * @throws SQLException if the parent result set is closed.
   */
  protected void verifyOpen() throws SQLException {
    if (parentResultSet.isClosed()) {
      throw new SQLException(Error.lookup(Error.RESULT_SET_CLOSED));
    }
  }

  /**
   * Returns an {@link ArrayList} of the parsed Timestream data.
   *
   * @return an {@link ArrayList} containing the parsed data.
   * @throws SQLException if an error occurred while parsing the data.
   */
  List<Object> getArrayList() throws SQLException {
    verifyOpen();
    if (this.array == null) {
      this.array = parseArray(this.timestreamArray);
    }
    return this.array;
  }

  /**
   * Returns a sub {@link ArrayList} of the parsed Timestream data starting from the index to the
   * number of items specified by count.
   *
   * @param index The starting index of the subarray.
   * @param count The number of items in the new subarray.
   * @return an {@link ArrayList} containing the parsed data.
   * @throws SQLException if an error occurred while parsing the data.
   */
  List<Object> getArrayList(final long index, final int count) throws SQLException {
    verifyIndex(index);
    return this.getArrayList().subList((int) index - 1, (int) index + count);
  }

  /**
   * Returns a sub {@link ArrayList} of the Timestream data parsed using the given conversion map,
   * starting from the index to the number of items specified by count.
   *
   * @param index The starting index of the subarray.
   * @param count The number of items in the new subarray.
   * @param map   The conversion map specifying the Timestream data type to convert to the Java data
   *              type.
   * @return an {@link ArrayList} containing the parsed data.
   * @throws SQLException if an error occurred while parsing the data.
   */
  private List<Object> getArrayList(final long index, final int count, final Map<String, Class<?>> map)
    throws SQLException {
    verifyIndex(index);
    final List<Object> arrayList = this.getArrayList(map);
    return arrayList.subList((int) index - 1, (int) index + count);
  }

  /**
   * Returns an {@link ArrayList} of the Timestream data parsed using the given conversion map.
   *
   * @param map The conversion map specifying the Timestream data type to convert to the Java data
   *            type.
   * @return an {@link ArrayList} containing the parsed data.
   * @throws SQLException if an error occurred while parsing the data.
   */
  private List<Object> getArrayList(final Map<String, Class<?>> map) throws SQLException {
    verifyOpen();
    return parseArray(this.timestreamArray, baseType, map);
  }

  /**
   * Parse the elements in the given array list as their original data type.
   *
   * @param array An {@link ArrayList} containing data from Timestream.
   * @return an {@link ArrayList} containing parsed elements.
   * @throws SQLException if there is an error retrieving data from the array.
   */
  private List<Object> parseArray(final List<?> array) throws SQLException {
    return parseArray(array, this.baseType, conversionMap);
  }

  /**
   * Parse the elements in the given array list as the target Java data type specified in the given
   * conversion map.
   *
   * @param array         An {@link ArrayList} containing data from Timestream.
   * @param sourceType    The value's {@link TimestreamDataType}.
   * @param conversionMap The map specifying the conversion from a {@link TimestreamDataType} source
   *                      type to a target Java class.
   * @return an {@link ArrayList} containing parsed elements.
   * @throws SQLException if there is an error retrieving data from the array.
   */
  private List<Object> parseArray(
    final List<?> array,
    final TimestreamDataType sourceType,
    final Map<String, Class<?>> conversionMap)
    throws SQLException {
    final List<Object> arrayList = new ArrayList<>();

    if (array.isEmpty()) {
      return arrayList;
    }

    final JdbcType targetType = TimestreamDataType
      .retrieveTargetType(sourceType, conversionMap)
      .getJdbcType();

    if (array.get(0) instanceof TimeSeriesDataPoint) {
      if (sourceType != TimestreamDataType.TIMESERIES) {
        // If the sourceType is not TIMESERIES, then a logical error has happened when creating
        // column metadata in `TimestreamArrayResultSet` and the driver is trying to parse
        // the data using the faulty ResultSet metadata.
        throw new RuntimeException(
            Error.getErrorMessage(LOGGER, Error.INVALID_DATA_AT_ARRAY, array));
      }

      if (targetType == TimestreamDataType.TIMESERIES.getJdbcType()) {
        return ImmutableList.of(array);
      }

      arrayList.add(Conversions.convert(
        sourceType,
        targetType,
        new Datum().withTimeSeriesValue((Collection<TimeSeriesDataPoint>) array),
        this.parentResultSet::addWarning));

      return arrayList;
    }

    if (array.get(0) instanceof Datum) {
      switch (sourceType) {
        case ARRAY: {
          for (final Object o : array) {
            final Object arrayValue = ((Datum) o).getArrayValue();
            if (arrayValue == null) {
              // If the sourceType is TimestreamDataType.ARRAY but the datum does not
              // contain arrayValue, then a logical error has happened when creating a
              // column metadata for the array.
              throw new RuntimeException(
                  Error.getErrorMessage(LOGGER, Error.INVALID_DATA_AT_ARRAY, array));
            }

            final TimestreamArray result = new TimestreamArray(
              (List<Object>) arrayValue,
              this.timestreamBaseType.getArrayColumnInfo().getType(),
              this.parentResultSet,
              conversionMap);

            if (targetType == JdbcType.VARCHAR) {
              arrayList.add(String.valueOf(result.getArrayList()));
              continue;
            }

            final List<Object> resultList = populateArrayListToDatum(result.getArrayList(), result);

            final TimestreamArray convertedResult = new TimestreamArray(
                resultList,
                TimestreamDataType.createTypeWithMap(
                    new Type(),
                    this.timestreamBaseType.getArrayColumnInfo().getType(),
                    conversionMap),
                this.parentResultSet,
                conversionMap);
            arrayList.add(convertedResult);
          }
          break;
        }

        case ROW: {
          for (final Object o : array) {
            final Row rowValue = ((Datum) o).getRowValue();
            if ((rowValue == null) || (rowValue.getData() == null)) {
              throw new RuntimeException(
                  Error.getErrorMessage(LOGGER, Error.INVALID_DATA_AT_ROW, array));
            }

            final TimestreamStruct struct = new TimestreamStruct(
                rowValue.getData(),
                this.timestreamBaseType.getRowColumnInfo(),
                this.parentResultSet,
                conversionMap);

            if (targetType == JdbcType.VARCHAR) {
              final List<Datum> data = new ArrayList<>();
              for (final Object attribute : struct.getAttributes()) {
                data.add(new Datum().withScalarValue(attribute.toString()));
              }
              final Object convertedResult = Conversions.convert(
                  baseType,
                  targetType,
                  new Datum().withRowValue(new Row().withData(data)),
                  this.parentResultSet::addWarning
              );
              arrayList.add(convertedResult);
              continue;
            }

            final List<Datum> convertedDatumList = TimestreamStruct
                .populateObjectToDatum(struct.getAttributes());

            final TimestreamStruct convertedStruct = new TimestreamStruct(
                convertedDatumList,
                this.timestreamBaseType.getRowColumnInfo(),
                this.parentResultSet,
                conversionMap);

            arrayList.add(convertedStruct);
          }
          break;
        }

        default: {
          for (final Object object : array) {
            arrayList.add(Conversions.convert(
              sourceType,
              targetType,
              (Datum) object,
              this.parentResultSet::addWarning));
          }
          break;
        }
      }
    } else {
      throw new RuntimeException(Error.getErrorMessage(LOGGER, Error.INVALID_DATA_AT_ARRAY, array));
    }

    return arrayList;
  }

  /**
   * Populate an array list to a list of {@link Datum}.
   *
   * @param arrayList An {@link ArrayList} that need to be converted.
   * @param result    The outer {@link TimestreamArray}.
   * @return A list of datum converted from the array list.
   * @throws SQLException if there is an error populating the {@link ArrayList}.
   */
  static List<Object> populateArrayListToDatum(
      final List<Object> arrayList,
      final TimestreamArray result) throws SQLException {
    final List<Object> resultList = new ArrayList<>();

    for (final Object arrayObject : arrayList) {
      if (arrayObject instanceof TimestreamArray) {
        // Check for situation when there is a nested TimeSeries object in an array.
        if (JdbcType.JAVA_OBJECT.name().equals(((TimestreamArray) arrayObject).getBaseTypeName())) {
          resultList.add(result.timestreamArray.get(0));
          continue;
        }
        final List<Object> nestedArray = populateArrayListToDatum(
            ((TimestreamArray) arrayObject).getArrayList(),
            result);
        resultList.add(new Datum().withArrayValue((List<Datum>) (List<?>) nestedArray));
        continue;
      }
      if (arrayObject instanceof TimestreamStruct) {
        final List<Datum> nestedRow = ((TimestreamStruct) arrayObject).getStruct();
        resultList.add(new Datum().withRowValue(new Row().withData(nestedRow)));
        continue;
      }
      // When there is TimeSeries object in array, it is wrapped up with an array list outside.
      if (arrayObject instanceof ArrayList) {
        resultList.add(new Datum().withTimeSeriesValue((List<TimeSeriesDataPoint>) arrayObject));
        continue;
      }
      resultList.add(new Datum().withScalarValue(arrayObject.toString()));
    }
    return resultList;
  }

  /**
   * Verify if the given array index is valid.
   *
   * @param arrayIndex the 1-based array index.
   * @throws SQLException if the index is not valid for this array.
   */
  private void verifyIndex(long arrayIndex) throws SQLException {
    if ((1 > arrayIndex) || (arrayIndex > this.timestreamArray.size())) {
      throw Error
          .createSQLException(LOGGER, Error.INVALID_INDEX, arrayIndex, this.timestreamArray.size());
    }
  }

  /**
   * Converts the given {@link ArrayList} to an array.
   *
   * @param list The {@link ArrayList} to convert.
   * @return an array.
   */
  private Object convertToArray(final List<Object> list) {
    if (!list.isEmpty() && list.get(0) instanceof TimestreamArray) {
      // Nested arrays need to be handled differently.
      return convertToArray(list, list.get(0).getClass());
    }

    // Convert empty Arrays or simple Arrays to Object arrays.
    return list.toArray();
  }

  /**
   * Converts the given {@link ArrayList} into an array of the given type {@link T}.
   *
   * @param arrayList The {@link ArrayList} to convert.
   * @param valueClass The Java class of the data in the {@link ArrayList}.
   * @param <T>        The target data type of the array.
   * @return an array in the given data type.
   */
  private <T> Object convertToArray(final List<Object> arrayList, Class<T> valueClass) {
    return arrayList.toArray((T[]) java.lang.reflect.Array.newInstance(valueClass, arrayList.size()));
  }

  /**
   * Getter for timestreamArray.
   *
   * @return TimestreamArray.
   */
  List<Object> getTimestreamArray() {
    return this.timestreamArray;
  }
}
