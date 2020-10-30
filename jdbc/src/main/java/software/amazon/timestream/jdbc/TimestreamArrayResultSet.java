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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ResultSet representing a Timestream Array.
 */
public class TimestreamArrayResultSet extends TimestreamBaseResultSet {
  private final List<Row> arrayResultSet = new ArrayList<>();
  private final TimestreamDataType timestreamDataType;
  private final Type baseType;

  /**
   * Constructor.
   *
   * @param array    A Timestream array.
   * @param baseType The {@link TimestreamDataType} of the elements in the array.
   * @throws SQLException if there is an error parsing the rows in the array.
   */
  TimestreamArrayResultSet(final List<Object> array, final Type baseType) throws SQLException {
    super(null, 1000);
    this.baseType = baseType;
    this.timestreamDataType = TimestreamDataType.fromType(baseType);
    this.rsMeta = createColumnMetadata(ImmutableList.of(
      TimestreamDataType.createColumnInfo(TimestreamDataType.INTEGER, Constants.INDEX),
      TimestreamDataType.createColumnInfo(
        new ColumnInfo(),
        baseType,
        Constants.VALUE)));
    populateCurrentRows(array);
  }

  @Override
  protected void doClose() {
    // Do nothing.
  }

  @Override
  protected boolean doNextPage() throws SQLException {
    verifyOpen();
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    verifyOpen();
    return getRow() > arrayResultSet.size();
  }

  @Override
  public boolean isLast() throws SQLException {
    verifyOpen();
    return !rowItr.hasNext();
  }

  /**
   * Map the elements in the array to a Timestream {@link Row}.
   *
   * @param array A Timestream array.
   * @throws SQLException if there is an error parsing the rows in the array.
   */
  private void populateCurrentRows(final List<Object> array) throws SQLException {
    for (int i = 0; i < array.size(); i++) {
      final Datum indexDatum = new Datum().withScalarValue(String.valueOf(i + 1));
      switch (this.timestreamDataType) {
        case ROW: {
          arrayResultSet.add(new Row().withData(
            indexDatum,
            new Datum().withRowValue(new Row().withData(((TimestreamStruct) array.get(i)).getStruct()))
          ));
          break;
        }

        case TIMESERIES: {
          arrayResultSet.add(new Row().withData(
            indexDatum,
            new Datum().withTimeSeriesValue((List<TimeSeriesDataPoint>) array.get(i))
          ));
          break;
        }

        case ARRAY: {
          final List<Datum> datumList = new ArrayList<>();
          datumList.add(indexDatum);
          for (Object o : array) {
            if (o instanceof TimestreamArray) {
              // The value is a TimestreamArray. This happens when we are inside a nested array.
              // We need to call `getArrayList` here to retrieve the parsed values in the
              // TimestreamArray.
              o = ((TimestreamArray) o).getArrayList();
            }

            final List<Datum> arrayValueList = new ArrayList<>();
            for (final Object val : ((List<Object>) o)) {
              arrayValueList.add(TimestreamDataType.createDatum(
                new Datum(),
                val,
                this.baseType.getArrayColumnInfo().getType()));
            }
            datumList.add(new Datum().withArrayValue(arrayValueList));

          }
          arrayResultSet.add(new Row().withData(datumList));
          break;
        }

        default: {
          arrayResultSet.add(new Row().withData(
            indexDatum,
            new Datum().withScalarValue(String.valueOf(array.get(i)))
          ));

          break;
        }
      }
    }

    this.rowItr = arrayResultSet.iterator();
  }
}
