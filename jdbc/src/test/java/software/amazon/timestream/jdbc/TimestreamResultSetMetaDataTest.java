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
import com.amazonaws.services.timestreamquery.model.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.timestream.jdbc.TimestreamResultSetMetaData.ColInfo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests of TimestreamResultSetMetaData.
 */
class TimestreamResultSetMetaDataTest {

  private TimestreamResultSetMetaData rsMeta;
  private static final int COLINFOLIST_SIZE = 1;
  private static final Type TYPE = new Type().withScalarType(TimestreamDataType.VARCHAR.toString());
  private static final String NAME = "columnName";
  private static final ColumnInfo COLUMN_INFO = new ColumnInfo();

  private enum ScalarDataType {
    BOOLEAN(TimestreamDataType.BOOLEAN, 5, 5),
    INTEGER(TimestreamDataType.INTEGER, 11, 11),
    BIGINT(TimestreamDataType.BIGINT, 20, 19),
    DOUBLE(TimestreamDataType.DOUBLE, 15, 53),
    VARCHAR(TimestreamDataType.VARCHAR, 2147483647, 2147483647),
    DATE(TimestreamDataType.DATE, 10, 10),
    TIME(TimestreamDataType.TIME, 18, 18),
    TIMESTAMP(TimestreamDataType.TIMESTAMP, 29, 29);

    private final TimestreamDataType timestreamDataType;
    private final int displaySize;
    private final int precision;

    ScalarDataType(TimestreamDataType timestreamDataType, int displaySize, int precision) {
      this.timestreamDataType = timestreamDataType;
      this.displaySize = displaySize;
      this.precision = precision;
    }
  }

  /**
   * Initialize the {@link TimestreamResultSetMetaData} with the specified {@link ColInfo}.
   *
   * @param type The {@link Type} of the data.
   */
  private void initializeCol(Type type) {
    final List<ColInfo> colInfoList = new ArrayList<>();
    final ColInfo colInfo = new ColInfo(type, NAME);
    colInfoList.add(colInfo);
    rsMeta = new TimestreamResultSetMetaData(colInfoList);
  }

  @BeforeEach
  void init() {
    initializeCol(TYPE);
  }

  @Test
  void testGetCatalogName() throws SQLException {
    Assertions.assertNull(rsMeta.getCatalogName(1));

    final int invalidColumn = 0;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.getCatalogName(invalidColumn));
  }

  @Test
  void testGetColumnClassName() throws SQLException {
    Assertions
      .assertEquals(TimestreamDataType.getJavaClassName(TYPE), rsMeta.getColumnClassName(1));

    final int invalidColumn = 2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.getColumnClassName(invalidColumn));
  }

  @Test
  void testGetColumnCount() {
    Assertions.assertEquals(COLINFOLIST_SIZE, rsMeta.getColumnCount());
  }

  @ParameterizedTest
  @EnumSource(ScalarDataType.class)
  void testGetColumnDisplaySizeWithScalarType(ScalarDataType testDataType) throws SQLException {
    final Type type = new Type().withScalarType(testDataType.timestreamDataType.toString());
    initializeCol(type);
    Assertions.assertEquals(testDataType.displaySize, rsMeta.getColumnDisplaySize(1));
  }

  @Test
  void testGetColumnLabel() throws SQLException {
    Assertions.assertEquals(NAME, rsMeta.getColumnLabel(1));
  }

  @Test
  void testGetColumnTypeWithNonScalarType() throws SQLException {
    final Type arrayType = new Type().withArrayColumnInfo(COLUMN_INFO);
    final int expectedCode = TimestreamDataType.getJdbcTypeCode(arrayType).jdbcCode;
    initializeCol(arrayType);
    Assertions.assertEquals(expectedCode, rsMeta.getColumnType(1));
  }

  @ParameterizedTest
  @EnumSource(ScalarDataType.class)
  void testGetColumnTypeWithScalarType(ScalarDataType testDataType) throws SQLException {
    final Type type = new Type().withScalarType(testDataType.toString());
    final int expectedCode = TimestreamDataType.getJdbcTypeCode(type).jdbcCode;
    initializeCol(type);
    Assertions.assertEquals(expectedCode, rsMeta.getColumnType(1));
  }

  @Test
  void testGetColumnTypeNameWithArrayType() throws SQLException {
    final Type arrayType = new Type().withArrayColumnInfo(COLUMN_INFO);
    final String expectation = TimestreamDataType.fromType(arrayType).toString();
    initializeCol(arrayType);
    Assertions.assertEquals(expectation, rsMeta.getColumnTypeName(1));
  }

  @Test
  void testGetColumnTypeNameWithTimeSeriesType() throws SQLException {
    final Type timeSeriesType = new Type().withTimeSeriesMeasureValueColumnInfo(COLUMN_INFO);
    final String expectation = TimestreamDataType.fromType(timeSeriesType).toString();
    initializeCol(timeSeriesType);
    Assertions.assertEquals(expectation, rsMeta.getColumnTypeName(1));
  }

  @ParameterizedTest
  @EnumSource(ScalarDataType.class)
  void testGetColumnTypeNameWithWithScalarType(ScalarDataType testDataType) throws SQLException {
    final Type scalarType = new Type().withScalarType(testDataType.toString());
    final String expectedCode = TimestreamDataType.fromType(scalarType).toString();
    initializeCol(scalarType);
    Assertions.assertEquals(expectedCode, rsMeta.getColumnTypeName(1));
  }

  @Test
  void testGetPrecisionWithNonScalarType() throws SQLException {
    final Type arrayType = new Type().withArrayColumnInfo(COLUMN_INFO);
    initializeCol(arrayType);
    Assertions.assertEquals(0, rsMeta.getPrecision(1));

    final Type timeSeriesType = new Type().withTimeSeriesMeasureValueColumnInfo(COLUMN_INFO);
    initializeCol(timeSeriesType);
    Assertions.assertEquals(0, rsMeta.getPrecision(1));
  }

  @ParameterizedTest
  @EnumSource(ScalarDataType.class)
  void testGetPrecisionWithScalarType(ScalarDataType testDataType) throws SQLException {
    final Type type = new Type().withScalarType(testDataType.timestreamDataType.toString());
    initializeCol(type);
    Assertions.assertEquals(testDataType.precision, rsMeta.getPrecision(1));
  }

  @Test
  void testGetScale() throws SQLException {
    Assertions.assertEquals(0, rsMeta.getScale(1));

    final int invalidColumn = 2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.getScale(invalidColumn));
  }

  @Test
  void testIsAutoIncrement() throws SQLException {
    Assertions.assertFalse(rsMeta.isAutoIncrement(1));

    final int invalidColumn = -1;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isAutoIncrement(invalidColumn));
  }

  @Test
  void testIsCaseSensitive() throws SQLException {
    Assertions.assertTrue(rsMeta.isCaseSensitive(1));

    final int invalidColumn = 2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isCaseSensitive(invalidColumn));
  }

  @Test
  void testIsCurrency() throws SQLException {
    Assertions.assertFalse(rsMeta.isCurrency(1));

    final int invalidColumn = 0;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isCurrency(invalidColumn));
  }

  @Test
  void testIsDefinitelyWritable() throws SQLException {
    Assertions.assertFalse(rsMeta.isDefinitelyWritable(1));

    final int invalidColumn = 2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isDefinitelyWritable(invalidColumn));
  }

  @Test
  void testIsNullableWithNull() throws SQLException {
    Assertions.assertEquals(1, rsMeta.isNullable(1));
  }

  @Test
  void testIsReadOnly() throws SQLException {
    Assertions.assertTrue(rsMeta.isReadOnly(1));

    final int invalidColumn = 0;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isReadOnly(invalidColumn));
  }

  @Test
  void testIsSearchable() throws SQLException {
    Assertions.assertTrue(rsMeta.isSearchable(1));

    final int invalidColumn = 2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isSearchable(invalidColumn));
  }

  @ParameterizedTest
  @EnumSource(value = ScalarDataType.class, names = {"INTEGER", "BIGINT"})
  void testIsSignedWithWithScalarType(ScalarDataType testDataType) throws SQLException {
    final Type scalarType = new Type().withScalarType(testDataType.toString());
    initializeCol(scalarType);
    Assertions.assertTrue(rsMeta.isSigned(1));
  }

  @Test
  void testIsSignedWithNonScalarType() throws SQLException {
    final Type arrayType = new Type().withArrayColumnInfo(COLUMN_INFO);
    initializeCol(arrayType);
    Assertions.assertFalse(rsMeta.isSigned(1));

    final Type timeSeriesType = new Type().withTimeSeriesMeasureValueColumnInfo(COLUMN_INFO);
    initializeCol(timeSeriesType);
    Assertions.assertFalse(rsMeta.isSigned(1));
  }

  @Test
  void testIsWritable() throws SQLException {
    Assertions.assertFalse(rsMeta.isWritable(1));

    final int invalidColumn = -2;
    Assertions.assertThrows(SQLException.class, () -> rsMeta.isWritable(invalidColumn));
  }
}
