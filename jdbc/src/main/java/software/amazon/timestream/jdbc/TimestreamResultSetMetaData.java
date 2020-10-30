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

import com.amazonaws.services.timestreamquery.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;

/**
 * TimestreamResultSetMetaData class that contains the meta data information of the result set.
 */
public class TimestreamResultSetMetaData implements java.sql.ResultSetMetaData {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamResultSetMetaData.class);
  /**
   * Class containing the column's name and the Timestream data type.
   */
  static class ColInfo {
    private final Type type;
    private final String name;

    /**
     * Constructor for the column metadata.
     *
     * @param type The type of the column.
     * @param name The name of the column.
     */
    ColInfo(Type type, String name) {
      this.type = type;
      this.name = name;
    }

    /**
     * Gets the Timestream data type.
     *
     * @return the Timestream data type.
     */
    Type getType() {
      return type;
    }
  }

  private final List<ColInfo> columnInfo;

  /**
   * Constructor.
   *
   * @param columnInfo The column metadata for the parent result set.
   */
  TimestreamResultSetMetaData(List<ColInfo> columnInfo) {
    this.columnInfo = columnInfo;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    verifyIndex(column);
    LOGGER.debug("Return null for catalog name since there are no default catalogs.");
    return null;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    verifyIndex(column);

    final ColInfo col = columnInfo.get(column - 1);

    return TimestreamDataType.getJavaClassName(col.type);
  }

  @Override
  public int getColumnCount() {
    return columnInfo.size();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    verifyIndex(column);
    switch (getColumnType(column)) {
      case Types.BOOLEAN: {
        // False has 5 characters.
        return 5;
      }

      case Types.INTEGER: {
        return 11;
      }

      case Types.BIGINT: {
        return 20;
      }

      case Types.DOUBLE: {
        return 15;
      }

      case Types.DATE: {
        return 10;
      }

      case Types.TIME: {
        // Nanosecond precision.
        return 18;
      }

      case Types.TIMESTAMP: {
        // Nanosecond precision.
        return 29;
      }

      case Types.STRUCT:
      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.VARCHAR: {
        return Integer.MAX_VALUE;
      }

      default: {
        throw new SQLFeatureNotSupportedException(
          Error.lookup(Error.UNSUPPORTED_TYPE, getColumnClassName(column)));
      }
    }
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    verifyIndex(column);
    return columnInfo.get(column - 1).name;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Unknown Timestream data types are mapped to {@link Types#VARCHAR} to provide more useful
   * information.
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    verifyIndex(column);
    final ColInfo col = columnInfo.get(column - 1);

    return TimestreamDataType.getJdbcTypeCode(col.type).jdbcCode;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    verifyIndex(column);
    return TimestreamDataType.fromType(columnInfo.get(column - 1).type).toString();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    verifyIndex(column);
    return TimestreamDataType.fromType(columnInfo.get(column - 1).type).getPrecision();
  }

  @Override
  public int getScale(int column) throws SQLException {
    verifyIndex(column);
    return 0;
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    verifyIndex(column);
    LOGGER.debug("Returning null for schema name since schemas are not supported.");
    return null;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    verifyIndex(column);
    LOGGER.debug("Returning null for table name since there are no default tables.");
    return null;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    verifyIndex(column);
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    verifyIndex(column);
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    verifyIndex(column);
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    verifyIndex(column);
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    verifyIndex(column);
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    verifyIndex(column);
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    verifyIndex(column);
    return true;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    verifyIndex(column);
    final int type = getColumnType(column);
    return (type == Types.INTEGER) ||
      (type == Types.BIGINT) ||
      (type == Types.DOUBLE);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return (null != iface) && iface.isAssignableFrom(this.getClass());
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    verifyIndex(column);
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }

    throw Error.createSQLException(LOGGER, Error.CANNOT_UNWRAP, iface.toString());
  }

  /**
   * Gets the Timestream data type of the data stored at the specified column index.
   *
   * @param column The 1-based column index.
   * @return the Timestream data type.
   * @throws SQLException if the column index is invalid.
   */
  Type getTimestreamType(int column) throws SQLException {
    verifyIndex(column);
    return this.columnInfo.get(column - 1).getType();
  }

  /**
   * Verify if the given column index is valid.
   *
   * @param column the 1-based column index.
   * @throws SQLException if the column index is not valid for this result.
   */
  private void verifyIndex(int column) throws SQLException {
    if ((1 > column) || (column > columnInfo.size())) {
      throw Error.createSQLException(LOGGER, Error.INVALID_INDEX, column, columnInfo.size());
    }
  }
}
