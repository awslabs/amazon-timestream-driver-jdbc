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
import com.amazonaws.services.timestreamquery.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Base class for TimestreamResultSet.
 */
abstract class TimestreamBaseResultSet implements java.sql.ResultSet {
  static final Calendar DEFAULT_CALENDAR = Calendar.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamBaseResultSet.class);
  private final TimestreamStatement statement;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private boolean wasNullFlag;
  private final int fieldSize;
  private final Map<String, Class<?>> typeMap;
  protected SQLWarning warnings;
  protected Iterator<Row> rowItr;
  protected TimestreamResultSetMetaData rsMeta;
  private int fetchSize;
  private Map<String, Integer> nameIndexMap;
  private List<TimestreamDataType> tsTypes;
  private List<Datum> currentRowData;
  private int rowIndex = 0;

  /**
   * Base constructor to seed with the parent statement.
   *
   * @param statement The parent statement, may be null for metadata results.
   * @param fetchSize The starting fetch size for the result set.
   */
  TimestreamBaseResultSet(final TimestreamStatement statement, final int fetchSize) {
    this(statement, fetchSize, new HashMap<>(), 0);
  }

  /**
   * Base constructor to seed with the parent statement and the default type map.
   *
   * @param statement    The parent statement, may be null for metadata results.
   * @param fetchSize    The starting fetch size for the result set.
   * @param map          The conversion map specifying the default conversions for types.
   * @param maxFieldSize The maximum number of bytes that can be returned for character and binary
   *                     column values.
   */
  TimestreamBaseResultSet(
      final TimestreamStatement statement,
      final int fetchSize,
      final Map<String, Class<?>> map,
      final int maxFieldSize) {
    this.statement = statement;
    this.fetchSize = fetchSize;
    this.typeMap = map;
    this.fieldSize = maxFieldSize;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    verifyOpen();
    if (row < 1) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.INVALID_ROW_VALUE);
    } else if (this.rowIndex > row) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
    }

    while ((this.rowIndex < row) && next());
    return !isBeforeFirst() && !isAfterLast();
  }

  @Override
  public void afterLast() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void clearWarnings() {
    this.warnings = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed.getAndSet(true)) {
      return;
    }
    doClose();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    verifyOpen();
    final Integer index = nameIndexMap.get(columnLabel);
    if (null == index) {
      throw Error.createSQLException(LOGGER, Error.INVALID_COLUMN_LABEL, columnLabel);
    }
    return index;
  }

  @Override
  public boolean first() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return getArray(columnIndex, new HashMap<>());
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getArray(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    final String value = getString(columnIndex);
    if (null == value) {
      return null;
    }
    return new ByteArrayInputStream(getString(columnIndex).getBytes(StandardCharsets.US_ASCII));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    final BigDecimal converted = getBigDecimal(columnIndex);
    if (null != converted) {
      return converted.setScale(scale, RoundingMode.HALF_UP);
    }

    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }
    return (BigDecimal) Conversions
        .convert(sourceType, JdbcType.DECIMAL, currentCell, this::addWarning);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_BINARY_STREAM);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      Blob.class.toString());
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getBlob(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return Boolean.FALSE;
    }
    if (sourceType == TimestreamDataType.BOOLEAN) {
      return Boolean.parseBoolean(currentCell.getScalarValue());
    }
    return (boolean) Conversions
        .convert(sourceType, JdbcType.BOOLEAN, currentCell, this::addWarning);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return (byte) 0;
    }
    return (byte) Conversions.convert(sourceType, JdbcType.TINYINT, currentCell, this::addWarning);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      byte[].class.toString());
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    final String value = getString(columnIndex);
    return (null == value) ? null : new StringReader(value);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      Clob.class.toString());
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getClob(findColumn(columnLabel));
  }

  @Override
  public int getConcurrency() {
    return TimestreamResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getDate(columnIndex, DEFAULT_CALENDAR);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel), DEFAULT_CALENDAR);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }

    final Date dbDate;
    if (sourceType == TimestreamDataType.DATE) {
      try {
        dbDate = Date.valueOf(currentCell.getScalarValue());
      } catch (final IllegalArgumentException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    } else {
      dbDate = (Date) Conversions
          .convert(sourceType, JdbcType.DATE, currentCell, this::addWarning);
    }

    if (cal == null) {
      cal = DEFAULT_CALENDAR;
    }
    final Instant zdtInstant = dbDate
      .toLocalDate()
      .atStartOfDay(cal.getTimeZone().toZoneId())
      .toInstant();
    return new Date(zdtInstant.toEpochMilli());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getDate(findColumn(columnLabel), cal);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return 0;
    }
    if (sourceType == TimestreamDataType.DOUBLE) {
      try {
        return Double.parseDouble(currentCell.getScalarValue());
      } catch (final NumberFormatException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    }

    return (double) Conversions.convert(sourceType, JdbcType.DOUBLE, currentCell, this::addWarning);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public int getFetchDirection() {
    return TimestreamResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    verifyOpen();
    return fetchSize;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return 0.0f;
    }

    return (float) Conversions.convert(sourceType, JdbcType.FLOAT, currentCell, this::addWarning);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public int getHoldability() {
    return TimestreamResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return 0;
    }
    if (sourceType == TimestreamDataType.INTEGER) {
      try {
        return Integer.parseInt(currentCell.getScalarValue());
      } catch (final NumberFormatException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    }

    return (int) Conversions.convert(sourceType, JdbcType.INTEGER, currentCell, this::addWarning);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return 0L;
    }
    if (sourceType == TimestreamDataType.BIGINT) {
      try {
        return Long.parseLong(currentCell.getScalarValue());
      } catch (final NumberFormatException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    }

    return (long) Conversions.convert(sourceType, JdbcType.BIGINT, currentCell, this::addWarning);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    verifyOpen();
    return this.rsMeta;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return getCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getNCharacterStream(findColumn(columnLabel));
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      NClob.class.toString());
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return getNClob(findColumn(columnLabel));
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getObject(columnIndex, this.typeMap);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel), this.typeMap);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final TimestreamDataType tsType = (tsTypes.get(columnIndex - 1));
    final Class<?> targetType = map.get(tsType.getJdbcType().name());

    if (targetType != String.class) {
      if (tsType == TimestreamDataType.ARRAY) {
        return getArray(columnIndex, map);
      }
      if (tsType == TimestreamDataType.ROW) {
        return getRow(columnIndex, map);
      }
    }

    if (targetType != null) {
      return getObject(columnIndex, targetType);
    }

    return getObject(columnIndex, tsType.getJavaClass());
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    return getObject(findColumn(columnLabel), map);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    verifyOpen();

    final JdbcType targetType = TimestreamDataType.convertClassNameToJdbcType(type.getName());

    switch (targetType) {
      case TIME: {
        return (T) getTime(columnIndex);
      }

      case TIMESTAMP: {
        return (T) getTimestamp(columnIndex);
      }

      case DATE: {
        return (T) getDate(columnIndex);
      }

      default: {
        verifyOpen();
        verifyIndex(columnIndex);

        final Datum currentCell = this.currentRowData.get(columnIndex - 1);
        final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
        if (this.checkNull(currentCell)) {
          return null;
        }
        return (T) Conversions.convert(sourceType, targetType, currentCell, this::addWarning);
      }
    }
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      Ref.class.toString());
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return getRef(findColumn(columnLabel));
  }

  @Override
  public int getRow() throws SQLException {
    verifyOpen();
    return rowIndex;
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      RowId.class.toString());
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return getRowId(findColumn(columnLabel));
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      SQLXML.class.toString());
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return getSQLXML(findColumn(columnLabel));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return (short) 0;
    }

    return (short) Conversions
        .convert(sourceType, JdbcType.SMALLINT, currentCell, this::addWarning);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public TimestreamStatement getStatement() {
    return statement;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }
    final String result;
    if (sourceType == TimestreamDataType.VARCHAR) {
      result = currentCell.getScalarValue();
    } else {
      result = (String) Conversions
          .convert(sourceType, JdbcType.VARCHAR, currentCell, this::addWarning);
    }

    if (result != null && fieldSize != 0) {
      final byte[] bytes = result.getBytes(StandardCharsets.UTF_8);
      if (fieldSize < bytes.length) {
        final byte[] trimmedBytes = new byte[fieldSize];
        System.arraycopy(bytes, 0, trimmedBytes, 0, fieldSize);
        return new String(trimmedBytes, StandardCharsets.UTF_8);
      }
    }
    return result;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getTime(columnIndex, DEFAULT_CALENDAR);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel), DEFAULT_CALENDAR);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }
    final Time dbTime;
    if (sourceType == TimestreamDataType.TIME) {
      try {
        dbTime = Time.valueOf(LocalTime.parse(currentCell.getScalarValue()));
      } catch (final DateTimeParseException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    } else {
      dbTime = (Time) Conversions
          .convert(sourceType, JdbcType.TIME, currentCell, this::addWarning);
    }

    if (cal == null) {
      cal = DEFAULT_CALENDAR;
    }
    final LocalDateTime dt = dbTime
        .toLocalTime()
        .atDate(LocalDate.of(1970, 1, 1));
    return new Time(dt.atZone(cal.getTimeZone().toZoneId()).toInstant().toEpochMilli());
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(columnIndex, DEFAULT_CALENDAR);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel), DEFAULT_CALENDAR);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);

    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }

    final Timestamp dbTimeStamp;
    if (sourceType == TimestreamDataType.TIMESTAMP) {
      try {
        dbTimeStamp = Timestamp.valueOf(
            LocalDateTime.parse(currentCell.getScalarValue(), Constants.DATE_TIME_FORMATTER));
      } catch (final DateTimeParseException e) {
        throw Error.createSQLException(LOGGER, e, Error.INCORRECT_SOURCE_TYPE_AT_CELL, sourceType);
      }
    } else {
      dbTimeStamp = (Timestamp) Conversions
          .convert(sourceType, JdbcType.TIMESTAMP, currentCell, this::addWarning);
    }

    if (cal == null) {
      cal = DEFAULT_CALENDAR;
    }

    final Instant zdt = dbTimeStamp
      .toLocalDateTime()
      .atZone(cal.getTimeZone().toZoneId()).toInstant();
    final Timestamp timestamp = new Timestamp(zdt.toEpochMilli());
    timestamp.setNanos(zdt.getNano());
    return timestamp;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public int getType() {
    return TimestreamResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(
      LOGGER,
      Error.UNSUPPORTED_TYPE,
      URL.class.toString());
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return new ByteArrayInputStream(getString(columnIndex).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    verifyOpen();
    return warnings;
  }

  @Override
  public void insertRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    verifyOpen();
    return (-1 == rowIndex);
  }

  @Override
  public boolean isClosed() {
    return this.isClosed.get();
  }

  @Override
  public boolean isFirst() throws SQLException {
    verifyOpen();
    return (0 == rowIndex);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return (null != iface) && iface.isAssignableFrom(this.getClass());
  }

  @Override
  public boolean last() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public boolean next() throws SQLException {
    verifyOpen();

    if (rowItr == null) {
      return false;
    }

    while (!rowItr.hasNext()) {
      if (!doNextPage()) {
        return false;
      }
    }

    this.currentRowData = rowItr.next().getData();
    ++rowIndex;
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
  }

  @Override
  public void refreshRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_REFRESH_ROW);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    verifyOpen();
    if (rows < 0) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
    }

    while (rows-- > 0) {
      if (!next()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean rowDeleted() {
    return false;
  }

  @Override
  public boolean rowInserted() {
    return false;
  }

  @Override
  public boolean rowUpdated() {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != TimestreamResultSet.FETCH_FORWARD) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    verifyOpen();
    if (rows < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_FETCH_SIZE, rows);
    }
    this.fetchSize = Math.min(rows, Constants.MAX_FETCH_SIZE);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }

    throw Error.createSQLException(LOGGER, Error.CANNOT_UNWRAP, iface.toString());
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int i1)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int i)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, int length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(int columnIndex, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(String columnLabel, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateClob(String columnLabel, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateLong(int columnIndex, long l) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateLong(String columnLabel, long l) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(int columnIndex, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(String columnLabel, Reader x, long length)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(int columnIndex, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNClob(String columnLabel, Reader x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNString(int columnIndex, String x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNString(String columnLabel, String x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateRow() throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
  }

  @Override
  public boolean wasNull() throws SQLException {
    verifyOpen();
    return wasNullFlag;
  }

  /**
   * Retrieve the array with elements converted to the target type, if there is mapping specified in
   * the given conversion map.
   *
   * @param columnIndex The 1-based column index.
   * @param map         The conversion map specifying the target types for elements in the array.
   * @return the {@link Array}.
   * @throws SQLException if an error occurred while retrieving the data as an {@link Array}.
   */
  private Array getArray(final int columnIndex, final Map<String, Class<?>> map)
      throws SQLException {
    verifyOpen();
    verifyIndex(columnIndex);
    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    final TimestreamDataType sourceType = this.tsTypes.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }

    final List<Object> array;
    if (sourceType == TimestreamDataType.ARRAY) {
      array = currentCell
          .getArrayValue()
          .stream()
          .map(Object.class::cast)
          .collect(Collectors.toList());
    } else {
      array = (List<Object>) Conversions
          .convert(sourceType, JdbcType.ARRAY, currentCell, this::addWarning);
    }

    final Type origType = this.rsMeta.getTimestreamType(columnIndex);
    final ColumnInfo columnInfo = origType.getArrayColumnInfo();
    if (columnInfo != null) {
      return new TimestreamArray(array, columnInfo.getType(), this, map);
    }

    return new TimestreamArray(array, origType, this, map);
  }

  /**
   * Retrieve the struct with elements converted to the target type, if there is mapping specified
   * in the given conversion map.
   *
   * @param columnIndex The 1-based column index.
   * @param map         The conversion map specifying the target types for elements in the array.
   * @return the {@link Struct}.
   * @throws SQLException if an error occurred while retrieving the data as an {@link Struct}.
   */
  private Struct getRow(final int columnIndex, final Map<String, Class<?>> map)
      throws SQLException {
    final Datum currentCell = this.currentRowData.get(columnIndex - 1);
    if (this.checkNull(currentCell)) {
      return null;
    }

    final Row row = currentCell.getRowValue();
    final Type origType = this.rsMeta.getTimestreamType(columnIndex);
    final List<ColumnInfo> columnInfo = origType.getRowColumnInfo();
    // The driver does not support converting other data types into a JdbcType.STRUCT
    // If columnInfo is null, the source data is not a row, the convertData will throw
    // an exception before reaching this line.
    if (columnInfo == null) {
      throw new RuntimeException(
          Error.getErrorMessage(LOGGER, Error.INVALID_DATA_AT_ROW, origType));
    }

    return new TimestreamStruct(row.getData(), columnInfo, this, map);
  }

  /**
   * Close this result set.
   *
   * @throws SQLException if there is an error closing the result set.
   */
  protected abstract void doClose() throws SQLException;

  /**
   * Retrieve the next page of the results.
   *
   * @return {@code true} if there is another page; {@code false} otherwise.
   * @throws SQLException if there is an error retrieving the next page.
   */
  protected abstract boolean doNextPage() throws SQLException;

  /**
   * Create the column metadata for a result set.
   *
   * @param columnInfo The Timestream column information.
   * @return The column metadata for a result set.
   */
  protected TimestreamResultSetMetaData createColumnMetadata(List<ColumnInfo> columnInfo) {
    final List<TimestreamResultSetMetaData.ColInfo> colInfo = new ArrayList<>();
    this.nameIndexMap = new HashMap<>();
    this.tsTypes = new ArrayList<>();

    for (int i = 0; i < columnInfo.size(); ++i) {
      final ColumnInfo info = columnInfo.get(i);
      nameIndexMap.put(info.getName(), i + 1);
      colInfo.add(new TimestreamResultSetMetaData.ColInfo(
        info.getType(),
        info.getName()));
      tsTypes.add(TimestreamDataType.fromType(info.getType()));
    }

    return new TimestreamResultSetMetaData(colInfo);
  }

  /**
   * Verify the result set is open.
   *
   * @throws SQLException if the result set is closed.
   */
  protected void verifyOpen() throws SQLException {
    if (isClosed.get()) {
      throw Error.createSQLException(LOGGER, Error.RESULT_SET_CLOSED);
    }
  }

  /**
   * Verify if the given column index is valid.
   *
   * @param column The 1-based column index.
   * @throws SQLException If the column index is not valid for this result.
   */
  protected void verifyIndex(int column) throws SQLException {
    if ((1 > column) || (column > currentRowData.size())) {
      throw Error.createSQLException(LOGGER, Error.INVALID_INDEX, column, currentRowData.size());
    }
  }

  /**
   * Set wasNullFlag to true if current cell is null.
   *
   * @param datum A cell of data in the current row.
   * @return true if cell is null; false otherwise.
   */
  private boolean checkNull(Datum datum) {
    wasNullFlag = Boolean.TRUE.equals(datum.getNullValue());
    return wasNullFlag;
  }

  /**
   * Create a Datum object with the specified integer value.
   *
   * @param value the value to create the Datum with.
   * @return the newly created Datum object with an integer scalar value.
   */
  protected static Datum createDatum(int value) {
    return createDatum(String.valueOf(value));
  }

  /**
   * Create a Datum object with the specified String value.
   *
   * @param value the value to create the Datum with.
   * @return the newly created Datum object with a String scalar value.
   */
  protected static Datum createDatum(String value) {
    return new Datum().withScalarValue(value);
  }

  /**
   * Set a new warning if there were none, or add a new warning to the end of the list.
   *
   * @param warning The {@link SQLWarning} to add.
   */
  void addWarning(final SQLWarning warning) {
    LOGGER.warn(warning.getMessage());
    if (this.warnings == null) {
      this.warnings = warning;
      return;
    }

    this.warnings.setNextWarning(warning);
  }
}
