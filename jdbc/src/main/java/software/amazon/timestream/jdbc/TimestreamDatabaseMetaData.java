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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.StringJoiner;

public class TimestreamDatabaseMetaData implements java.sql.DatabaseMetaData {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamDatabaseMetaData.class);
  private static final int MAX_CATALOG_NAME_LENGTH = 60;
  private static final int MAX_TABLE_NAME_LENGTH = 60;
  // Max query length is actually 262,144 bytes of UTF-8 encoded data, but be pessimistic in terms of length in case
  // all characters are 4-byte characters.
  private static final int MAX_STATEMENT_LENGTH = 65536;
  private static final String NUMERIC_FUNCTIONS = String.join(",",
    new String[]{
      "ABS",
      "ACOS",
      "ASIN",
      "ATAN",
      "ATAN2",
      "CBRT",
      "CEIL",
      "CEILING",
      "COS",
      "COSH",
      "DEGREES",
      "E",
      "EXP",
      "FLOOR",
      "LN",
      "LOG2",
      "LOG10",
      "MOD",
      "PI",
      "POW",
      "POWER",
      "RADIANS",
      "RAND",
      "RANDOM",
      "ROUND",
      "SIGN",
      "SIN",
      "SQRT",
      "TAN",
      "TANH",
      "TRUNCATE"
    });
  private static final String STRING_FUNCTIONS = String.join(",",
    new String[]{
      "CHR",
      "CODEPOINT",
      "CONCAT",
      "LENGTH",
      "LOWER",
      "LPAD",
      "LTRIM",
      "REPLACE",
      "REVERSE",
      "RPAD",
      "RTRIM",
      "SPLIT",
      "STRPOS",
      "STRRPOS",
      "POSITION",
      "SUBSTR",
      "TRIM",
      "UPPER"
    });
  private static final String SYSTEM_FUNCTIONS = String.join(",",
    new String[]{
    });
  private static final String TIMEDATE_FUNCTIONS = String.join(",",
    new String[]{
      "CURRENT_DATE",
      "CURRENT_TIMESTAMP",
      "DATE_TRUNC",
      "NOW"
    });
  private final TimestreamConnection connection;

  /**
   * Constructor for seeding the database metadata with the parent connection.
   *
   * @param connection The parent connection.
   */
  public TimestreamDatabaseMetaData(TimestreamConnection connection) {
    this.connection = connection;
  }

  @Override
  public boolean allProceduresAreCallable() {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return true;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() {
    return false;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
    String attributeNamePattern) {
    return new TimestreamAttributesResultSet();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
    boolean nullable) {
    return new TimestreamBestRowIdentifierResultSet();
  }

  @Override
  public String getCatalogSeparator() {
    return ".";
  }

  @Override
  public String getCatalogTerm() {
    return "database";
  }

  @Override
  public ResultSet getCatalogs(){
    LOGGER.debug("Catalogs are not supported. Returning an empty result set.");
    return new TimestreamDatabasesResultSet();
  }

  @Override
  public ResultSet getClientInfoProperties() {
    return new TimestreamPropertiesResultSet();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
    String columnNamePattern) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_COLUMN_PRIVILEGES);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
    String columnNamePattern) throws SQLException {
    return new TimestreamColumnsResultSet(
      connection,
      schemaPattern,
      tableNamePattern,
      convertPattern(columnNamePattern));
  }

  @Override
  public TimestreamConnection getConnection() {
    return connection;
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
    String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_CROSS_REFERENCE);
  }

  @Override
  public int getDatabaseMajorVersion() {
    // TODO: dynamically look up database version.
    return 1;
  }

  @Override
  public int getDatabaseMinorVersion() {
    // TODO: dynamically look up database version.
    return 0;
  }

  @Override
  public String getDatabaseProductName() {
    return "Amazon Timestream";
  }

  @Override
  public String getDatabaseProductVersion() {
    // TODO: dynamically look up the database version.
    return "1.0";
  }

  @Override
  public int getDefaultTransactionIsolation() {
    return TimestreamConnection.TRANSACTION_NONE;
  }

  @Override
  public int getDriverMajorVersion() {
    return TimestreamDriver.DRIVER_MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return TimestreamDriver.DRIVER_MINOR_VERSION;
  }

  @Override
  public String getDriverName() {
    return "Amazon Timestream JDBC";
  }

  @Override
  public String getDriverVersion() {
    return TimestreamDriver.DRIVER_VERSION;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_EXPORTED_KEYS);
  }

  @Override
  public String getExtraNameCharacters() {
    return "@";
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaNamePattern,
    String tableNamePattern, String columnNamePattern) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_FUNCTION_COLUMNS);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_FUNCTIONS);
  }

  @Override
  public String getIdentifierQuoteString() {
    return "\"";
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    return new TimestreamImportedKeysResultSet();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
    boolean approximate) {
    return new TimestreamIndexResultSet();
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 2;
  }

  @Override
  public int getMaxBinaryLiteralLength() {
    LOGGER.debug("Binary is not a supported data type.");
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() {
    return MAX_CATALOG_NAME_LENGTH;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() {
    return 0;
  }

  @Override
  public int getMaxConnections() {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() {
    return 0;
  }

  @Override
  public int getMaxIndexLength() {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }

  @Override
  public int getMaxRowSize() {
    // Maximum result size is 1MB, so therefore a singe row cannot exceed this.
    return 1048576;
  }

  @Override
  public int getMaxSchemaNameLength() {
    return 0;
  }

  @Override
  public int getMaxStatementLength() {
    return MAX_STATEMENT_LENGTH;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() {
    return MAX_TABLE_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() {
    return 1;
  }

  @Override
  public int getMaxUserNameLength() {
    return 0;
  }

  @Override
  public String getNumericFunctions() {
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    return new TimestreamPrimaryKeysResultSet();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
    String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_PROCEDURE_COLUMNS);
  }

  @Override
  public String getProcedureTerm() {
    LOGGER.debug("Procedures are not supported. Returning null.");
    return null;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
    LOGGER.debug("Procedures are not supported. Returning an empty result set.");
    return new TimestreamProceduresResultSet();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
    String columnNamePattern) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_PSEUDO_COLUMNS);
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public String getSQLKeywords() {
    return "DATABASE,TIMESERIES,MEASURES";
  }

  @Override
  public int getSQLStateType() {
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public String getSchemaTerm() {
    LOGGER.debug("Schemas are not supported. Returning an empty string.");
    return "";
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return new TimestreamSchemasResultSet(this.connection, null);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return new TimestreamSchemasResultSet(this.connection, schemaPattern);
  }

  @Override
  public String getSearchStringEscape() {
    return "%";
  }

  @Override
  public String getStringFunctions() {
    return STRING_FUNCTIONS;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_SUPER_TABLES);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String tableNamePattern)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_SUPER_TYPES);
  }

  @Override
  public String getSystemFunctions() {
    return SYSTEM_FUNCTIONS;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_TABLE_PRIVILEGES);
  }

  @Override
  public ResultSet getTableTypes() {
    return new TimestreamTableTypesResultSet();
  }

  @Override
  public ResultSet getTables(
    String catalog,
    String schemaPattern,
    String tableNamePattern,
    String[] types) throws SQLException {
    return new TimestreamTablesResultSet(connection, schemaPattern, tableNamePattern, types);
  }

  @Override
  public String getTimeDateFunctions() {
    return TIMEDATE_FUNCTIONS;
  }

  @Override
  public ResultSet getTypeInfo() {
    return new TimestreamTypeInfoResultSet();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
    int[] types) throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_USER_DEFINED_TYPES);
  }

  @Override
  public String getURL() {
    final StringJoiner joiner = new StringJoiner(";");
    for (final TimestreamConnectionProperty value : TimestreamConnectionProperty.values()) {
      final Object property = this.getConnection()
        .getConnectionProperties()
        .get(value.getConnectionProperty());

      final String propertyStringPrefix = value.getConnectionProperty() + "=";

      if (property != null) {
        if (TimestreamConnectionProperty.SENSITIVE_PROPERTIES.contains(value)) {
          joiner.add(propertyStringPrefix + "***Sensitive Data Redacted***");
        } else {
          joiner.add(propertyStringPrefix + property);
        }
      } else if (value == TimestreamConnectionProperty.REGION) {
        joiner.add(propertyStringPrefix + value.getDefaultValue());
      }
    }
    LOGGER.debug("Sensitive data in the URL should be redacted.");
    return Constants.URL_PREFIX + Constants.URL_BRIDGE + joiner.toString();
  }

  @Override
  public String getUserName() {
    LOGGER.debug("User name is not supported. Returning null.");
    return null;
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
    throws SQLException {
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_VERSION_COLUMNS);
  }

  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  @Override
  public boolean isCatalogAtStart() {
    return true;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return (null != iface) && iface.isAssignableFrom(this.getClass());
  }

  @Override
  public boolean locatorsUpdateCopy() {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return true;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() {
    return true;
  }

  @Override
  public boolean supportsConvert() {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() {
    return true;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() {
    // Not applicable to Timestream.
    return false;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    // Not applicable to Timestream.
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() {
    // Not applicable to Timestream.
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return (type == ResultSet.TYPE_FORWARD_ONLY) && (concurrency == ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) {
    // No transaction support.
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) {
    return (ResultSet.TYPE_FORWARD_ONLY == type);
  }

  @Override
  public boolean supportsSavepoints() {
    return false;
  }

  /**
   * Retrieves whether a schema name can be used in a data manipulation statement.
   * @return true since functionality is supported
   */
  @Override
  public boolean supportsSchemasInDataManipulation() {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return true;
  }

  @Override
  public boolean supportsTableCorrelationNames() {
    return true;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) {
    return false;
  }

  @Override
  public boolean supportsTransactions() {
    return false;
  }

  @Override
  public boolean supportsUnion() {
    return true;
  }

  @Override
  public boolean supportsUnionAll() {
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }

    throw Error.createSQLException(LOGGER, Error.CANNOT_UNWRAP, iface.toString());
  }

  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean usesLocalFiles() {
    return false;
  }

  /**
   * Convert a SQL pattern into a regular expression.
   *
   * @param pattern the SQL pattern to convert.
   * @return the equivalent regular expression.
   */
  private static String convertPattern(String pattern) {
    if (Strings.isNullOrEmpty(pattern)) {
      return pattern;
    }

    return "^" + pattern
      .replaceAll("\\[", "\\[")
      .replaceAll("]", "\\]")
      .replaceAll("\\.", "\\.")
      .replaceAll("\\*", "\\*")
      .replaceAll("(?<!\\\\)_", ".")
      .replaceAll("\\\\_", "_")
      .replaceAll("(?<!\\\\)%", ".*")
      .replaceAll("\\\\%", "%") +
      "$";
  }
}
