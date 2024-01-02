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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.google.common.annotations.VisibleForTesting;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Timestream implementation of Connection, represents a physical connection to a database.
 */
public class TimestreamConnection implements java.sql.Connection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamConnection.class);

  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final TimestreamDatabaseMetaData databaseMetaData;
  private final Properties connectionProperties;
  private boolean metadataPreparedStatementEnabled = Boolean.parseBoolean(
      TimestreamConnectionProperty.ENABLE_METADATA_PREPARED_STATEMENT.getDefaultValue());
  private SQLWarning warnings;
  private Map<String, Class<?>> typeMap = new HashMap<>();
  @VisibleForTesting
  final ClientConfiguration clientConfiguration;
  AmazonTimestreamQuery queryClient;
  AmazonTimestreamQueryClientBuilder queryClientBuilder;

  /**
   * Constructor to seed the connection with the necessary information and configuration to
   * establish a connection.
   *
   * @param info                The connection properties.
   * @param clientConfiguration The client configuration.
   * @throws SQLException if property is not supported by the driver.
   */
  TimestreamConnection(
    @NonNull final Properties info,
    @NonNull final ClientConfiguration clientConfiguration) throws SQLException {
    this(info, clientConfiguration, HttpClients.createDefault());
  }

  /**
   * Constructor to seed the connection with the necessary information and configuration to
   * establish a connection.
   *
   * @param info                The connection properties.
   * @param clientConfiguration The client configuration.
   * @param httpClient          The HTTP Client to be used during SAML Authentication.
   * @throws SQLException if property is not supported by the driver.
   */
  TimestreamConnection(
    @NonNull final Properties info,
    @NonNull final ClientConfiguration clientConfiguration,
    @NonNull final CloseableHttpClient httpClient) throws SQLException {
    this.connectionProperties = info;
    this.clientConfiguration = clientConfiguration;
    initializeClients(info, httpClient);
    databaseMetaData = new TimestreamDatabaseMetaData(this);
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void clearWarnings() throws SQLException {
    verifyOpen();
    warnings = null;
  }

  @Override
  public void close() {
    if (!this.isClosed.getAndSet(true)) {
      LOGGER.debug("Closing the current opened connection.");
      this.queryClient.shutdown();
    }
  }

  @Override
  public void commit() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    verifyOpen();

    // Even though Arrays are supported, the only reason to create an Array in the application is to pass it as
    // a parameter which is not supported.
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.PARAMETERS_NOT_SUPPORTED);
  }

  @Override
  public Blob createBlob() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(
        LOGGER,
        Error.UNSUPPORTED_TYPE,
        Blob.class.toString());
  }

  @Override
  public Clob createClob() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(
        LOGGER,
        Error.UNSUPPORTED_TYPE,
        Clob.class.toString());
  }

  @Override
  public NClob createNClob() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(
        LOGGER,
        Error.UNSUPPORTED_TYPE,
        NClob.class.toString());
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(
        LOGGER,
        Error.UNSUPPORTED_TYPE,
        SQLXML.class.toString());
  }

  @Override
  public TimestreamStatement createStatement() throws SQLException {
    return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public TimestreamStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    verifyOpen();
    checkStatementAttributes(resultSetType, resultSetConcurrency);
    return new TimestreamStatement(this);
  }

  @Override
  public TimestreamStatement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    verifyOpen();

    // Even though Arrays are supported, the only reason to create a Struct in the application is to pass it as
    // a parameter which is not supported.
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.PARAMETERS_NOT_SUPPORTED);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    verifyOpen();

    // Always consider to be in auto-commit mode, since the driver is read-only.
    return true;
  }

  @Override
  public String getCatalog() throws SQLException {
    verifyOpen();
    LOGGER.debug("Returning null for getCatalog() since there isn't a default catalog in use.");
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    verifyOpen();
    final Properties clientInfo = new Properties();
    clientInfo.putAll(connectionProperties);
    clientInfo.putIfAbsent(
        TimestreamConnectionProperty.APPLICATION_NAME.getConnectionProperty(),
        TimestreamDriver.APPLICATION_NAME);
    return clientInfo;
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    verifyOpen();
    if (name == null) {
      LOGGER.debug("Null value is passed as name, falling back to get client info with null.");
      return null;
    }
    connectionProperties.putIfAbsent(
        TimestreamConnectionProperty.APPLICATION_NAME.getConnectionProperty(),
        TimestreamDriver.APPLICATION_NAME);
    return connectionProperties.getProperty(name);
  }

  @Override
  public int getHoldability() throws SQLException {
    verifyOpen();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public TimestreamDatabaseMetaData getMetaData() throws SQLException {
    verifyOpen();
    return databaseMetaData;
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    verifyOpen();
    return this.queryClientBuilder
        .getClientConfiguration()
        .getRequestTimeout();
  }

  @Override
  public String getSchema() throws SQLException {
    verifyOpen();
    return null;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    verifyOpen();
    return TimestreamConnection.TRANSACTION_NONE;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    verifyOpen();
    return typeMap;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    verifyOpen();
    return warnings;
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    verifyOpen();
    return true;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_TIMEOUT, timeout);
    }

    final AmazonTimestreamQuery client = this.getQueryClientBuilder()
        .withClientConfiguration(
            new ClientConfiguration(queryClientBuilder.getClientConfiguration())
                .withConnectionTimeout(timeout * 1000))
        .build();

    // Issue a query to validate the actual connection.
    try {
      client.query(new QueryRequest().withQueryString("SELECT 1"));
      return true;
    } catch (Exception e) {
      LOGGER.error("Connection is no longer valid: {}", e.getMessage());
      return false;
    } finally {
      client.shutdown();
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return (null != iface) && iface.isAssignableFrom(this.getClass());
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    verifyOpen();
    return sql;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_PREPARE_CALL);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    verifyOpen();
    return new TimestreamPreparedStatement(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_GENERATED_KEYS);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    verifyOpen();
    checkStatementAttributes(resultSetType, resultSetConcurrency);
    return new TimestreamPreparedStatement(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.PARAMETERS_NOT_SUPPORTED);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_GENERATED_KEYS);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_GENERATED_KEYS);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void rollback() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    verifyOpen();
    // Fake allowing autoCommit to be turned off, even though transactions are not supported, as some applications
    // turn this off without checking support.
    LOGGER.debug("Transactions are not supported, do nothing for setAutoCommit.");
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    verifyOpen();
    LOGGER.debug("Timestream does not support catalog. Do nothing.");
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    if (isClosed.get()) {
      final Map<String, ClientInfoStatus> failures = new HashMap<>();
      for (final String name : properties.stringPropertyNames()) {
        failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
      }
      throw Error.createSQLClientInfoException(LOGGER, Error.CONN_CLOSED, failures);
    }
    if (properties == null) {
      properties = new Properties();
      addWarning(new SQLWarning(Warning.lookup(Warning.NULL_PROPERTY)));
    }
    connectionProperties.clear();
    for (final String name : properties.stringPropertyNames()) {
      if (TimestreamConnectionProperty.isSupportedProperty(name)) {
        final String value = properties.getProperty(name);
        connectionProperties.put(name, value);
        LOGGER.debug("Successfully set client info with name {{}} and value {{}}", name, value);
      } else {
        addWarning(new SQLWarning(Warning.lookup(Warning.UNSUPPORTED_PROPERTY, name)));
      }
    }
    LOGGER.debug("Successfully set client info with all properties.");
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (isClosed.get()) {
      final Map<String, ClientInfoStatus> failure = new HashMap<>();
      failure.put(name, ClientInfoStatus.REASON_UNKNOWN);
      throw Error.createSQLClientInfoException(LOGGER, Error.CONN_CLOSED, failure);
    }
    if (TimestreamConnectionProperty.isSupportedProperty(name)) {
      connectionProperties.put(name, value);
      LOGGER.debug("Successfully set client info with name {{}} and value {{}}", name, value);
    } else {
      addWarning(new SQLWarning(Warning.lookup(Warning.UNSUPPORTED_PROPERTY, name)));
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    verifyOpen();

    if (milliseconds < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_TIMEOUT, milliseconds);
    }

    this.queryClientBuilder.getClientConfiguration().setRequestTimeout(milliseconds);
    LOGGER.debug("Network timeout is set to {} milliseconds.", milliseconds);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    verifyOpen();
    if (!readOnly) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
    }
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    verifyOpen();
    throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.UNSUPPORTED_SCHEMA);
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    verifyOpen();
    if (level != TimestreamConnection.TRANSACTION_NONE) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.TRANSACTIONS_NOT_SUPPORTED);
    }
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    verifyOpen();
    if (map == null) {
      LOGGER.debug("Null value is passed as conversion map, failing back to an empty hash map.");
      map = new HashMap<>();
    }
    typeMap = map;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }
    throw Error.createSQLException(LOGGER, Error.CANNOT_UNWRAP, iface.toString());
  }

  /**
   * Getter for the Timestream query client.
   *
   * @return the Timestream query client.
   */
  AmazonTimestreamQuery getQueryClient() {
    return queryClient;
  }

  /**
   * Getter for metadataPreparedStatementEnabled.
   *
   * @return true if metaData prepared statement is enabled; otherwise, return false.
   */
  boolean isMetadataPreparedStatementEnabled() {
    return metadataPreparedStatementEnabled;
  }

  /**
   * Gets a copy of the query client builder.
   *
   * @return a copy of the Timestream query client builder.
   */
  AmazonTimestreamQueryClientBuilder getQueryClientBuilder() {
    final AmazonTimestreamQueryClientBuilder client = AmazonTimestreamQueryClient
      .builder()
      .withCredentials(this.queryClientBuilder.getCredentials())
      .withClientConfiguration(
        new ClientConfiguration(this.queryClientBuilder.getClientConfiguration()));

    final String region = this.queryClientBuilder.getRegion();
    if (region != null) {
      client.setRegion(region);
    } else {
      client.setEndpointConfiguration(this.queryClientBuilder.getEndpoint());
    }
    return client;
  }

  /**
   * Getter for the Timestream connection properties.
   *
   * @return the connection properties.
   */
  Properties getConnectionProperties() {
    return connectionProperties;
  }

  /**
   * Check that the result set attributes are supported.
   *
   * @param resultSetType The type of result set requested.
   * @param resultSetConcurrency The concurrency of the result set requested.
   * @throws SQLFeatureNotSupportedException if the result set type is not supported.
   */
  private void checkStatementAttributes(int resultSetType, int resultSetConcurrency)
      throws SQLFeatureNotSupportedException {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.RESULT_FORWARD_ONLY);
    } else if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw Error.createSQLFeatureNotSupportedException(LOGGER, Error.READ_ONLY);
    }
  }

  /**
   * Builds the query client and validates the connection with a simple query.
   *
   * @param info                The connection properties.
   * @param credentialsProvider if property is not supported by the driver.
   * @throws SQLException if a Timestream service endpoint is specified without a signing region.
   */
  void buildQueryClientAndVerifyConnection(
    final Properties info,
    final AWSCredentialsProvider credentialsProvider) throws SQLException {
    this.queryClientBuilder = AmazonTimestreamQueryClient
      .builder()
      .withClientConfiguration(this.clientConfiguration);

    final Object endpoint = info.get(TimestreamConnectionProperty.ENDPOINT.getConnectionProperty());

    Object region = info.get(TimestreamConnectionProperty.REGION.getConnectionProperty());

    if (endpoint != null) {
      if (region == null) {
        throw Error.createSQLException(LOGGER, Error.MISSING_SERVICE_REGION);
      }

      if (endpoint.toString().isEmpty()) {
        throw Error.createSQLException(LOGGER, Error.INVALID_ENDPOINT);
      }

      queryClientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint.toString(), region.toString()));
    } else {
      if (region == null) {
        region = TimestreamConnectionProperty.REGION.getDefaultValue();
      }
      queryClientBuilder.withRegion(region.toString());
    }
    queryClientBuilder.withCredentials(credentialsProvider);

    // Build the client and issue a query to validate the actual connection.
    try {
      queryClient = this.queryClientBuilder.build();
      queryClient.query(new QueryRequest().withQueryString("SELECT 1"));
    } catch (final Exception sdkClientException) {
      throw Error.createSQLException(
        LOGGER,
        Constants.CONNECTION_FAILURE_SQL_STATE,
        sdkClientException,
        Error.CONN_FAILED);
    }
  }

  /**
   * Configures the SDK's network settings.
   *
   * @param info                The {@link Properties} used to create a connection.
   * @param clientConfiguration The query client's configuration.
   * @throws SQLException if one of the options is invalid.
   */
  private void configureSdkOptions(
    final Properties info,
    final ClientConfiguration clientConfiguration) throws SQLException {
    try {
      final int requestTimeout = Integer.parseInt(info
        .getOrDefault(
          TimestreamConnectionProperty.REQUEST_TIMEOUT.getConnectionProperty(),
          TimestreamConnectionProperty.REQUEST_TIMEOUT.getDefaultValue())
        .toString());

      final int socketTimeout = Integer.parseInt(info
        .getOrDefault(
          TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(),
          TimestreamConnectionProperty.SOCKET_TIMEOUT.getDefaultValue())
        .toString());

      if (socketTimeout < 0) {
        throw Error.createSQLException(LOGGER, Error.INVALID_TIMEOUT, socketTimeout);
      }

      final int maxConnections = Integer.parseInt(info
        .getOrDefault(
          TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(),
          TimestreamConnectionProperty.MAX_CONNECTIONS.getDefaultValue())
        .toString());

      if (maxConnections < 0) {
        throw Error.createSQLException(LOGGER, Error.INVALID_MAX_CONNECTIONS, maxConnections);
      }

      final String maxRetryCount = info
        .getOrDefault(
          TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(),
          TimestreamConnectionProperty.MAX_RETRY_COUNT.getDefaultValue())
        .toString();

      if (!maxRetryCount.isEmpty()) {
        final int maxRetryCountClient = Integer.parseInt(maxRetryCount);
        if (maxRetryCountClient < 0) {
          throw Error.createSQLException(LOGGER, Error.INVALID_MAX_RETRY_COUNT, maxRetryCountClient);
        }

        clientConfiguration.withMaxErrorRetry(maxRetryCountClient);
      }

      clientConfiguration
        .withRequestTimeout(requestTimeout)
        .withSocketTimeout(socketTimeout)
        .withMaxConnections(maxConnections);
    } catch (final NumberFormatException ne) {
      throw Error.createSQLException(LOGGER, Constants.CONNECTION_EXCEPTION_SQL_STATE, ne, Error.INVALID_NUMERIC_CONNECTION_VALUE);
    }
  }

  /**
   * Creates an {@link TimestreamOktaCredentialsProvider} instance.
   *
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @param oktaFieldsMap A {@link Map} that contains all the required fields.
   * @return an {@link TimestreamOktaCredentialsProvider} instance.
   */
  TimestreamOktaCredentialsProvider createOktaCredentialsProvider(CloseableHttpClient httpClient,
    final Map<String, String> oktaFieldsMap) {
    LOGGER.info("Creating an Okta credentials provider.");
    return new TimestreamOktaCredentialsProvider(httpClient, oktaFieldsMap);
  }

  /**
   * Creates an {@link TimestreamAzureADCredentialsProvider} instance.
   *
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @param azureADFieldsMap A {@link Map} that contains all the required fields.
   * @return an {@link TimestreamAzureADCredentialsProvider} instance.
   */
  TimestreamAzureADCredentialsProvider createAzureADCredentialsProvider(
    CloseableHttpClient httpClient, final Map<String, String> azureADFieldsMap) {
    LOGGER.info("Creating an Azure AD credentials provider.");
    return new TimestreamAzureADCredentialsProvider(httpClient, azureADFieldsMap);
  }

  /**
   * Initialize the clients for communication with the Timestream service.
   *
   * @param info The connection properties.
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @throws SQLException if property is not supported by the driver.
   */
  private void initializeClients(Properties info, CloseableHttpClient httpClient) throws SQLException {
    LOGGER.info("Initializing the client.");
    configureSdkOptions(info, this.clientConfiguration);
    buildQueryClientAndVerifyConnection(info, createCustomCredentialsProvider(info, httpClient));
    metadataPreparedStatementEnabled = Boolean.parseBoolean(info
        .getOrDefault(TimestreamConnectionProperty.ENABLE_METADATA_PREPARED_STATEMENT.getConnectionProperty(),
            TimestreamConnectionProperty.ENABLE_METADATA_PREPARED_STATEMENT.getDefaultValue())
        .toString());
      }

  /**
   * Set a new warning if there were none, or add a new warning to the end of the list.
   *
   * @param warning the {@link SQLWarning} to be set.
   */
  private void addWarning(SQLWarning warning) {
    LOGGER.warn(warning.getMessage());
    if (this.warnings == null) {
      this.warnings = warning;
      return;
    }
    this.warnings.setNextWarning(warning);
  }

  /**
   * Verify the connection is open.
   *
   * @throws SQLException if the connection is closed.
   */
  private void verifyOpen() throws SQLException {
    if (isClosed.get()) {
      throw Error.createSQLException(LOGGER, Error.CONN_CLOSED);
    }
  }

  /**
   * Creates the custom credentials provider of the class if an appropriate classname is provided.
   *
   * @param info  The connection properties.
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @return An AWSCredentialsProvider instance.
   * @throws SQLException if property is not supported by the driver.
   */
  private AWSCredentialsProvider createCustomCredentialsProvider(final Properties info, CloseableHttpClient httpClient)
      throws SQLException {
    final String idpName = info
        .getOrDefault(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(), "")
        .toString();

    // If specified, use the SAML based credentials provider.
    if (!idpName.isEmpty()) {
      switch (idpName.toLowerCase()) {
        case Constants.OKTA_IDP_NAME: {
          final Map<String, String> oktaFieldsMap = extractRequiredProperties(
            info,
            TimestreamConnectionProperty.OKTA_PROPERTY_SET);
          return createOktaCredentialsProvider(httpClient, oktaFieldsMap).createCredentialsProvider();
        }

        case Constants.AAD_IDP_NAME: {
          final Map<String, String> azureADFieldsMap = extractRequiredProperties(info,
            TimestreamConnectionProperty.AAD_PROPERTY_SET);
          return createAzureADCredentialsProvider(httpClient, azureADFieldsMap)
            .createCredentialsProvider();
        }

        default: {
          throw Error
              .createSQLException(LOGGER, Error.UNSUPPORTED_SAML_CREDENTIALS_PROVIDER, idpName);
        }
      }
    }

    final String awsCredentialsProviderClassName = info
        .getOrDefault(TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty(), "")
        .toString();

    // If specified, use the AWSCredentialsProvider.
    if (!awsCredentialsProviderClassName.isEmpty()) {
      switch (awsCredentialsProviderClassName.toLowerCase()) {
        case Constants.PROPERTIES_FILE_CREDENTIALS_PROVIDER_CLASSNAME: {
          LOGGER.info("Creating a PropertiesFileCredentialsProvider.");
          final String customCredentialsFilePath = info
            .getOrDefault(
              TimestreamConnectionProperty.CUSTOM_CREDENTIALS_FILE_PATH.getConnectionProperty(),
              "")
            .toString();
          if (customCredentialsFilePath.isEmpty()) {
            throw Error
              .createSQLException(LOGGER, Error.INVALID_CREDENTIALS_FILE_PATH);
          }

          return new PropertiesFileCredentialsProvider(customCredentialsFilePath);
        }

        case Constants.INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME: {
          LOGGER.info("Creating an InstanceProfileCredentialsProvider.");
          return new InstanceProfileCredentialsProvider(false);
        }

        default: {
          throw Error.createSQLException(LOGGER, Error.UNSUPPORTED_AWS_CREDENTIALS_PROVIDER,
              awsCredentialsProviderClassName);
        }
      }
    }

    final String accessKey = info
        .getOrDefault(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "").toString();
    final String secretKey = info
        .getOrDefault(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "").toString();

    if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
      final String sessionToken = info
          .getOrDefault(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(), "").toString();
      final AWSCredentials credentials;
      if (sessionToken.isEmpty()) {
        credentials = new BasicAWSCredentials(accessKey, secretKey);
      } else {
        credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
      }

      LOGGER.info("Creating an AWSStaticCredentialsProvider.");
      return new AWSStaticCredentialsProvider(credentials);
    }
    LOGGER.info(
        "No custom credentials provider is created. Returning the DefaultAWSCredentialsProviderChain.");
    return DefaultAWSCredentialsProviderChain.getInstance();
  }

  /**
   * Checks if the specified keys exists in the Properties instance and returns a list with the
   * values.
   *
   * @param info             A Properties instance that contains the connection properties the user
   *                         provided.
   * @param propertyNameList A String list that contains the field names that needs to be
   *                         extracted.
   * @return A Map containing the required key value pairs.
   * @throws SQLException if the values could not be found.
   */
  private Map<String, String> extractRequiredProperties(final Properties info,
    EnumSet<TimestreamConnectionProperty> propertyNameList)
    throws SQLException {
    final Map<String, String> requiredPropertiesMap = new HashMap<>();
    for (TimestreamConnectionProperty property : propertyNameList) {
      final String propertyName = property.getConnectionProperty();
      final String value = info.getOrDefault(propertyName, "").toString();
      if (!value.isEmpty()) {
        requiredPropertiesMap.put(propertyName, value);
      } else {
        throw Error.createSQLException(
          LOGGER,
          Error.MISSING_REQUIRED_IDP_PARAMETER, propertyName);
      }
    }

    return requiredPropertiesMap;
  }
}
