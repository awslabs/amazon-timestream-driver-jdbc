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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataSource implementation of Timestream, supports pooled and non-pooled connections.
 */
public class TimestreamDataSource implements javax.sql.DataSource,
  javax.sql.ConnectionPoolDataSource, ConnectionEventListener {

  private static final Logger LOGGER = Logger
    .getLogger("TimestreamDataSource");

  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    LOGGER.setLevel(Level.FINEST);
  }

  @VisibleForTesting
  final ClientConfiguration clientConfiguration = new ClientConfiguration()
    .withUserAgentSuffix(
      Constants.UA_ID_PREFIX + TimestreamDriver.DRIVER_VERSION + TimestreamDriver.APP_NAME_SUFFIX);
  @VisibleForTesting
  final Map<Properties, LinkedList<TimestreamConnection>> availablePools = new HashMap<>();
  private final Properties samlAuthenticationProperties = new Properties();
  private final Properties sdkProperties = new Properties();
  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  private String region;
  private String credentialsProviderClass;
  private String credentialsFilePath;
  private String endpoint;
  private boolean isEnableMetaDataPreparedStatement =
    Boolean.parseBoolean(
      TimestreamConnectionProperty.ENABLE_METADATA_PREPARED_STATEMENT.getDefaultValue());

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(accessKeyId, secretAccessKey);
  }

  @Override
  public Connection getConnection(String accessKey, String secretKey) throws SQLException {
    LOGGER.fine("Instantiating a TimestreamConnection from TimestreamDataSource.");
    return createTimestreamConnection(getProperties(accessKey, secretKey));
  }

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    return getPooledConnection(accessKeyId, secretAccessKey);
  }

  @Override
  public PooledConnection getPooledConnection(String accessKey, String secretKey)
    throws SQLException {
    final Properties properties = getProperties(accessKey, secretKey);
    final List<TimestreamConnection> poolForCredentials = availablePools
      .computeIfAbsent(properties, k -> new LinkedList<>());

    TimestreamPooledConnection timestreamPooledConnection = null;
    while (!poolForCredentials.isEmpty() && (timestreamPooledConnection == null)) {
      final TimestreamConnection connection = poolForCredentials.remove(0);
      if (!connection.isClosed()) {
        LOGGER.info("Returning an open connection from the connection pool.");
        timestreamPooledConnection = createTimestreamPooledConnection(connection);
      }
    }

    if (timestreamPooledConnection == null) {
      LOGGER.info("Could not find a connection in the pool, creating a connection.");
      timestreamPooledConnection = createTimestreamPooledConnection(
        createTimestreamConnection(getProperties(accessKey, secretKey)));
    }

    timestreamPooledConnection.addConnectionEventListener(this);
    return timestreamPooledConnection;
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    // NOOP
  }

  @Override
  public int getLoginTimeout() {
    return this.clientConfiguration.getConnectionTimeout() / Constants.NUM_MILLISECONDS_IN_SECOND;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    if (seconds < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_TIMEOUT, seconds);
    }

    this.clientConfiguration.setConnectionTimeout(seconds * Constants.NUM_MILLISECONDS_IN_SECOND);
  }

  @Override
  public Logger getParentLogger() {
    return LOGGER;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return (null != iface) && iface.isAssignableFrom(this.getClass());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }

    throw Error.createSQLException(LOGGER, Error.CANNOT_UNWRAP, iface.toString());
  }

  /**
   * Getter for the AWS access key ID.
   *
   * @return the AWS access key ID.
   */
  public String getAccessKeyId() {
    return accessKeyId;
  }

  /**
   * Setter for the AWS access key ID.
   *
   * @param accessKeyId The AWS access key ID.
   */
  public void setAccessKeyId(final String accessKeyId) {
    this.accessKeyId = accessKeyId;
  }

  /**
   * Getter for the AWS secret access key.
   *
   * @return the AWS secret access key.
   */
  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Setter for the AWS secret access key.
   *
   * @param secretAccessKey The AWS secret access key.
   */
  public void setSecretAccessKey(final String secretAccessKey) {
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * Getter for the temporary AWS session token.
   *
   * @return the temporary session token required to access a database with multi-factor
   * authentication (MFA) enabled.
   */
  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Setter for the AWS session token.
   *
   * @param sessionToken The temporary session token required to access a database with multi-factor
   *                     authentication (MFA) enabled.
   */
  public void setSessionToken(final String sessionToken) {
    this.sessionToken = sessionToken;
  }

  /**
   * Getter for the database's region.
   *
   * @return the database's region.
   */
  public String getRegion() {
    return region;
  }

  /**
   * Setter for the AWS database region.
   *
   * @param region The database's region.
   */
  public void setRegion(final String region) {
    this.region = region;
  }

  /**
   * Getter for the Timestream service endpoint.
   *
   * @return the service endpoint.
   */
  public String getEndpoint() {
    return endpoint;
  }

  /**
   * Setter for the Timestream service endpoint.
   *
   * @param endpoint The service endpoint.
   */
  public void setEndpoint(final String endpoint) {
    this.endpoint = endpoint;
  }

  /**
   * Getter for the enableMetaDataPreparedStatement.
   *
   * @return enableMetaDataPreparedStatement.
   */
  public boolean isEnableMetaDataPreparedStatement() {
    return isEnableMetaDataPreparedStatement;
  }

  /**
   * Setter for tracking if the driver can return metadata for PreparedStatements.
   *
   * @param enableMetaDataPreparedStatement A flag that record enableMetaDataPreparedStatement.
   */
  public void setEnableMetaDataPreparedStatement(final boolean enableMetaDataPreparedStatement) {
    this.isEnableMetaDataPreparedStatement = enableMetaDataPreparedStatement;
  }

  /**
   * Gets credentials provider class.
   *
   * @return the credentials provider class
   */
  public String getCredentialsProviderClass() {
    return credentialsProviderClass;
  }

  /**
   * Sets credentials provider class. The class has to be one of `PropertiesFileCredentialsProvider`
   * or `InstanceProfileCredentialsProvider` to use for authentication.
   *
   * @param credentialsProviderClass the credentials provider class
   */
  public void setCredentialsProviderClass(final String credentialsProviderClass) {
    this.credentialsProviderClass = credentialsProviderClass;
  }

  /**
   * Gets the path to a properties file containing AWS security credentials `accessKey`
   * and `secretKey`.
   *
   * @return the credentials file path
   */
  public String getCredentialsFilePath() {
    return credentialsFilePath;
  }

  /**
   * Sets the path to a properties file containing AWS security credentials `accessKey`
   * and `secretKey`. This is only required if `AwsCredentialsProviderClass` is specified as
   * `PropertiesFileCredentialsProvider`.
   *
   * @param credentialsFilePath the credentials file path
   */
  public void setCredentialsFilePath(final String credentialsFilePath) {
    this.credentialsFilePath = credentialsFilePath;
  }

  /**
   * Gets the Identity Provider (IdP) name to use for SAML-based authentication. One of `Okta` or
   * `AzureAD`.
   *
   * @return the Identity Provider (IdP) name to use for SAML-based authentication. One of `Okta` or
   * `AzureAD`.
   */
  public String getIdpName() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(),
        TimestreamConnectionProperty.IDP_NAME.getDefaultValue())
      .toString();
  }

  /**
   * Sets the Identity Provider (IdP) name to use for SAML-based authentication. One of `Okta` or
   * `AzureAD`.
   *
   * @param idpName the Identity Provider (IdP) name to use for SAML-based authentication. One of
   *                `Okta` or `AzureAD`.
   */
  public void setIdpName(final String idpName) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(),
      idpName);
  }

  /**
   * Gets the hostname of the specified IdP.
   *
   * @return the IdP hostname.
   */
  public String getIdpHost() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.IDP_HOST.getConnectionProperty(),
        TimestreamConnectionProperty.IDP_HOST.getDefaultValue())
      .toString();
  }

  /**
   * Sets the hostname of the specified IdP.
   *
   * @param idpHost the IdP hostname.
   */
  public void setIdpHost(final String idpHost) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.IDP_HOST.getConnectionProperty(),
      idpHost);
  }

  /**
   * Gets the user name for the specified IdP account.
   *
   * @return the user name for the specified IdP account.
   */
  public String getIdpUserName() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(),
        TimestreamConnectionProperty.IDP_USERNAME.getDefaultValue())
      .toString();
  }

  /**
   * Sets the user name for the specified IdP account.
   *
   * @param idpUserName he user name for the specified IdP account.
   */
  public void setIdpUserName(final String idpUserName) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(),
      idpUserName);
  }

  /**
   * Gets the password for the specified IdP account.
   *
   * @return the password for the specified IdP account.
   */
  public String getIdpPassword() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(),
        TimestreamConnectionProperty.IDP_PASSWORD.getDefaultValue())
      .toString();
  }

  /**
   * Sets the password for the specified IdP account.
   *
   * @param idpPassword the password for the specified IdP account.
   */
  public void setIdpPassword(final String idpPassword) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(),
      idpPassword);
  }

  /**
   * Gets the unique Okta-provided ID associated with the Timestream application.
   *
   * @return the unique Okta-provided ID associated with the Timestream application.
   */
  public String getOktaAppId() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.OKTA_APP_ID.getConnectionProperty(),
        TimestreamConnectionProperty.OKTA_APP_ID.getDefaultValue())
      .toString();
  }

  /**
   * Sets the unique Okta-provided ID associated with the Timestream application.
   *
   * @param oktaAppId the unique Okta-provided ID associated with the Timestream application.
   */
  public void setOktaAppId(final String oktaAppId) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.OKTA_APP_ID.getConnectionProperty(),
      oktaAppId);
  }

  /**
   * Gets the Amazon Resource Name (ARN) of the role that the caller is assuming.
   *
   * @return the the Amazon Resource Name (ARN) of the role that the caller is assuming.
   */
  public String getAwsRoleArn() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(),
        TimestreamConnectionProperty.AWS_ROLE_ARN.getDefaultValue())
      .toString();
  }

  /**
   * Sets the Amazon Resource Name (ARN) of the role that the caller is assuming.
   *
   * @param awsRoleArn the Amazon Resource Name (ARN) of the role that the caller is assuming.
   */
  public void setAwsRoleArn(final String awsRoleArn) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(),
      awsRoleArn);
  }

  /**
   * Gets the Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
   *
   * @return the Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
   */
  public String getIdpArn() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(),
        TimestreamConnectionProperty.IDP_ARN.getDefaultValue())
      .toString();
  }

  /**
   * Sets the Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
   *
   * @param idpArn the Amazon Resource Name (ARN) of the SAML provider in IAM that describes the
   *               IdP.
   */
  public void setIdpArn(final String idpArn) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(),
      idpArn);
  }

  /**
   * Gets the unique id of the registered application on Azure AD.
   *
   * @return the unique id of the registered application on Azure AD.
   */
  public String getAadAppId() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.AAD_APP_ID.getConnectionProperty(),
        TimestreamConnectionProperty.AAD_APP_ID.getDefaultValue())
      .toString();
  }

  /**
   * Sets the unique id of the registered application on Azure AD.
   *
   * @param aadAppId the unique id of the registered application on Azure AD.
   */
  public void setAadAppId(final String aadAppId) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.AAD_APP_ID.getConnectionProperty(),
      aadAppId);
  }

  /**
   * Gets the client secret associated with the registered application on Azure AD used to authorize
   * fetching tokens.
   *
   * @return the client secret associated with the registered application on Azure AD used to
   * authorize fetching tokens.
   */
  public String getAadClientSecret() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.AAD_CLIENT_SECRET.getConnectionProperty(),
        TimestreamConnectionProperty.AAD_CLIENT_SECRET.getDefaultValue())
      .toString();
  }

  /**
   * Sets the client secret associated with the registered application on Azure AD used to authorize
   * fetching tokens.
   *
   * @param aadClientSecret the client secret associated with the registered application on Azure AD
   *                        used to authorize fetching tokens.
   */
  public void setAadClientSecret(final String aadClientSecret) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.AAD_CLIENT_SECRET.getConnectionProperty(),
      aadClientSecret);
  }

  /**
   * Gets Azure Active Directory tenant ID.
   *
   * @return the Azure Active Directory tenant ID.
   */
  public String getAadTenantId() {
    return samlAuthenticationProperties
      .getOrDefault(
        TimestreamConnectionProperty.AAD_TENANT_ID.getConnectionProperty(),
        TimestreamConnectionProperty.AAD_TENANT_ID.getDefaultValue())
      .toString();
  }

  /**
   * Sets Azure Active Directory tenant ID.
   *
   * @param aadTenantId the Azure Active Directory tenant ID.
   */
  public void setAadTenantId(final String aadTenantId) {
    samlAuthenticationProperties.put(
      TimestreamConnectionProperty.AAD_TENANT_ID.getConnectionProperty(),
      aadTenantId);
  }

  /**
   * Gets the time in milliseconds the client will wait for a query request before timing out.
   *
   * @return the request timeout value in milliseconds.
   */
  public int getRequestTimeout() {
    return Integer.parseInt(sdkProperties
      .getOrDefault(
        TimestreamConnectionProperty.REQUEST_TIMEOUT.getConnectionProperty(),
        TimestreamConnectionProperty.REQUEST_TIMEOUT.getDefaultValue())
      .toString());
  }

  /**
   * Sets the time in milliseconds the client will wait for a query request before timing out.
   * A non-positive value disables this feature.
   *
   * @param requestTimeout the request timeout value in milliseconds.
   * @see ClientConfiguration#setRequestTimeout(int)
   */
  public void setRequestTimeout(int requestTimeout) {
    sdkProperties.setProperty(
      TimestreamConnectionProperty.REQUEST_TIMEOUT.getConnectionProperty(),
      String.valueOf(requestTimeout));
  }

  /**
   * Gets the time in milliseconds the client will wait for data to be transferred over an open
   * connection before timing out.
   *
   * @return the socket timeout value in milliseconds.
   * @see ClientConfiguration#getSocketTimeout()
   */
  public int getSocketTimeout() {
    return Integer.parseInt(sdkProperties
      .getOrDefault(
        TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(),
        TimestreamConnectionProperty.SOCKET_TIMEOUT.getDefaultValue())
      .toString());
  }

  /**
   * Sets the time in milliseconds the client will wait for data to be transferred over an open
   * connection before timing out.
   *
   * @param socketTimeout The socket timeout value in milliseconds.
   * @see ClientConfiguration#setSocketTimeout(int)
   * @throws SQLException if the timeout value is negative.
   */
  public void setSocketTimeout(int socketTimeout) throws SQLException {
    if (socketTimeout < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_TIMEOUT, socketTimeout);
    }

    sdkProperties.setProperty(
      TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(),
      String.valueOf(socketTimeout));
  }

  /**
   * Gets the maximum number of allowed concurrently opened HTTP connections to the Timestream
   * service.
   *
   * @see ClientConfiguration#getMaxConnections()
   * @return the maximum number of allowed opened HTTP connections.
   */
  public int getMaxConnections() {
    return Integer.parseInt(sdkProperties
      .getOrDefault(
        TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(),
        TimestreamConnectionProperty.MAX_CONNECTIONS.getDefaultValue())
      .toString());
  }

  /**
   * Sets the maximum number of allowed concurrently opened HTTP connections to the Timestream
   * service.
   *
   * @param maxConnections The maximum number of allowed opened HTTP connections.
   * @see ClientConfiguration#setMaxConnections(int)
   * @throws SQLException if the value is negative.
   */
  public void setMaxConnections(int maxConnections) throws SQLException {
    if (maxConnections < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_MAX_CONNECTIONS, maxConnections);
    }

    sdkProperties.setProperty(
      TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(),
      String.valueOf(maxConnections));
  }

  /**
   * Gets the maximum number of retry attempts for retryable errors with 5XX error codes in the
   * client.
   *
   * @see ClientConfiguration#getMaxErrorRetry()
   * @return the maximum number of retry attempts for retryable errors; -1 if value has not been
   * set.
   */
  public int getMaxRetryCount() {
    final String maxRetry = sdkProperties
      .getProperty(TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty());
    return (maxRetry == null) ? -1 : Integer.parseInt(maxRetry);
  }

  /**
   * Sets the maximum retry attempts for retryable errors on the client.
   *
   * @param maxRetryCountClient The maximum number of retry attempts for retryable errors.
   * @see ClientConfiguration#setMaxErrorRetry(int)
   * @throws SQLException if the value is negative.
   */
  public void setMaxRetryCount(int maxRetryCountClient) throws SQLException {
    if (maxRetryCountClient < 0) {
      throw Error.createSQLException(LOGGER, Error.INVALID_MAX_RETRY_COUNT, maxRetryCountClient);
    }

    sdkProperties.setProperty(
      TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(),
      String.valueOf(maxRetryCountClient));
  }

  @Override
  public void connectionClosed(ConnectionEvent event) {
    final TimestreamPooledConnection eventSource = (TimestreamPooledConnection) event.getSource();
    eventSource.removeConnectionEventListener(this);
    final TimestreamConnection connection = (TimestreamConnection) eventSource.getConnection();
    if (connection.isClosed()) {
      LOGGER.info("Connection is closed, not recycling connection back into the connection pool.");
    } else {
      LOGGER.info("Connection is still open, recycling the connection back into the connection pool.");
      availablePools.get(connection.getConnectionProperties()).add(connection);
    }
  }

  @Override
  public void connectionErrorOccurred(ConnectionEvent event) {
    // NOOP - If a connection error occurs, do not recycle this connection.
  }

  /**
   * Helper function that returns a Properties instance.
   *
   * @param accessKey The AWS access key ID.
   * @param secretKey The AWS secret access key.
   * @return Properties instance.
   * @throws SQLException if an empty endpoint is provided or no signing region is provided with the
   *                      endpoint.
   */
  private Properties getProperties(final String accessKey, final String secretKey)
    throws SQLException {
    final Properties properties = new Properties();
    if (accessKey != null && secretKey != null) {
      properties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), accessKey);
      properties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), secretKey);
    }

    if (this.getSessionToken() != null) {
      properties.put(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(), this.getSessionToken());
    }

    if (this.endpoint != null) {
      if (this.endpoint.isEmpty()) {
        final String error = Error.lookup(Error.INVALID_ENDPOINT);
        LOGGER.severe(error);
        throw new SQLException(error);
      }

      if (this.region == null) {
        final String error = Error.lookup(Error.MISSING_SERVICE_REGION);
        LOGGER.severe(error);
        throw new SQLException(error);
      }

      properties.put(TimestreamConnectionProperty.ENDPOINT.getConnectionProperty(), this.endpoint);
      properties.put(TimestreamConnectionProperty.REGION.getConnectionProperty(), this.region);
    }

    if (this.samlAuthenticationProperties
        .get(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty()) != null) {
      properties.putAll(samlAuthenticationProperties);
    }

    if (this.credentialsProviderClass != null) {
      properties.put(
        TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty(),
        this.credentialsProviderClass);

      if (this.credentialsFilePath != null) {
        properties.put(
          TimestreamConnectionProperty.CUSTOM_CREDENTIALS_FILE_PATH.getConnectionProperty(),
          this.credentialsFilePath);
      }
    }

    properties.putAll(sdkProperties);
    return properties;
  }

  /**
   * Creates a TimestreamPooledConnection object.
   *
   * @param timestreamConnection A TimestreamConnection that's wrapped within a
   *                             TimestreamPooledConnection.
   * @return A TimestreamPooledConnection instance.
   */
  private TimestreamPooledConnection createTimestreamPooledConnection(
    final TimestreamConnection timestreamConnection) {
    return new TimestreamPooledConnection(timestreamConnection);
  }

  /**
   * Creates a TimestreamConnection object.
   *
   * @param info Properties instance containing credential information.
   * @return TimestreamConnection instance.
   * @throws SQLException if connection cannot getClientInfo from the connection.
   */
  protected TimestreamConnection createTimestreamConnection(final Properties info)
    throws SQLException {
    return new TimestreamConnection(info, this.clientConfiguration);
  }
}
