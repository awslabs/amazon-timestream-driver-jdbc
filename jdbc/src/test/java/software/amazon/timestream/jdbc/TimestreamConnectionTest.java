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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamquery.model.AmazonTimestreamQueryException;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

class TimestreamConnectionTest {

  private static final int DEFAULT_REQUEST_TIMEOUT = 0;
  private static final String[] DEFAULT_OKTA_PROPERTIES_ARRAY = new String[]{"Okta",
    "testUserName", "testPassword", "testRoleARN", "testIdpARN", "testAppID",
    "testIdpHost"};
  private static final String[] DEFAULT_AAD_PROPERTIES_ARRAY = new String[]{"AzureAD",
    "testUserName", "testPassword", "testRoleARN", "testIdpARN", "testAppID",
    "testClientSecret", "testTenantID"};

  @Mock
  private AmazonTimestreamQuery mockQueryClient;

  @Mock
  private AmazonTimestreamQueryClientBuilder mockQueryClientBuilder;

  @Mock
  private CloseableHttpClient mockHttpClient;

  @Mock
  private CloseableHttpResponse mockOktaSessionTokenResponse;

  @Mock
  private CloseableHttpResponse mockOktaSAMLAssertionResponse;

  @Mock
  private StatusLine mockSessionTokenRequestStatusLine;

  @Mock
  private StatusLine mockAccessTokenRequestStatusLine;

  @Mock
  private CloseableHttpResponse mockAzureADAccessTokenResponse;

  @Mock
  private StatusLine mockSAMLRequestStatusLine;

  private MockTimestreamConnection connection;

  @BeforeEach
  void init() throws SQLException {
    MockitoAnnotations.initMocks(this);

    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "key");
    properties
        .put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "secret");
    properties.put(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(), "token");

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);
  }

  @Test
  void testClose() {
    Mockito.doNothing().when(mockQueryClient).shutdown();
    Assertions.assertFalse(connection.isClosed());

    connection.close();

    Assertions.assertTrue(connection.isClosed());
  }

  @Test
  void testCloseTwice() {
    Mockito.doNothing().when(mockQueryClient).shutdown();
    Assertions.assertFalse(connection.isClosed());

    connection.close();
    connection.close();

    Assertions.assertTrue(connection.isClosed());
  }

  @Test
  void testCreateStatementOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.createStatement());
  }

  @Test
  void testCreateStatementWithUnsupportedAttributes() {
    final SQLException resultSetTypeException = Assertions.assertThrows(
        SQLException.class,
        () -> connection.createStatement(
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY));
    TimestreamTestUtils.validateException(
        resultSetTypeException,
        Error.lookup(Error.RESULT_FORWARD_ONLY));

    final SQLException concurrencyException = Assertions.assertThrows(
        SQLException.class,
        () -> connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE));
    TimestreamTestUtils.validateException(concurrencyException, Error.lookup(Error.READ_ONLY));
  }

  @Test
  void testGetAutoCommitOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.getAutoCommit());
  }

  @Test
  void testGetCatalogOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.getCatalog());
  }

  @Test
  void testGetNetworkTimeout() throws SQLException {
    Mockito.when(mockQueryClientBuilder.getClientConfiguration())
        .thenReturn(new ClientConfiguration());

    Assertions.assertEquals(DEFAULT_REQUEST_TIMEOUT, connection.getNetworkTimeout());
  }

  @Test
  void testGetNetworkTimeoutOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.getNetworkTimeout());
  }

  @Test
  void testSetNetworkTimeout() throws SQLException {
    final int timeoutValue = 5000000;
    final ArgumentCaptor<ClientConfiguration> argument = ArgumentCaptor
        .forClass(ClientConfiguration.class);
    Mockito
        .when(mockQueryClientBuilder.getClientConfiguration())
        .thenReturn(new ClientConfiguration());
    Mockito
        .when(mockQueryClientBuilder.withClientConfiguration(argument.capture()))
        .thenReturn(mockQueryClientBuilder);
    Mockito.when(mockQueryClientBuilder.build()).thenReturn(mockQueryClient);

    connection.setNetworkTimeout(null, timeoutValue);
    Assertions.assertEquals(timeoutValue, connection.getNetworkTimeout());

    final TimestreamStatement statement = connection.createStatement();
    Assertions.assertEquals(connection, statement.getConnection());
    Assertions.assertEquals(timeoutValue, argument.getValue().getRequestTimeout());
  }

  @Test
  void testSetNetworkTimeoutOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.getNetworkTimeout());
  }

  @Test
  void testSetNetworkTimeoutWithNegativeTime() {
    Assertions.assertThrows(SQLException.class, () -> connection.setNetworkTimeout(null, -1));
  }

  /**
   * Test calling a method on a closed {@link TimestreamConnection}.
   *
   * @param method The method executable.
   */
  private void testMethodOnClosedConnection(final Executable method) {
    connection.close();

    final Exception exception = Assertions.assertThrows(SQLException.class, method);
    TimestreamTestUtils.validateException(
        exception,
        Error.lookup(Error.CONN_CLOSED));
  }

  @Test
  void testUserAgentIdentifierSuffix() {
    final String actualSuffix = connection.clientConfiguration.getUserAgentSuffix();
    final String expectedSuffix =
        Constants.UA_ID_PREFIX + TimestreamDriver.DRIVER_VERSION + TimestreamDriver.APP_NAME_SUFFIX;
    Assertions.assertEquals(expectedSuffix, actualSuffix);
  }

  @Test
  void testSetClientInfoOnCloseConnection() {
    testMethodOnClosedConnection(() -> connection.setClientInfo(new Properties()));
  }

  @Test
  void testSetAndGetClientInfo() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(), "new_user");
    connection.setClientInfo(properties);
    Assertions.assertEquals(properties, connection.getClientInfo());
    Assertions.assertNull(connection.getClientInfo(null));
  }

  @Test
  void testSetUnsupportedClientInfo() throws SQLException {
    final Properties properties = new Properties();
    properties.put("user_name", "new_user");
    connection.setClientInfo(properties);
    TimestreamTestUtils.validateWarning(connection.getWarnings(),
        Warning.lookup(Warning.UNSUPPORTED_PROPERTY, "user_name"));
  }

  @Test
  void testSetClientInfoWithNullProperties() throws SQLException {
    Assertions.assertNull(connection.getWarnings());
    connection.setClientInfo(null);
    Assertions.assertNotNull(connection.getWarnings());
    Assertions.assertEquals(
        Warning.lookup(Warning.NULL_PROPERTY),
        connection.getWarnings().getMessage());
  }

  @Test
  void testSetClientInfoWithNullPropertiesTwice() throws SQLException {
    Assertions.assertNull(connection.getWarnings());
    connection.setClientInfo(null);
    connection.setClientInfo(null);

    SQLWarning warnings = connection.getWarnings();
    for (int i = 0; i < 2; i++) {
      Assertions.assertEquals(
          Warning.lookup(Warning.NULL_PROPERTY),
          warnings.getMessage());

      warnings = warnings.getNextWarning();
    }
  }

  @Test
  void testSetClientInfoWithNameAndValueOnClosedConnection() {
    testMethodOnClosedConnection(() -> connection.setClientInfo("foo", "bar"));
  }

  @Test
  void testSetClientInfoWithNameAndValue() throws SQLException {
    connection.setClientInfo(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    Assertions.assertNull(connection.getWarnings());

    Assertions.assertEquals(
        "foo",
        connection.getClientInfo().getProperty(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty()));
  }

  @Test
  void testSetTypeMapOnCloseConnection() {
    testMethodOnClosedConnection(() -> connection.setTypeMap(new HashMap<>()));
  }

  @Test
  void testSetAndGetTypeMap() throws SQLException {
    final Map<String, Class<?>> map = new ImmutableMap.Builder<String, Class<?>>()
        .put(TimestreamDataType.INTEGER.name(), Double.class)
        .build();
    connection.setTypeMap(map);
    Assertions.assertEquals(map, connection.getTypeMap());
    connection.setTypeMap(null);
    Assertions.assertTrue(connection.getTypeMap().isEmpty());
  }

  @Test
  void testIsValidWithInvalidTimeout() {
    Assertions.assertThrows(SQLException.class, () -> connection.isValid(-1));
  }

  @Test
  void testIsValid() throws SQLException {
    final ArgumentCaptor<ClientConfiguration> clientConfig = ArgumentCaptor
        .forClass(ClientConfiguration.class);

    final ClientConfiguration clientConfiguration = new ClientConfiguration();
    Mockito.when(mockQueryClientBuilder.getClientConfiguration()).thenReturn(clientConfiguration);

    Mockito.when(mockQueryClient.query(Mockito.any())).thenReturn(null);
    Mockito
        .when(mockQueryClientBuilder.withClientConfiguration(clientConfig.capture()))
        .thenReturn(mockQueryClientBuilder);
    Mockito.when(mockQueryClientBuilder.build()).thenReturn(mockQueryClient);

    Assertions.assertTrue(connection.isValid(2));
    Assertions.assertEquals(2000, clientConfig.getValue().getConnectionTimeout());
  }

  @Test
  void testIsValidWithException() throws SQLException {
    final ArgumentCaptor<ClientConfiguration> clientConfig = ArgumentCaptor
        .forClass(ClientConfiguration.class);

    final ClientConfiguration clientConfiguration = new ClientConfiguration();
    Mockito.when(mockQueryClientBuilder.getClientConfiguration()).thenReturn(clientConfiguration);
    Mockito.when(mockQueryClient.query(Mockito.any()))
        .thenThrow(AmazonTimestreamQueryException.class);

    Mockito
        .when(mockQueryClientBuilder.withClientConfiguration(clientConfig.capture()))
        .thenReturn(mockQueryClientBuilder);
    Mockito.when(mockQueryClientBuilder.build()).thenReturn(mockQueryClient);

    Assertions.assertFalse(connection.isValid(2));
    Assertions.assertEquals(2000, clientConfig.getValue().getConnectionTimeout());
  }

  @Test
  void testPropertiesFileCredentialsProviderIsSet() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty(),
        Constants.PROPERTIES_FILE_CREDENTIALS_PROVIDER_CLASSNAME);
    properties.put(TimestreamConnectionProperty.CUSTOM_CREDENTIALS_FILE_PATH.getConnectionProperty(),
        "/custom/file/.path");

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Assertions.assertEquals(properties, connection.getClientInfo());
    Assertions.assertTrue(
        connection.getCredentialsProvider() instanceof PropertiesFileCredentialsProvider);
  }

  @ParameterizedTest
  @ValueSource(strings = {Constants.INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME,
      "insTanCeprOfilEcreDentiAlspRoviDeR", "InstanceProfileCredentialsProvider",
      "INSTANCEPROFILECREDENTIALSPROVIDER"})
  void testInstanceProfileCredentialsProviderIsSet(
      final String instanceProfileCredentialsProviderClassName) throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty(),
        instanceProfileCredentialsProviderClassName);

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Assertions.assertEquals(properties, connection.getClientInfo());
    Assertions
        .assertTrue(
            connection.getCredentialsProvider() instanceof InstanceProfileCredentialsProvider);
  }

  @Test
  void testUnsupportedCredentialsProviderThrows() {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty(),
        "UnsupportedCredentialsProvider");

    Assertions.assertThrows(SQLException.class, () -> new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @Test
  void testCredentialsClassWithEmptySessionToken() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "key");
    properties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "secret");

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Assertions.assertTrue(
        connection.getCredentialsProvider().getCredentials() instanceof BasicAWSCredentials);
  }

  @Test
  void testCredentialsClassWithEmptySecretAccessKey() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "key");

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Assertions.assertEquals(DefaultAWSCredentialsProviderChain.getInstance(),
        connection.getCredentialsProvider());
  }

  @Test
  void testCredentialsClassWithSessionToken() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "key");
    properties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "secret");
    properties.put(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(), "token");

    connection = new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Assertions.assertTrue(
        connection.getCredentialsProvider().getCredentials() instanceof BasicSessionCredentials);
  }

  @ParameterizedTest
  @MethodSource("oktaPropertiesWithEmptyFieldsProvider")
  void testEmptyOktaProperties(Properties oktaPropertiesWithEmptyFields) {
    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaPropertiesWithEmptyFields,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @ParameterizedTest
  @MethodSource("azureADPropertiesWithEmptyFieldsProvider")
  void testEmptyAzureADProperties(Properties azureADPropertiesWithEmptyFields) {
    Assertions.assertThrows(SQLException.class,
      () -> new MockTimestreamConnection(
        azureADPropertiesWithEmptyFields,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @ParameterizedTest
  @MethodSource("oktaPropertiesWithSpecialCharsProvider")
  void testOktaPropertiesWithSpecialChars(Properties oktaPropertiesWithSpecialChars)
      throws IOException, SQLException {
    setOktaValidSessionTokenResponse();
    setOktaValidSAMLAssertionResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    connection = new MockTimestreamConnection(
        oktaPropertiesWithSpecialChars,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Mockito.verify(mockHttpClient, Mockito.times(2)).execute(Mockito.any(HttpUriRequest.class));
    Assertions
        .assertTrue(connection.getCredentialsProvider() instanceof AWSStaticCredentialsProvider);
  }

  @Test
  void testAzureADFailedAccessTokenRequestThrows() throws IOException {
    setAzureADFailedAccessTokenResponseCode();
    Mockito
      .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
      .thenReturn(mockAzureADAccessTokenResponse);

    final Properties azureADProperties = createIdPProperties(DEFAULT_AAD_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.AAD_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
      () -> new MockTimestreamConnection(
        azureADProperties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @Test
  void testAzureADEmptyAccessTokenResponseThrows() throws IOException {
    setAzureADEmptyAccessTokenResponse();
    Mockito
      .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
      .thenReturn(mockAzureADAccessTokenResponse);

    final Properties azureADProperties = createIdPProperties(DEFAULT_AAD_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.AAD_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
      () -> new MockTimestreamConnection(
        azureADProperties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @Test
  void testOktaResponseWithoutSAMLAssertionThrows() throws IOException {
    setOktaValidSessionTokenResponse();
    setOktaEmptySAMLAssertionResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testAzureADResponseWithoutAccessTokenThrows() throws IOException {
    setAzureADEmptySessionTokenResponse();
    Mockito
      .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
      .thenReturn(mockAzureADAccessTokenResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_AAD_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.AAD_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
      () -> new MockTimestreamConnection(
        oktaProperties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @Test
  void testOktaResponseWithoutSessionTokenThrows() throws IOException {
    setOktaEmptySessionTokenResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testOktaFailedSAMLRequestThrows() throws IOException {
    setOktaValidSessionTokenResponse();
    setOktaFailedSAMLAssertionResponseCode();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testOktaFailedSessionTokenRequestThrows() throws IOException {
    setOktaFailedSessionTokenResponseCode();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testAzureADInvalidAccessTokenResponseThrows() throws IOException {
    setAzureADInvalidAccessTokenResponse();
    Mockito
      .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
      .thenReturn(mockAzureADAccessTokenResponse);

    final Properties azureADProperties = createIdPProperties(DEFAULT_AAD_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.AAD_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
      () -> new MockTimestreamConnection(
        azureADProperties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @Test
  void testAzureADCredentialsProviderInstance() throws IOException, SQLException {
    setAzureADValidSessionTokenResponse();
    Mockito
      .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
      .thenReturn(mockAzureADAccessTokenResponse);

    final Properties azureADProperties = createIdPProperties(DEFAULT_AAD_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.AAD_PROPERTY_SET);

    connection = new MockTimestreamConnection(
      azureADProperties,
      mockQueryClient,
      mockQueryClientBuilder,
      mockHttpClient);

    Assertions
      .assertTrue(connection.getCredentialsProvider() instanceof AWSStaticCredentialsProvider);
  }

  @Test
  void testOktaCredentialsProviderInstance() throws SQLException, IOException {
    setOktaValidSessionTokenResponse();
    setOktaValidSAMLAssertionResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    connection = new MockTimestreamConnection(
        oktaProperties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient);

    Mockito.verify(mockHttpClient, Mockito.times(2)).execute(Mockito.any());
    Assertions
        .assertTrue(connection.getCredentialsProvider() instanceof AWSStaticCredentialsProvider);
  }

  @Test
  void testUnsupportedIdpNameThrows() {
    final String[] unsupportedOktaPropertiesArray = new String[]{
      "UnsupportedIdpName", "testUserName",
      "testPassword", "testRoleARN", "testIdpARN",
      "appID", "testIdpHost"};
    final Properties oktaProperties = createIdPProperties(unsupportedOktaPropertiesArray,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testOktaNonUTF8EncodedSessionTokenResponseThrows() throws IOException {
    setOktaNonUTF8EncodedSessionTokenResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testOktaInvalidSAMLAssertionResponseThrows() throws IOException {
    setOktaValidSessionTokenResponse();
    setOktaInvalidSAMLAssertionResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse, mockOktaSAMLAssertionResponse);

    final Properties oktaProperties = createIdPProperties(DEFAULT_OKTA_PROPERTIES_ARRAY,
      TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @Test
  void testOktaInvalidJSONResponseThrows() throws IOException {
    setOktaInvalidJSONTokenResponse();
    Mockito
        .when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class)))
        .thenReturn(mockOktaSessionTokenResponse);

    final Properties oktaProperties = createIdPProperties(
      DEFAULT_OKTA_PROPERTIES_ARRAY, TimestreamConnectionProperty.OKTA_PROPERTY_SET);

    Assertions.assertThrows(SQLException.class,
        () -> new MockTimestreamConnection(
            oktaProperties,
            mockQueryClient,
            mockQueryClientBuilder,
            mockHttpClient));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#invalidSdkOptions")
  void testConnectionWithInvalidSDKOptions(final Properties properties) {
    Assertions.assertThrows(SQLException.class, () -> new MockTimestreamConnection(
        properties,
        mockQueryClient,
        mockQueryClientBuilder,
        mockHttpClient));
  }

  @ParameterizedTest
  @MethodSource("software.amazon.timestream.jdbc.TimestreamTestUtils#nonNumericSdkOptions")
  @DisplayName("Test creating a connection with invalid, non-numeric input for SDK configuration options")
  void testConnectionWithNonNumericSDKOptions(final Properties properties) {
    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> new MockTimestreamConnection(
      properties,
      mockQueryClient,
      mockQueryClientBuilder,
      mockHttpClient));

    Assertions.assertEquals(Constants.CONNECTION_EXCEPTION_SQL_STATE, exception.getSQLState());
  }

  @Test
  void testConnectionWithSDKOptions() throws SQLException {
    final Properties properties = new Properties();
    properties.put(TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(), "10");
    properties.put(TimestreamConnectionProperty.REQUEST_TIMEOUT.getConnectionProperty(), "12");
    properties.put(TimestreamConnectionProperty.MAX_RETRY_COUNT.getConnectionProperty(), "1000");
    properties.put(TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(), "200");

    final MockTimestreamConnection timestreamConnection = new MockTimestreamConnection(
      properties,
      mockQueryClient,
      mockQueryClientBuilder,
      mockHttpClient);

    Assertions.assertEquals(10, timestreamConnection.clientConfiguration.getSocketTimeout());
    Assertions.assertEquals(12, timestreamConnection.clientConfiguration.getRequestTimeout());
    Assertions.assertEquals(1000, timestreamConnection.clientConfiguration.getMaxErrorRetry());
    Assertions.assertEquals(200, timestreamConnection.clientConfiguration.getMaxConnections());
  }

  /**
   * Create a {@link Properties} instance using the array that contains ordered Idp property
   * values.
   *
   * @param propertyValues An Array of Idp specific property values.
   * @param idpPropertyFields an {@link EnumSet} that contains the Idp specific field names.
   * @return Generated {@link Properties} instance.
   */
  private static Properties createIdPProperties(final String[] propertyValues,
    final EnumSet<TimestreamConnectionProperty> idpPropertyFields) {
    final Properties idpProperties = new Properties();
    int i = 0;
    for (final TimestreamConnectionProperty idpPropertyField : idpPropertyFields) {
      idpProperties.put(idpPropertyField.getConnectionProperty(), propertyValues[i++]);
    }
    return idpProperties;
  }

  /**
   * SAML assertion request returns an invalid HTML.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setOktaInvalidSAMLAssertionResponse() throws UnsupportedEncodingException {
    setOktaSucceededSAMLAssertionResponseCode();
    Mockito.when(mockOktaSAMLAssertionResponse.getEntity())
        .thenReturn(new StringEntity("<<<html lang=></htm>"));
  }

  /**
   * Session token request returns a non UTF-8 Encoded value.
   *
   * @throws IOException if the StringEntity could not be initialized.
   */
  private void setOktaNonUTF8EncodedSessionTokenResponse() throws IOException {
    setOktaSucceededSessionTokenResponseCode();

    final byte[] nonUTF8Value = "\u00de\u00ad\u00be\u00ef".getBytes(StandardCharsets.UTF_8);
    Mockito.when(mockOktaSessionTokenResponse.getEntity())
        .thenReturn(new StringEntity(new String(nonUTF8Value, StandardCharsets.UTF_8)));
  }

  /**
   * Session token request returns an invalid JSON.
   *
   * @throws IOException if the StringEntity could not be initialized.
   */
  private void setOktaInvalidJSONTokenResponse() throws IOException {
    setOktaSucceededSessionTokenResponseCode();
    Mockito.when(mockOktaSessionTokenResponse.getEntity()).thenReturn(new StringEntity("}{"));
  }

  /**
   * Access token response returns an invalid JSON.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setAzureADInvalidAccessTokenResponse() throws UnsupportedEncodingException {
    setAzureADSucceededAccessTokenResponseCode();
    Mockito.when(mockAzureADAccessTokenResponse.getEntity()).thenReturn(new StringEntity("}{"));
  }

  /**
   * Session token request returns an empty JSON without the required token field.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setOktaEmptySessionTokenResponse() throws UnsupportedEncodingException {
    setOktaSucceededSessionTokenResponseCode();
    Mockito.when(mockOktaSessionTokenResponse.getEntity()).thenReturn(new StringEntity("{}"));
  }

  /**
   * Access token request returns an empty JSON without the required token field.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setAzureADEmptySessionTokenResponse() throws UnsupportedEncodingException {
    setAzureADSucceededAccessTokenResponseCode();
    Mockito.when(mockAzureADAccessTokenResponse.getEntity()).thenReturn(new StringEntity("{}"));
  }

  /**
   * SAML assertion request returns an empty HTML without the required assertion field.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setOktaEmptySAMLAssertionResponse() throws UnsupportedEncodingException {
    setOktaSucceededSAMLAssertionResponseCode();
    Mockito.when(mockOktaSAMLAssertionResponse.getEntity()).thenReturn(new StringEntity(
        "<html><body></body></html>"));
  }

  /**
   * Access token request returns an empty HTML without the required access token field.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setAzureADEmptyAccessTokenResponse() throws UnsupportedEncodingException {
    setAzureADSucceededAccessTokenResponseCode();
    Mockito.when(mockAzureADAccessTokenResponse.getEntity()).thenReturn(new StringEntity(
      "{}"));
  }

  /**
   * Session token response returns a valid mock session token.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setOktaValidSessionTokenResponse() throws UnsupportedEncodingException {
    setOktaSucceededSessionTokenResponseCode();
    Mockito.when(mockOktaSessionTokenResponse.getEntity()).thenReturn(new StringEntity("{ "
        + "\"sessionToken\":"
        + " \"mockSessionToken\""
        + "}"));
  }

  /**
   * Access token response returns a valid mock session token.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setAzureADValidSessionTokenResponse() throws UnsupportedEncodingException {
    setAzureADSucceededAccessTokenResponseCode();
    Mockito.when(mockAzureADAccessTokenResponse.getEntity()).thenReturn(new StringEntity("{ "
      + "\"access_token\":"
      + " \"aVeryLongMockAccessTokenValue\""
      + "}"));
  }

  /**
   * SAML assertion response returns a valid mock SAML assertion.
   *
   * @throws UnsupportedEncodingException if the StringEntity could not be initialized.
   */
  private void setOktaValidSAMLAssertionResponse() throws UnsupportedEncodingException {
    setOktaSucceededSAMLAssertionResponseCode();
    Mockito.when(mockOktaSAMLAssertionResponse.getEntity()).thenReturn(new StringEntity(
        "<html><body><input name=\"SAMLResponse\" value=\"aVeryLongMockSAMLAssertion\"/></body></html>"));
  }

  /**
   * Session token request succeeds with 200 OK.
   */
  private void setOktaSucceededSessionTokenResponseCode() {
    Mockito.when(mockSessionTokenRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockOktaSessionTokenResponse.getStatusLine()).thenReturn(mockSessionTokenRequestStatusLine);
  }

  /**
   * SAML assertion request succeeds with 200 OK.
   */
  private void setOktaSucceededSAMLAssertionResponseCode() {
    Mockito.when(mockSAMLRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockOktaSAMLAssertionResponse.getStatusLine()).thenReturn(mockSAMLRequestStatusLine);
  }

  /**
   * Access token request succeeds with 200 OK.
   */
  private void setAzureADSucceededAccessTokenResponseCode() {
    Mockito.when(mockAccessTokenRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockAzureADAccessTokenResponse.getStatusLine()).thenReturn(mockAccessTokenRequestStatusLine);
  }

  /**
   * Access token request fails with 404.
   */
  private void setAzureADFailedAccessTokenResponseCode() {
    Mockito.when(mockAccessTokenRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    Mockito.when(mockAzureADAccessTokenResponse.getStatusLine()).thenReturn(mockAccessTokenRequestStatusLine);
  }

  /**
   * Session token request fails with 404.
   */
  private void setOktaFailedSessionTokenResponseCode() {
    Mockito.when(mockSessionTokenRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    Mockito.when(mockOktaSessionTokenResponse.getStatusLine()).thenReturn(mockSessionTokenRequestStatusLine);
  }

  /**
   * SAML assertion request fails with 404.
   */
  private void setOktaFailedSAMLAssertionResponseCode() {
    Mockito.when(mockSAMLRequestStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    Mockito.when(mockOktaSAMLAssertionResponse.getStatusLine()).thenReturn(mockSAMLRequestStatusLine);
  }

  /**
   * Returns a stream of Okta {@link Properties} instances, each missing an individual field to be used as an
   * input for the parameterized testEmptyOktaProperties test.
   *
   * @return A {@link Stream} of Okta {@link Properties}, each one missing an individual field.
   */
  private static Stream<Properties> oktaPropertiesWithEmptyFieldsProvider() {
    final ArrayList<Properties> properties = new ArrayList<>();

    for (int i = 1; i < DEFAULT_OKTA_PROPERTIES_ARRAY.length; i++) {
      final String[] tempPropertiesArray = Arrays
          .copyOf(DEFAULT_OKTA_PROPERTIES_ARRAY, DEFAULT_OKTA_PROPERTIES_ARRAY.length);
      tempPropertiesArray[i] = "";
      properties.add(
        createIdPProperties(tempPropertiesArray, TimestreamConnectionProperty.OKTA_PROPERTY_SET));
    }

    return properties.stream();
  }

  /**
   * Returns a stream of Azure AD {@link Properties} instances, each missing an individual field to be used as an
   * input for the parameterized testEmptyAzureADProperties test.
   *
   * @return A {@link Stream} of Azure AD {@link Properties}, each one missing an individual field.
   */
  private static Stream<Properties> azureADPropertiesWithEmptyFieldsProvider() {
    final ArrayList<Properties> properties = new ArrayList<>();

    for (int i = 1; i < DEFAULT_AAD_PROPERTIES_ARRAY.length; i++) {
      final String[] tempPropertiesArray = Arrays
        .copyOf(DEFAULT_AAD_PROPERTIES_ARRAY, DEFAULT_AAD_PROPERTIES_ARRAY.length);
      tempPropertiesArray[i] = "";
      properties.add(
        createIdPProperties(tempPropertiesArray, TimestreamConnectionProperty.AAD_PROPERTY_SET));
    }

    return properties.stream();
  }

  /**
   * Returns a stream of Okta {@link Properties} instances, with special characters in password and
   * user name fields.
   *
   * @return A {@link Stream} of Okta {@link Properties}, with special characters in password and
   * user name fields.
   */
  private static Stream<Properties> oktaPropertiesWithSpecialCharsProvider() {
    final String[] tempPropertiesArrayWithSpecialUserNameOnly = DEFAULT_OKTA_PROPERTIES_ARRAY
      .clone();
    tempPropertiesArrayWithSpecialUserNameOnly[2] = "d'Jy`&r4[|\"*[}=";
    final String[] tempPropertiesArrayWithSpecialPasswordOnly = DEFAULT_OKTA_PROPERTIES_ARRAY
      .clone();
    tempPropertiesArrayWithSpecialPasswordOnly[3] = "d'Jy`&r4[|\"*[}=";
    final String[] tempPropertiesArrayWithSpecialUserNameAndPassword = tempPropertiesArrayWithSpecialUserNameOnly
      .clone();
    tempPropertiesArrayWithSpecialPasswordOnly[3] = "d'Jy`&r4[|\"*[}=";
    return Stream.of(
      createIdPProperties(tempPropertiesArrayWithSpecialUserNameOnly,
        TimestreamConnectionProperty.OKTA_PROPERTY_SET),
      createIdPProperties(tempPropertiesArrayWithSpecialPasswordOnly,
        TimestreamConnectionProperty.OKTA_PROPERTY_SET),
      createIdPProperties(tempPropertiesArrayWithSpecialUserNameAndPassword,
        TimestreamConnectionProperty.OKTA_PROPERTY_SET)
    );
  }
}
