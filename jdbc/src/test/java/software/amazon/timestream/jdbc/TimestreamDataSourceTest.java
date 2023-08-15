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

import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Properties;

class TimestreamDataSourceTest {

  /**
   * The default connection timeout set when instantiating a new {@link
   * com.amazonaws.ClientConfiguration}
   */
  private static final int DEFAULT_CONNECTION_TIMEOUT = 10;

  private static final String OKTA_IDP_NAME = "okta";
  private static final String OKTA_IDP_HOST = "oktaHost";
  private static final String USERNAME = "usr";
  private static final String PASSWORD = "pwd";
  private static final String APP_ID = "appId";
  private static final String ROLE_ARN = "role";
  private static final String IDP_ARN = "arn";
  private static final String AAD_IDP_NAME = "azureAD";
  private static final String AAD_CLIENT_SECRET = "secret";
  private static final String AAD_TENANT_ID = "tenant";

  private TimestreamDataSource timestreamDataSource;
  final Properties credentialSet1 = createCredentialSet("1");
  final Properties credentialSet2 = createCredentialSet("2");

  @Mock
  private AmazonTimestreamQuery mockQueryClient;

  @Mock
  private MockTimestreamConnection mockTimestreamConnection;

  @BeforeEach
  void init() {
    MockitoAnnotations.initMocks(this);
    timestreamDataSource = new TimestreamDataSource();
  }

  @Test
  void testGetLoginTimeout() {
    Assertions.assertEquals(DEFAULT_CONNECTION_TIMEOUT, timestreamDataSource.getLoginTimeout());
  }

  @Test
  void testSetLoginTimeoutWithValidTime() throws SQLException {
    timestreamDataSource.setLoginTimeout(10);
    Assertions.assertEquals(10, timestreamDataSource.getLoginTimeout());
    Assertions.assertEquals(10000, timestreamDataSource.clientConfiguration.getConnectionTimeout());

    timestreamDataSource.setLoginTimeout(0);
    Assertions.assertEquals(0, timestreamDataSource.getLoginTimeout());
    Assertions.assertEquals(0, timestreamDataSource.clientConfiguration.getConnectionTimeout());
  }

  @Test
  void testSetLoginTimeoutWithInvalidTime() {
    Assertions.assertThrows(SQLException.class, () -> timestreamDataSource.setLoginTimeout(-1));
  }

  @Test
  void testDataSourceWithEndpoint() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.ENDPOINT.getConnectionProperty(), "endpoint.com");
    expected.put(TimestreamConnectionProperty.REGION.getConnectionProperty(), "east");

    mockTimestreamDataSource.setEndpoint("endpoint.com");
    mockTimestreamDataSource.setRegion("east");

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(expected, constructedConnectionProperties);
  }

  @Test
  void testDataSourceWithEndpointDiscoveryAndRegion() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
        mockTimestreamConnection);

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.REGION.getConnectionProperty(), "east");

    mockTimestreamDataSource.setRegion("east");
    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(expected, constructedConnectionProperties);
  }

  @Test
  void testDataSourceWithEmptyEndpoint() {
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    mockTimestreamDataSource.setEndpoint("");
    mockTimestreamDataSource.setRegion("east");

    Assertions.assertThrows(SQLException.class, mockTimestreamDataSource::getConnection);
  }

  @Test
  void testDataSourceWithEndpointNoRegion() {
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);
    mockTimestreamDataSource.setEndpoint("endpoint");
    Assertions.assertThrows(SQLException.class, mockTimestreamDataSource::getConnection);
  }

  @Test
  @DisplayName("Test creating a connection with either no access key or no secret key")
  void testDataSourceWithIncompleteIAMCredentials() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);

    // Only access key ID specified.
    MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);
    mockTimestreamDataSource.setAccessKeyId("foo");

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNull(constructedConnectionProperties.getProperty(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty()));
    Assertions.assertNull(constructedConnectionProperties.getProperty(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty()));

    // Only secret access key specified.
    mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);
    mockTimestreamDataSource.setSecretAccessKey("bar");

    mockTimestreamDataSource.getConnection();
    constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNull(constructedConnectionProperties.getProperty(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty()));
    Assertions.assertNull(constructedConnectionProperties.getProperty(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty()));
  }

  @Test
  @DisplayName("Test constructing a DataSource using InstanceProfileCredentialsProvider.")
  void testDataSourceWithInstanceCredentialsProvider() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    mockTimestreamDataSource
      .setCredentialsProviderClass(Constants.INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME);

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(
      Constants.INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME,
      constructedConnectionProperties.getProperty(TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty()));
    Assertions.assertEquals(
      Constants.INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME,
      mockTimestreamDataSource.getCredentialsProviderClass());
  }

  @Test
  @DisplayName("Test constructing a DataSource using PropertiesFileCredentialsProvider.")
  void testDataSourceWithPropertiesFileCredentialsProvider() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    mockTimestreamDataSource
      .setCredentialsProviderClass(Constants.PROPERTIES_FILE_CREDENTIALS_PROVIDER_CLASSNAME);
    mockTimestreamDataSource.setCredentialsFilePath("file_path");

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(
      Constants.PROPERTIES_FILE_CREDENTIALS_PROVIDER_CLASSNAME,
      constructedConnectionProperties.getProperty(
        TimestreamConnectionProperty.AWS_CREDENTIALS_PROVIDER_CLASS.getConnectionProperty()));
    Assertions.assertEquals(
      "file_path",
      constructedConnectionProperties
        .getProperty(TimestreamConnectionProperty.CUSTOM_CREDENTIALS_FILE_PATH.getConnectionProperty()));
  }

  @Test
  void testDataSourceWithOkta() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(), OKTA_IDP_NAME);
    expected.put(TimestreamConnectionProperty.IDP_HOST.getConnectionProperty(), OKTA_IDP_HOST);
    expected.put(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(), USERNAME);
    expected.put(TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(), PASSWORD);
    expected.put(TimestreamConnectionProperty.OKTA_APP_ID.getConnectionProperty(), APP_ID);
    expected.put(TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(), ROLE_ARN);
    expected.put(TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(), IDP_ARN);

    mockTimestreamDataSource.setIdpName(OKTA_IDP_NAME);
    mockTimestreamDataSource.setIdpHost(OKTA_IDP_HOST);
    mockTimestreamDataSource.setIdpUserName(USERNAME);
    mockTimestreamDataSource.setIdpPassword(PASSWORD);
    mockTimestreamDataSource.setOktaAppId(APP_ID);
    mockTimestreamDataSource.setAwsRoleArn(ROLE_ARN);
    mockTimestreamDataSource.setIdpArn(IDP_ARN);

    Assertions.assertEquals(OKTA_IDP_NAME, mockTimestreamDataSource.getIdpName());
    Assertions.assertEquals(OKTA_IDP_HOST, mockTimestreamDataSource.getIdpHost());
    Assertions.assertEquals(USERNAME, mockTimestreamDataSource.getIdpUserName());
    Assertions.assertEquals(PASSWORD, mockTimestreamDataSource.getIdpPassword());
    Assertions.assertEquals(APP_ID, mockTimestreamDataSource.getOktaAppId());

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(expected, constructedConnectionProperties);
  }

  @Test
  void testDataSourceWithAAD() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.IDP_NAME.getConnectionProperty(), AAD_IDP_NAME);
    expected.put(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty(), USERNAME);
    expected.put(TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty(), PASSWORD);
    expected.put(TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty(), ROLE_ARN);
    expected.put(TimestreamConnectionProperty.IDP_ARN.getConnectionProperty(), IDP_ARN);
    expected.put(TimestreamConnectionProperty.AAD_APP_ID.getConnectionProperty(), APP_ID);
    expected.put(TimestreamConnectionProperty.AAD_CLIENT_SECRET.getConnectionProperty(), AAD_CLIENT_SECRET);
    expected.put(TimestreamConnectionProperty.AAD_TENANT_ID.getConnectionProperty(), AAD_TENANT_ID);

    mockTimestreamDataSource.setIdpName(AAD_IDP_NAME);
    mockTimestreamDataSource.setIdpUserName(USERNAME);
    mockTimestreamDataSource.setIdpPassword(PASSWORD);
    mockTimestreamDataSource.setAwsRoleArn(ROLE_ARN);
    mockTimestreamDataSource.setIdpArn(IDP_ARN);
    mockTimestreamDataSource.setAadAppId(APP_ID);
    mockTimestreamDataSource.setAadClientSecret(AAD_CLIENT_SECRET);
    mockTimestreamDataSource.setAadTenantId(AAD_TENANT_ID);

    Assertions.assertEquals(AAD_IDP_NAME, mockTimestreamDataSource.getIdpName());
    Assertions.assertEquals(USERNAME, mockTimestreamDataSource.getIdpUserName());
    Assertions.assertEquals(PASSWORD, mockTimestreamDataSource.getIdpPassword());
    Assertions.assertEquals(ROLE_ARN, mockTimestreamDataSource.getAwsRoleArn());
    Assertions.assertEquals(IDP_ARN, mockTimestreamDataSource.getIdpArn());
    Assertions.assertEquals(APP_ID, mockTimestreamDataSource.getAadAppId());
    Assertions.assertEquals(AAD_CLIENT_SECRET, mockTimestreamDataSource.getAadClientSecret());
    Assertions.assertEquals(AAD_TENANT_ID, mockTimestreamDataSource.getAadTenantId());

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(expected, constructedConnectionProperties);
  }

  @Test
  void testDataSourceWithNoCredentials() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    Assertions.assertEquals(new Properties(), propertiesArgumentCaptor.getValue());
  }

  @Test
  void testUserAgentIdentifierSuffix() {
    final String actualSuffix = timestreamDataSource.clientConfiguration.getUserAgentSuffix();
    final String expectedSuffix =
      Constants.UA_ID_PREFIX + TimestreamDriver.DRIVER_VERSION + TimestreamDriver.APP_NAME_SUFFIX;
    Assertions.assertEquals(expectedSuffix, actualSuffix);
  }

  @Test
  void testPooledConnectionWithEmptyPool() throws SQLException {
    Mockito.when(mockTimestreamConnection.getConnectionProperties()).thenReturn(credentialSet1);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    // Use credentialSet1 with DataSource.
    setDataSourceCredentials(mockTimestreamDataSource, credentialSet1);

    // Create a connection with credentialSet1.
    Assertions.assertTrue(mockTimestreamDataSource.availablePools.isEmpty());
    final TimestreamPooledConnection timestreamPooledConnection1 = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();
    final TimestreamConnection timestreamConnection1 = (TimestreamConnection) timestreamPooledConnection1
      .getConnection();

    // Check if the pool is initialized.
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());
    Assertions.assertEquals(0, mockTimestreamDataSource.availablePools.get(credentialSet1).size());

    // Close the connection and check if the connection was recycled.
    timestreamPooledConnection1.close();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.get(credentialSet1).size());

    // Retrieve a connection with the same credential set and make sure its the same one that we have previously received.
    final TimestreamPooledConnection sameTimestreamPooledConnection = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();
    final TimestreamConnection sameTimestreamConnection = (TimestreamConnection) sameTimestreamPooledConnection
      .getConnection();
    Assertions.assertEquals(timestreamConnection1, sameTimestreamConnection);
  }

  @Test
  void testPooledConnectionWithMultipleCredentials() throws SQLException {
    Mockito.when(mockTimestreamConnection.getConnectionProperties()).thenReturn(credentialSet1).thenReturn(credentialSet2);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    // Create a connection with credentialSet1.
    setDataSourceCredentials(mockTimestreamDataSource, credentialSet1);
    Assertions.assertTrue(mockTimestreamDataSource.availablePools.isEmpty());
    final TimestreamPooledConnection timestreamPooledConnection1 = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();

    // Create a connection with credentialSet2.
    setDataSourceCredentials(mockTimestreamDataSource, credentialSet2);
    final TimestreamPooledConnection timestreamPooledConnection2 = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();

    // Close both connections and verify that they are sent into two separate pools.
    timestreamPooledConnection1.close();
    timestreamPooledConnection2.close();
    Assertions.assertEquals(2, mockTimestreamDataSource.availablePools.size());
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.get(credentialSet1).size());
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.get(credentialSet2).size());
  }

  @Test
  void testPooledConnectionCloseTwice() throws SQLException {
    Mockito.when(mockTimestreamConnection.getConnectionProperties()).thenReturn(credentialSet1);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    // Create a connection with credentialSet1.
    setDataSourceCredentials(mockTimestreamDataSource, credentialSet1);
    Assertions.assertTrue(mockTimestreamDataSource.availablePools.isEmpty());
    final TimestreamPooledConnection timestreamPooledConnection = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());

    timestreamPooledConnection.close();
    timestreamPooledConnection.close();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.get(credentialSet1).size());
  }

  @Test
  void testClosedUnderlyingConnectionNotRecycled() throws SQLException {
    Mockito.when(mockTimestreamConnection.getConnectionProperties()).thenReturn(credentialSet1).thenReturn(credentialSet2);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    // Create a connection with credentialSet1.
    setDataSourceCredentials(mockTimestreamDataSource, credentialSet1);
    Assertions.assertTrue(mockTimestreamDataSource.availablePools.isEmpty());
    final TimestreamPooledConnection timestreamPooledConnection = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());

    final TimestreamConnection timestreamConnection = (TimestreamConnection) timestreamPooledConnection
      .getConnection();

    // Close the underlying connection first.
    timestreamConnection.queryClient = mockQueryClient;
    Mockito.doNothing().when(mockQueryClient).shutdown();
    timestreamConnection.close();
    timestreamConnection.close();
    // Then close the pool connection.
    Mockito.when(mockTimestreamConnection.isClosed()).thenReturn(true);
    timestreamPooledConnection.close();
    // Make sure that it is not recycled.
    Assertions.assertEquals(0, mockTimestreamDataSource.availablePools.get(credentialSet1).size());
  }

  @Test
  void testGetPooledConnectionWithClosedConnection() throws SQLException {
    Mockito.when(mockTimestreamConnection.getConnectionProperties()).thenReturn(credentialSet1).thenReturn(credentialSet2);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    setDataSourceCredentials(mockTimestreamDataSource, credentialSet1);
    Assertions.assertTrue(mockTimestreamDataSource.availablePools.isEmpty());
    final TimestreamPooledConnection timestreamPooledConnection = (TimestreamPooledConnection) mockTimestreamDataSource
      .getPooledConnection();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());

    timestreamPooledConnection.close();
    Mockito.when(mockTimestreamConnection.isClosed()).thenReturn(true);
    mockTimestreamDataSource.getPooledConnection();
    Assertions.assertEquals(1, mockTimestreamDataSource.availablePools.size());
  }

  @Test
  void testSetMaxConnectionsWithInvalidValue() {
    Assertions.assertThrows(SQLException.class, () -> timestreamDataSource.setMaxConnections(-1));
  }

  @Test
  void testGetMaxRetryCount() throws SQLException {
    // If maxRetryCountClient has not been initialized, -1 should be returned.
    Assertions.assertEquals(-1, timestreamDataSource.getMaxRetryCount());

    timestreamDataSource.setMaxRetryCount(10);
    Assertions.assertEquals(10, timestreamDataSource.getMaxRetryCount());
  }

  @Test
  void testSetMaxRetryCountWithInvalidValue() {
    Assertions.assertThrows(SQLException.class, () -> timestreamDataSource.setMaxRetryCount(-1));
  }

  @Test
  void testSetSocketTimeoutWithInvalidValue() {
    Assertions.assertThrows(SQLException.class, () -> timestreamDataSource.setSocketTimeout(-1));
  }

  @Test
  void testDataSourceWithSDKConfigurations() throws SQLException {
    final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    final MockTimestreamDataSource mockTimestreamDataSource = new MockTimestreamDataSource(
      mockTimestreamConnection);

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.REQUEST_TIMEOUT.getConnectionProperty(), "20000");
    expected.put(TimestreamConnectionProperty.SOCKET_TIMEOUT.getConnectionProperty(), "20000");
    expected.put(TimestreamConnectionProperty.MAX_CONNECTIONS.getConnectionProperty(), "1");
    mockTimestreamDataSource.setSocketTimeout(20000);
    mockTimestreamDataSource.setRequestTimeout(20000);
    mockTimestreamDataSource.setMaxConnections(1);

    mockTimestreamDataSource.getConnection();
    Mockito.verify(mockTimestreamConnection).setClientInfo(propertiesArgumentCaptor.capture());

    final Properties constructedConnectionProperties = propertiesArgumentCaptor.getValue();
    Assertions.assertNotNull(constructedConnectionProperties);
    Assertions.assertEquals(expected, constructedConnectionProperties);
  }

  /**
   * Sets the connection properties with the given credential set on the {@link
   * TimestreamDataSource} object.
   *
   * @param timestreamDataSource The {@link TimestreamDataSource} object used for tests.
   * @param credentialSet            The IAM credentials to use for the {@link TimestreamDataSource}
   *                                 object.
   */
  private void setDataSourceCredentials(
    final MockTimestreamDataSource timestreamDataSource,
    final Properties credentialSet) {
    timestreamDataSource.setAccessKeyId(
      credentialSet.getProperty(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty()));
    timestreamDataSource.setSecretAccessKey(
      credentialSet.getProperty(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty()));
    timestreamDataSource.setSessionToken(
      credentialSet.getProperty(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty()));
    timestreamDataSource
      .setRegion(credentialSet.getProperty(TimestreamConnectionProperty.REGION.getConnectionProperty()));
  }

  /**
   * Creates a {@link Properties} instance that contains mock AWS Credentials. Only contains
   * accessKeyID, accessKey, sessionToken and region.
   *
   * @param suffix The suffix that is appended to the default values.
   * @return A {@link Properties} instance.
   */
  private Properties createCredentialSet(String suffix) {
    final Properties credentialSet = new Properties();
    credentialSet.setProperty(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(),
      "testAccessKeyId" + suffix);
    credentialSet.setProperty(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(),
      "testSecretAccessKey" + suffix);
    credentialSet.setProperty(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(),
      "testSecretAccessKey" + suffix);
    credentialSet.setProperty(TimestreamConnectionProperty.SESSION_TOKEN.getConnectionProperty(),
      "testSessionToken" + suffix);

    return credentialSet;
  }
}
