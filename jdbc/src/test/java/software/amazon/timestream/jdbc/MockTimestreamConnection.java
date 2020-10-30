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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Test subclass for TimestreamConnection that overrides the creation of {@link
 * AmazonTimestreamQuery} and {@link AmazonTimestreamQueryClientBuilder} to avoid creating a real
 * connection.
 */
class MockTimestreamConnection extends TimestreamConnection {
  private AWSCredentialsProvider credentialsProvider;
  private Properties properties;

  MockTimestreamConnection(
      final Properties info,
      final AmazonTimestreamQuery mockQueryClient,
      final AmazonTimestreamQueryClientBuilder mockQueryClientBuilder,
      final CloseableHttpClient mockHttpClient)
      throws SQLException {
    super(
      info,
      new ClientConfiguration().withUserAgentSuffix(TimestreamDriver.getUserAgentSuffix()),
      mockHttpClient);
    this.queryClient = mockQueryClient;
    this.queryClientBuilder = mockQueryClientBuilder;
  }

  @Override
  void buildQueryClientAndVerifyConnection(Properties info,
      AWSCredentialsProvider credentialsProvider) {
    this.properties = info;
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  AmazonTimestreamQueryClientBuilder getQueryClientBuilder() {
    return this.queryClientBuilder;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return this.getConnectionProperties();
  }

  @Override
  MockTimestreamOktaCredentialsProvider createOktaCredentialsProvider(final CloseableHttpClient httpClient,
      final Map<String, String> oktaFieldsMap) {
    return new MockTimestreamOktaCredentialsProvider(httpClient, oktaFieldsMap);
  }

  @Override
  MockTimestreamAzureADCredentialsProvider createAzureADCredentialsProvider(final CloseableHttpClient httpClient,
    final Map<String, String> azureADFieldsMap) {
    return new MockTimestreamAzureADCredentialsProvider(httpClient, azureADFieldsMap);
  }

  /**
   * Gets the credential provider used to construct the connection.
   *
   * @return the {@link AWSCredentialsProvider} used to construct the connection.
   */
  AWSCredentialsProvider getCredentialsProvider() {
    return this.credentialsProvider;
  }

  /**
   * Gets the properties used to construct the connection.
   *
   * @return the {@link Properties} object used to construct the connection.
   */
  Properties getProperties() {
    return properties;
  }
}
