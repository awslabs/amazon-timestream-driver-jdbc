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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithSAMLRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

abstract class TimestreamSAMLCredentialsProvider {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TimestreamSAMLCredentialsProvider.class);
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final String userName;
  protected final String password;
  protected final String roleARN;
  protected final String idpARN;
  protected final CloseableHttpClient httpClient;

  TimestreamSAMLCredentialsProvider(CloseableHttpClient httpClient, final Map<String, String> fieldsMap) {
    this.userName = fieldsMap.get(TimestreamConnectionProperty.IDP_USERNAME.getConnectionProperty());
    this.password = fieldsMap.get(TimestreamConnectionProperty.IDP_PASSWORD.getConnectionProperty());
    this.roleARN = fieldsMap.get(TimestreamConnectionProperty.AWS_ROLE_ARN.getConnectionProperty());
    this.idpARN = fieldsMap.get(TimestreamConnectionProperty.IDP_ARN.getConnectionProperty());
    this.httpClient = httpClient;
  }

  /**
   * Creates an {@link AWSStaticCredentialsProvider} instance using the credentials fetched through Okta
   * SAML authentication.
   *
   * @return An {@link AWSStaticCredentialsProvider} instance.
   * @throws SQLException If unable to parse the response body while fetching SAML Assertion.
   */
  AWSCredentialsProvider createCredentialsProvider() throws SQLException {
    final Credentials credentials = createSAMLRequestAndFetchCredentials();

    final String accessKeyID = credentials.getAccessKeyId();
    final String secretAccessKey = credentials.getSecretAccessKey();
    final String sessionToken = credentials.getSessionToken();

    return new AWSStaticCredentialsProvider(
      new BasicSessionCredentials(accessKeyID, secretAccessKey, sessionToken));
  }

  protected abstract String getSAMLAssertion() throws SQLException;

  /**
   * Fetches the AWS Credentials using the provided SAML Assertion.
   *
   * @param samlRequest An {@link AssumeRoleWithSAMLRequest} instance representing the Okta SAML
   *                    request.
   * @return A {@link Credentials} instance that contains accessKeyID, secretAccessKey and
   * sessionToken.
   */
  protected Credentials fetchCredentialsWithSAMLAssertion(final AssumeRoleWithSAMLRequest samlRequest) {
    LOGGER.debug("Fetching the AWS credentials with the SAML assertion.");
    return AWSSecurityTokenServiceClientBuilder
      .defaultClient()
      .assumeRoleWithSAML(samlRequest)
      .getCredentials();
  }

  /**
   * Fetches the AWS Credentials using the SAML Assertion.
   *
   * @return A {@link Credentials} instance that contains accessKeyID, secretAccessKey and
   * sessionToken.
   * @throws SQLException If unable to parse the response body while fetching SAML Assertion.
   */
  private Credentials createSAMLRequestAndFetchCredentials() throws SQLException {
    LOGGER.debug("Constructing an AssumeRoleWithSAMLRequest.");
    final AssumeRoleWithSAMLRequest samlRequest = new AssumeRoleWithSAMLRequest()
      .withRoleArn(this.roleARN)
      .withSAMLAssertion(getSAMLAssertion())
      .withPrincipalArn(this.idpARN);

    return fetchCredentialsWithSAMLAssertion(samlRequest);
  }
}
