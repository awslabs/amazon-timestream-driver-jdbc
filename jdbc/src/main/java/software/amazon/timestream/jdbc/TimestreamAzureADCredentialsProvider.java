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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Class handling Azure AD SSO.
 */
class TimestreamAzureADCredentialsProvider extends TimestreamSAMLCredentialsProvider {
  private final String appID;
  private final String clientSecret;
  private final String tenantID;

  /**
   * Constructor for {@link TimestreamAzureADCredentialsProvider}.
   *
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @param azureADFieldsMap A {@link Map} including the following required parameters:
   *                         <ol>
   *                         <li>userName: The user name for the specified IdP account.
   *                         <li>password: The password for the specified IdP account.
   *                         <li>roleARN:  The Amazon Resource Name (ARN) of the role that the caller is assuming.
   *                         <li>idpARN:   The Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
   *                         <li>appID:    The unique id of the registered application on Azure AD.
   *                         <li>clientSecret: The client secret associated with the registered application on Azure AD used to authorize fetching tokens.
   *                         <li>tenantID: The Azure AD Tenant ID.
   *                         </li></ol>
   */
  TimestreamAzureADCredentialsProvider(CloseableHttpClient httpClient, final Map<String, String> azureADFieldsMap) {
    super(httpClient, azureADFieldsMap);
    this.appID = azureADFieldsMap.get(TimestreamConnectionProperty.AAD_APP_ID.getConnectionProperty());
    this.clientSecret = azureADFieldsMap.get(TimestreamConnectionProperty.AAD_CLIENT_SECRET.getConnectionProperty());
    this.tenantID = azureADFieldsMap.get(TimestreamConnectionProperty.AAD_TENANT_ID.getConnectionProperty());
  }

  /**
   * Constructs the SAML Assertion with the access token.
   *
   * @return The SAML assertion needed to get temporary credentials from AWS.
   * @throws SQLException if unable to parse the response body or the request fails.
   */
  @Override
  protected String getSAMLAssertion() throws SQLException {
    final String accessToken = getAccessToken();
    final String assertion = "<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
      + "<samlp:Status>"
      + "<samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>"
      + "</samlp:Status>"
      + new String(Base64.decodeBase64(accessToken), StandardCharsets.UTF_8)
      + "</samlp:Response>";
    return new String(Base64.encodeBase64(assertion.getBytes(StandardCharsets.UTF_8)),
        StandardCharsets.UTF_8);
  }

  /**
   * Fetches an access token from Azure AD Oauth2 endpoint using the provided properties.
   *
   * @return The fetched access token from Azure AD.
   * @throws SQLException if unable to parse the response body or the request fails.
   */
  private String getAccessToken() throws SQLException {
    final String accessTokenEndpoint = "https://login.microsoftonline.com/" + this.tenantID + "/oauth2/token";

    final List<BasicNameValuePair> requestParameters =
      ImmutableList.of(new BasicNameValuePair("grant_type", "password"),
        new BasicNameValuePair("requested_token_type", "urn:ietf:params:oauth:token-type:saml2"),
        new BasicNameValuePair("username", userName),
        new BasicNameValuePair("password", password),
        new BasicNameValuePair("client_secret", clientSecret),
        new BasicNameValuePair("client_id", appID),
        new BasicNameValuePair("resource", appID));

    final HttpUriRequest accessTokenRequest = RequestBuilder
      .post()
      .setUri(accessTokenEndpoint)
      .addHeader("Accept", "application/json")
      .addHeader("Content-Type", "application/x-www-form-urlencoded")
      .setEntity(new UrlEncodedFormEntity(requestParameters, StandardCharsets.UTF_8))
      .build();

    try (CloseableHttpResponse response = this.httpClient.execute(accessTokenRequest)) {
      final StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
        throw Error.createSQLException(LOGGER, Error.AAD_ACCESS_TOKEN_REQUEST_FAILED);
      }
      final HttpEntity responseEntity = response.getEntity();
      final String responseString = EntityUtils.toString(responseEntity, "UTF-8");
      final JsonNode jsonNode = OBJECT_MAPPER.readTree(responseString).get("access_token");
      if (jsonNode == null) {
        throw Error.createSQLException(LOGGER, Error.INVALID_AAD_ACCESS_TOKEN_RESPONSE);
      }
      return jsonNode.asText();
    } catch (IOException e) {
      throw Error.createSQLException(LOGGER, e, Error.AAD_ACCESS_TOKEN_ERROR);
    }
  }
}
