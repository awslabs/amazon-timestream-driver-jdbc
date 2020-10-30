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
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

/**
 * Class handling Okta SSO.
 */
class TimestreamOktaCredentialsProvider extends TimestreamSAMLCredentialsProvider {
  private final String appID;
  private final String idpHost;

  /**
   * Constructor for {@link TimestreamOktaCredentialsProvider}.
   *
   * @param httpClient The HTTP Client to be used during SAML Authentication.
   * @param oktaFieldsMap A {@link Map} including the following required parameters:
   *                   <ol>
   *                   <li>userName: The user name for the specified IdP account.
   *                   <li>password: The password for the specified IdP account.
   *                   <li>roleARN:  The Amazon Resource Name (ARN) of the role that the caller is assuming.
   *                   <li>idpARN:   The Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
   *                   <li>appID:    The app ID that is generated during Okta setup.
   *                   <li>idpHost:  The hostname of the specified IdP.
   *                   </li></ol>
   */
  TimestreamOktaCredentialsProvider(CloseableHttpClient httpClient, final Map<String, String> oktaFieldsMap) {
    super(httpClient, oktaFieldsMap);
    this.appID = oktaFieldsMap.get(TimestreamConnectionProperty.OKTA_APP_ID.getConnectionProperty());
    this.idpHost = oktaFieldsMap.get(TimestreamConnectionProperty.IDP_HOST.getConnectionProperty());
  }

  /**
   * Fetches the SAML Assertion from Okta.
   *
   * @return The extracted SAMLResponse field inside the HTML response body from Okta.
   * @throws SQLException When unable to parse the response body.
   */
  @Override
  protected String getSAMLAssertion() throws SQLException {
    final String sessionToken = getSessionToken();

    final String baseUri =
      "https://" + this.idpHost + "/app/" + Constants.OKTA_AWS_APP_NAME + "/" + this.appID
        + "/sso/saml";

    final HttpUriRequest samlGetRequest = RequestBuilder
      .get()
      .setUri(baseUri)
      .addParameter("onetimetoken", sessionToken)
      .build();

    try (CloseableHttpResponse response = this.httpClient.execute(samlGetRequest)) {
      final StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
        throw Error.createSQLException(LOGGER, Error.OKTA_SAML_ASSERTION_REQUEST_FAILED);
      }

      final HttpEntity responseEntity = response.getEntity();
      final String responseHTMLAsString = EntityUtils.toString(responseEntity, "UTF-8");

      final Document document = Jsoup.parse(responseHTMLAsString);
      final Optional<String> samlResponseValue = Optional
        .ofNullable(document.selectFirst("[name=SAMLResponse]"))
        .map(field -> field.attr("value"));
      if (!samlResponseValue.isPresent()) {
        throw Error.createSQLException(LOGGER, Error.INVALID_SAML_RESPONSE);
      }
      return samlResponseValue.get();
    } catch (final IOException e) {
      throw Error.createSQLException(LOGGER, e, Error.OKTA_SAML_ASSERTION_ERROR);
    }
  }

  /**
   * Fetches the sessionToken from Okta that will be used to fetch the SAML Assertion from AWS.
   *
   * @return Session token from Okta.
   * @throws SQLException When unable to parse the response body.
   */
  private String getSessionToken() throws SQLException {
    final String sessionTokenEndpoint = "https://" + this.idpHost + "/api/v1/authn";

    final StringEntity requestBodyEntity;
    try {
      requestBodyEntity = new StringEntity(
        "{\"username\":\"" + this.userName + "\",\"password\":\"" + this.password + "\"}", "UTF-8");

      final HttpUriRequest sessionTokenRequest = RequestBuilder
        .post()
        .setUri(sessionTokenEndpoint)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .setEntity(requestBodyEntity)
        .build();

      LOGGER.debug("Fetching Okta session token from: \n" + sessionTokenEndpoint);
      try (CloseableHttpResponse response = this.httpClient.execute(sessionTokenRequest)) {
        final StatusLine statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
          throw Error.createSQLException(LOGGER, Error.OKTA_SESSION_TOKEN_REQUEST_FAILED);
        }

        final HttpEntity responseEntity = response.getEntity();
        final String responseString = EntityUtils.toString(responseEntity, "UTF-8");
        final JsonNode jsonNode = OBJECT_MAPPER.readTree(responseString).get("sessionToken");
        if (jsonNode == null) {
          throw Error.createSQLException(LOGGER, Error.INVALID_SESSION_TOKEN_RESPONSE);
        }
        return jsonNode.asText();
      }
    } catch (final IOException e) {
      throw Error.createSQLException(LOGGER, e, Error.OKTA_SESSION_TOKEN_ERROR);
    }
  }
}
