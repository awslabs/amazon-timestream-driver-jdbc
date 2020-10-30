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

import java.util.Arrays;
import java.util.EnumSet;

/**
 * Enum containing all supported connection properties.
 */
enum TimestreamConnectionProperty {
  APPLICATION_NAME("ApplicationName", "",
      "The name of the application currently utilizing the connection."),
  IDP_NAME("IdpName", "",
      "The Identity Provider name to use for SAML based authentication. Should either be 'Okta' or 'AzureAD'."),
  IDP_HOST("IdpHost", "",
      "The hostname of the specified Idp."),
  IDP_USERNAME("IdpUserName", "",
      "The user name for the specified Idp account."),
  IDP_PASSWORD("IdpPassword", "",
      "The password for the specified Idp account."),
  OKTA_APP_ID("OktaApplicationID", "",
      "The unique Okta-provided ID associated with the Timestream application."),
  AWS_ROLE_ARN("RoleARN", "",
      "The Amazon Resource Name (ARN) of the role that the caller is assuming."),
  IDP_ARN("IdpARN", "",
      "The Amazon Resource Name (ARN) of the SAML provider in IAM that describes the Idp."),
  AAD_APP_ID("AADApplicationID", "",
    "The unique id of the registered application on Azure AD."),
  AAD_CLIENT_SECRET("AADClientSecret", "",
    "The client secret associated with the registered application on Azure AD used to authorize fetching tokens."),
  AAD_TENANT_ID("AADTenant", "",
    "The Azure AD Tenant ID."),
  AWS_CREDENTIALS_PROVIDER_CLASS("AwsCredentialsProviderClass", "",
      "The AWSCredentialsProvider class that user wants to use."),
  CUSTOM_CREDENTIALS_FILE_PATH("CustomCredentialsFilePath", "",
      "The AWS credentials file that user wants to use."),
  ACCESS_KEY_ID("AccessKeyId", "", "The AWS user access key id."),
  SECRET_ACCESS_KEY("SecretAccessKey", "", "The AWS user secret access key."),
  SESSION_TOKEN("SessionToken", "", "The database's region."),
  REGION(
    "Region",
    "us-east-1",
    "The temporary session token required to access a database with multi-factor authentication (MFA) enabled."),
  ENABLE_METADATA_PREPARED_STATEMENT(
      "EnableMetaDataPreparedStatement",
      Boolean.FALSE.toString(),
      "Enables the driver to return metadata for PreparedStatements, "
          + "but this will incur an additional cost with Timestream when retrieving the metadata."),
  ENDPOINT("Endpoint", "", "Timestream service endpoint containing the resources."),
  REQUEST_TIMEOUT(
    "RequestTimeout",
    String.valueOf(ClientConfiguration.DEFAULT_REQUEST_TIMEOUT),
    "The time in milliseconds the AWS SDK will wait for a query request before timing out."),
  SOCKET_TIMEOUT(
    "SocketTimeout",
    String.valueOf(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT),
    "The time in milliseconds the AWS SDK will wait for data to be transferred over an open connection before timing out."),
  MAX_RETRY_COUNT(
    "MaxRetryCount",
    "",
    "The maximum number of retry attempts for retryable errors with 5XX error codes in the SDK."),
  MAX_CONNECTIONS(
    "MaxConnections",
    String.valueOf(ClientConfiguration.DEFAULT_MAX_CONNECTIONS),
    "The maximum number of allowed concurrently opened HTTP connections to the Timestream service.");

  protected static final EnumSet<TimestreamConnectionProperty> OKTA_PROPERTY_SET = EnumSet
    .of(IDP_NAME, IDP_USERNAME, IDP_PASSWORD, AWS_ROLE_ARN, IDP_ARN, OKTA_APP_ID, IDP_HOST);

  protected static final EnumSet<TimestreamConnectionProperty> AAD_PROPERTY_SET = EnumSet
    .of(IDP_NAME, IDP_USERNAME, IDP_PASSWORD, AWS_ROLE_ARN, IDP_ARN, AAD_APP_ID, AAD_CLIENT_SECRET,
      AAD_TENANT_ID);

  static final EnumSet<TimestreamConnectionProperty> SENSITIVE_PROPERTIES = EnumSet
    .of(IDP_USERNAME, IDP_PASSWORD, AWS_ROLE_ARN, IDP_ARN, AAD_CLIENT_SECRET, AAD_TENANT_ID, ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);

  private final String connectionProperty;
  private final String defaultValue;
  private final String description;

  /**
   * Constructor.
   *
   * @param connectionProperty String representing the connection property.
   * @param defaultValue       String representing the default value of the property.
   * @param description        Description of the property.
   */
  TimestreamConnectionProperty(
    final String connectionProperty,
    final String defaultValue,
    final String description) {
    this.connectionProperty = connectionProperty;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  /**
   * Gets connection property.
   *
   * @return the connection property.
   */
  String getConnectionProperty() {
    return connectionProperty;
  }

  /**
   * Gets the default value of the connection property.
   *
   * @return the default value of the connection property.
   */
  String getDefaultValue() {
    return defaultValue;
  }

  /**
   * Gets description.
   *
   * @return the description.
   */
  String getDescription() {
    return description;
  }

  /**
   * Check if the property is supported by the driver.
   *
   * @param name The name of the property.
   * @return {@code true} if property is supported; {@code false} otherwise.
   */
  static boolean isSupportedProperty(final String name) {
    return Arrays
        .stream(TimestreamConnectionProperty.values())
        .anyMatch(value -> value.getConnectionProperty().equals(name));
  }
}
