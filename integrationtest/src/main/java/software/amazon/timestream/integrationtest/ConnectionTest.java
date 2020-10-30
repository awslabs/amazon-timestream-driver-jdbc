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

package software.amazon.timestream.integrationtest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.timestream.jdbc.TimestreamDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Integration tests for different connection methods.
 */
class ConnectionTest {
  private static final String EXPECTED_DEFAULT_CONNECTION_URL = "jdbc:timestream://Region=us-east-1";
  private static final String ENDPOINT = "query-cell0.timestream.us-east-1.amazonaws.com";
  private static final String REGION = "us-east-1";
  private static final String CONNECTION_FAILURE_SQL_STATE = "08006";

  @Test
  void testConnectionWithDefaultCredentialsChain() throws SQLException {
    try (Connection connection = DriverManager.getConnection(Constants.URL, new Properties())) {
      validateConnection(
        connection,
        EXPECTED_DEFAULT_CONNECTION_URL
      );
    }

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    try (Connection connection = dataSource.getConnection()) {
      validateConnection(
        connection,
        EXPECTED_DEFAULT_CONNECTION_URL
      );
    }
  }

  @Test
  void testConnectionWithRegion() throws SQLException {
    final Properties properties = new Properties();
    final String urlWithRegion = Constants.URL + "://Region=us-east-1";

    try (Connection connection = DriverManager.getConnection(urlWithRegion, new Properties())) {
      validateConnection(
        connection,
        EXPECTED_DEFAULT_CONNECTION_URL
      );
    }

    properties.put("region", REGION);
    try (Connection connection = DriverManager.getConnection(Constants.URL, properties)) {
      validateConnection(
        connection,
        EXPECTED_DEFAULT_CONNECTION_URL
      );
    }

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setRegion(REGION);
    try (Connection connection = dataSource.getConnection()) {
      validateConnection(
        connection,
        EXPECTED_DEFAULT_CONNECTION_URL
      );
    }
  }

  @Test
  void testConnectionWithSDKOptions() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("RequestTimeout", "1200");
    properties.setProperty("SocketTimeout", "3000");
    properties.setProperty("MaxRetryCount", "2");
    properties.setProperty("MaxConnections", "1");
    try (Connection connection = DriverManager.getConnection(Constants.URL, properties)) {
      Assertions.assertEquals("1200", connection.getClientInfo("RequestTimeout"));
      Assertions.assertEquals("3000", connection.getClientInfo("SocketTimeout"));
      Assertions.assertEquals("2", connection.getClientInfo("MaxRetryCount"));
      Assertions.assertEquals("1", connection.getClientInfo("MaxConnections"));
    }

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setRequestTimeout(1200);
    dataSource.setSocketTimeout(3000);
    dataSource.setMaxRetryCount(2);
    dataSource.setMaxConnections(1);
    try (Connection connection = dataSource.getConnection()) {
      Assertions.assertEquals("1200", connection.getClientInfo("RequestTimeout"));
      Assertions.assertEquals("3000", connection.getClientInfo("SocketTimeout"));
      Assertions.assertEquals("2", connection.getClientInfo("MaxRetryCount"));
      Assertions.assertEquals("1", connection.getClientInfo("MaxConnections"));
    }
  }

  @Test
  void testConnectionWithShortTimeoutValue() {
    final Properties shortRequestTimeout = new Properties();
    shortRequestTimeout.setProperty("RequestTimeout", "1");
    final SQLException shortRequestTimeoutException = Assertions.assertThrows(
      SQLException.class,
      () -> DriverManager.getConnection(Constants.URL, shortRequestTimeout));
    Assertions.assertEquals(CONNECTION_FAILURE_SQL_STATE, shortRequestTimeoutException.getSQLState());

    final Properties shortSocketTimeout = new Properties();
    shortSocketTimeout.setProperty("SocketTimeout", "1");
    final SQLException shortSocketTimeoutException = Assertions.assertThrows(
      SQLException.class,
      () -> DriverManager.getConnection(Constants.URL, shortSocketTimeout));
    Assertions.assertEquals(CONNECTION_FAILURE_SQL_STATE, shortSocketTimeoutException.getSQLState());
  }

  @Test
  void testConnectionWithValidEndpointConfiguration() throws SQLException {
    final String expectedUrl = "jdbc:timestream://Region=us-east-1;Endpoint=query-cell0.timestream.us-east-1.amazonaws.com";
    final Properties properties = new Properties();
    final String urlWithEndpoint = Constants.URL + "://Endpoint=query-cell0.timestream.us-east-1.amazonaws.com;Region=us-east-1";
    try (Connection connection = DriverManager.getConnection(urlWithEndpoint, properties)) {
      validateConnection(
        connection,
        expectedUrl
      );
    }

    properties.put("Endpoint", ENDPOINT);
    properties.put("Region", REGION);
    try (Connection connection = DriverManager.getConnection(Constants.URL, properties)) {
      validateConnection(
        connection,
        expectedUrl
      );
    }

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setEndpoint(ENDPOINT);
    dataSource.setRegion(REGION);
    try (Connection connection = dataSource.getConnection()) {
      validateConnection(
        connection,
        expectedUrl
      );
    }
  }

  @Test
  void testConnectionWithInvalidEndpoint() {
    final Properties properties = new Properties();
    final String urlWithInvalidEndpoint = Constants.URL + "://Endpoint=foo;Region=us-east-1";

    SQLException exception = Assertions.assertThrows(
      SQLException.class,
      () -> DriverManager.getConnection(urlWithInvalidEndpoint, properties));

    Assertions.assertEquals(CONNECTION_FAILURE_SQL_STATE, exception.getSQLState());

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setEndpoint(urlWithInvalidEndpoint);
    dataSource.setRegion("us-east-1");
    exception = Assertions.assertThrows(SQLException.class, dataSource::getConnection);
    Assertions.assertEquals(CONNECTION_FAILURE_SQL_STATE, exception.getSQLState());
  }

  @Test
  @DisplayName("Test creating a connection with an endpoint but no signing region.")
  void testConnectionWithInvalidEndpointConfiguration() {
    final Properties properties = new Properties();
    final String urlWithEndpoint = Constants.URL + "://Endpoint=query-cell0.timestream.us-east-1.amazonaws.com";
    Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(urlWithEndpoint, properties));

    properties.put("Endpoint", ENDPOINT);
    Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(Constants.URL, properties));

    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setEndpoint(ENDPOINT);
    Assertions.assertThrows(SQLException.class, dataSource::getConnection);
  }

  private void validateConnection(
    final Connection connection,
    final String expectedUrl) throws SQLException {
    Assertions.assertNotNull(connection);
    Assertions.assertEquals(
      expectedUrl,
      connection.getMetaData().getURL()
    );
  }
}
