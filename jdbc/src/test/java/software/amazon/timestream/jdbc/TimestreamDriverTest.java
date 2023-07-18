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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;

class TimestreamDriverTest {
  private MockTimestreamDriver driver;

  @BeforeEach
  void init() {
    driver = new MockTimestreamDriver();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    Constants.URL_PREFIX,
    Constants.URL_PREFIX + " ",
    " " + Constants.URL_PREFIX,
    Constants.URL_PREFIX + Constants.URL_BRIDGE,
    Constants.URL_PREFIX + Constants.URL_BRIDGE + " ",
    " " + Constants.URL_PREFIX + Constants.URL_BRIDGE,
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "foo"})
  void testAcceptsURLWithValidUrlPrefix(final String url) {
    Assertions.assertTrue(driver.acceptsURL(url));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {
      "jDbC:TimeSTREam",
      "jdbc:timestream ://",
      Constants.URL_PREFIX + " " + Constants.URL_BRIDGE,
      "foo", Constants.URL_PREFIX + "foo"})
  void testAcceptsURLWithInvalidUrl(final String url) {
    Assertions.assertFalse(driver.acceptsURL(url));
  }

  @Test
  void testConnectWithNullUrlNullProperties() throws SQLException {
    Assertions.assertNull(driver.connect(null, null));
  }

  @Test
  void testDriverApplicationName() throws SQLException {
    Assertions.assertEquals("java.exe", TimestreamDriver.APPLICATION_NAME);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      Constants.URL_PREFIX,
      Constants.URL_PREFIX + Constants.URL_BRIDGE,
      Constants.URL_PREFIX + " ",
      Constants.URL_PREFIX + Constants.URL_BRIDGE + " "})
  void testConnectWithValidUrlNullProperties(final String url) throws SQLException {
    final TimestreamConnection connection = driver.connect(url, null);
    final Properties constructedProperties = connection.getClientInfo();

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(new Properties(), constructedProperties);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      Constants.URL_PREFIX + Constants.URL_BRIDGE + "ApplicationName=Tableau",
      Constants.URL_PREFIX + Constants.URL_BRIDGE + "ApplicationName =Tableau",
      Constants.URL_PREFIX + Constants.URL_BRIDGE + " ApplicationName=Tableau",
      Constants.URL_PREFIX + Constants.URL_BRIDGE + " ApplicationName =Tableau; UnsupportedProperty =value"})
  void testConnectWithValidAndHasPropertiesInUrlNullProperties(final String url) throws SQLException {
    final TimestreamConnection connection = driver.connect(url, null);
    final Properties constructedProperties = connection.getClientInfo();

    final Properties expectedProperties = new Properties();
    expectedProperties.put("ApplicationName", "Tableau");

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(expectedProperties, constructedProperties);
  }

  @Test
  void testConnectWithNullUrlValidProperties() throws SQLException {
    final Properties prop = new Properties();
    prop.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    prop.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    Assertions.assertNull(driver.connect(null, prop));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      Constants.URL_PREFIX,
      Constants.URL_PREFIX + Constants.URL_BRIDGE})
  void testConnectWithValidUrlValidProperties(final String url) throws SQLException {
    final Properties inputProperties = new Properties();
    inputProperties.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    inputProperties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    final TimestreamConnection connection = driver.connect(url, inputProperties);
    final Properties constructedProperties = connection.getClientInfo();

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    expected.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(expected, constructedProperties);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      Constants.URL_PREFIX + Constants.URL_BRIDGE + "endpoint=endpoint.com",
      Constants.URL_PREFIX + Constants.URL_BRIDGE + "region=ca-central-1"})
  void testConnectWithInvalidCasingUrlEmptyProperties(final String url) throws SQLException {
    final TimestreamConnection connection = driver.connect(url, new Properties());
    final Properties constructedProperties = connection.getClientInfo();

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(new Properties(), constructedProperties);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      Constants.URL_PREFIX,
      Constants.URL_PREFIX + Constants.URL_BRIDGE})
  void testConnectWithValidUrlInvalidCasingProperties(final String url) throws SQLException {
    final Properties inputProperties = new Properties();
    inputProperties.put(TimestreamConnectionProperty.REGION.getConnectionProperty().toLowerCase(),
        "ca-central-1");
    inputProperties.put(TimestreamConnectionProperty.ENDPOINT.getConnectionProperty().toLowerCase(),
        "endpoint.com");

    final TimestreamConnection connection = driver.connect(url, inputProperties);
    final Properties constructedProperties = connection.getClientInfo();

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(new Properties(), constructedProperties);
  }

  @Test
  void testConnectWithDifferentProperties() throws SQLException {
    final String url =
        Constants.URL_PREFIX + Constants.URL_BRIDGE + TimestreamConnectionProperty.ACCESS_KEY_ID
            .getConnectionProperty() + "=foo";
    final Properties inputProperties = new Properties();
    inputProperties.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    final TimestreamConnection connection = driver.connect(url, inputProperties);
    final Properties constructedProperties = connection.getClientInfo();

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    expected.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    Assertions.assertNotNull(connection);
    Assertions.assertEquals(expected, constructedProperties);
  }

  @Test
  void testConnectWithDuplicateProperties() throws SQLException {
    final String url =
        Constants.URL_PREFIX + Constants.URL_BRIDGE + TimestreamConnectionProperty.ACCESS_KEY_ID
            .getConnectionProperty() + "=foo";
    final Properties info = new Properties();
    info.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "bar");
    final TimestreamConnection connection = driver.connect(url, info);

    Assertions.assertNotNull(connection);

    final Properties constructedProperties = connection.getClientInfo();

    final Properties expected = new Properties();
    expected.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");

    Assertions.assertEquals(expected, constructedProperties);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "=",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + " =",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "foo=",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "=bar",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + " =bar",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "foo==bar",
    Constants.URL_PREFIX + Constants.URL_BRIDGE + "=foo=bar="})
  void testGetPropertyInfoWithInvalidUrl(final String invalidUrl) {
    Assertions.assertThrows(
      SQLException.class,
      () -> driver.getPropertyInfo(invalidUrl, new Properties()));
  }

  @Test
  void testGetPropertyInfoWithValidUrl() throws SQLException {
    final String validUrl =
      Constants.URL_PREFIX + Constants.URL_BRIDGE +
        TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty() + "=bar";
    final DriverPropertyInfo[] expected = createExpectedDriverPropertyList(val -> {
      final String value;
      if (val == TimestreamConnectionProperty.ACCESS_KEY_ID) {
        value = "bar";
      } else {
        value = null;
      }
      return createDefaultDriverInfo(val, value);
    });

    validateDriverPropertyInfo(expected, driver.getPropertyInfo(validUrl, new Properties()));
  }

  @ParameterizedTest
  @EmptySource
  @ValueSource(strings = {
    Constants.URL_PREFIX,
    Constants.URL_PREFIX + Constants.URL_BRIDGE,
    "foo"})
  void testGetPropertyInfoWithProperty(final String url) throws SQLException {
    final Properties prop = new Properties();
    prop.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");
    prop.put(TimestreamConnectionProperty.SECRET_ACCESS_KEY.getConnectionProperty(), "bar");

    final DriverPropertyInfo[] expected = createExpectedDriverPropertyList(val -> {
      final String value;
      if (val == TimestreamConnectionProperty.ACCESS_KEY_ID) {
        value = "foo";
      } else if (val == TimestreamConnectionProperty.SECRET_ACCESS_KEY) {
        value = "bar";
      } else {
        value = null;
      }
      return createDefaultDriverInfo(val, value);
    });

    validateDriverPropertyInfo(expected, driver.getPropertyInfo(url, prop));
  }

  @Test
  void testGetPropertyInfoWithUrlAndProperty() throws SQLException {
    final String validUrl =
      Constants.URL_PREFIX + Constants.URL_BRIDGE + TimestreamConnectionProperty.ACCESS_KEY_ID
        .getConnectionProperty() + "=bar";
    final Properties prop = new Properties();
    prop.put(TimestreamConnectionProperty.ACCESS_KEY_ID.getConnectionProperty(), "foo");

    final DriverPropertyInfo[] expected = createExpectedDriverPropertyList(val -> {
      final String value;
      if (val == TimestreamConnectionProperty.ACCESS_KEY_ID) {
        value = "bar";
      } else {
        value = null;
      }
      return createDefaultDriverInfo(val, value);
    });

    validateDriverPropertyInfo(expected, driver.getPropertyInfo(validUrl, prop));
  }

  /**
   * Initializes a {@link DriverPropertyInfo} using the given {@link TimestreamConnectionProperty}
   * and the given value.
   *
   * @param property A supported connection property.
   * @param value    The property's value.
   * @return a {@link DriverPropertyInfo}.
   */
  private DriverPropertyInfo createDefaultDriverInfo(TimestreamConnectionProperty property,
    final String value) {
    final DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(
      property.getConnectionProperty(), value);
    driverPropertyInfo.description = property.getDescription();
    return driverPropertyInfo;
  }

  /**
   * Compare the given expected {@link DriverPropertyInfo} array with the actual {@link
   * DriverPropertyInfo} array retrieved by {@link TimestreamDriver#getPropertyInfo(String,
   * Properties)}.
   *
   * @param expected The expected {@link DriverPropertyInfo} array
   * @param actual   The actual {@link DriverPropertyInfo} array retrieved by {@link
   *                 TimestreamDriver#getPropertyInfo(String, Properties)}
   */
  private void validateDriverPropertyInfo(
    final DriverPropertyInfo[] expected,
    final DriverPropertyInfo[] actual) {
    for (int i = 0; i < expected.length; i++) {
      final DriverPropertyInfo expectedProperty = expected[i];
      final DriverPropertyInfo actualProperty = actual[i];
      Assertions.assertEquals(expectedProperty.name, actualProperty.name);
      Assertions.assertEquals(expectedProperty.value, actualProperty.value);
      Assertions.assertEquals(expectedProperty.description, actualProperty.description);
    }
  }

  /**
   * Creates a {@link DriverPropertyInfo} array using the populateFunction.
   *
   * @param populateFunction A lambda that creates a {@link DriverPropertyInfo} given a {@link
   *                         TimestreamConnectionProperty}
   * @return A {@link DriverPropertyInfo} array.
   */
  private DriverPropertyInfo[] createExpectedDriverPropertyList(
      Function<TimestreamConnectionProperty, DriverPropertyInfo> populateFunction) {
    return Arrays
        .stream(TimestreamConnectionProperty.values())
        .map(populateFunction)
        .toArray(DriverPropertyInfo[]::new);
  }
}
