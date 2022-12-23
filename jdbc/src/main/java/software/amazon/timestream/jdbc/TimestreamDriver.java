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
import com.google.common.base.Strings;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Timestream JDBC Driver class.
 */
public class TimestreamDriver implements java.sql.Driver {
    private static final Logger LOGGER = Logger.getLogger("software.amazon.timestream.jdbc.TimestreamDriver");

    static final int DRIVER_MAJOR_VERSION;
    static final int DRIVER_MINOR_VERSION;
    static final String DRIVER_VERSION;
    static final String APP_NAME_SUFFIX;
    static final String APPLICATION_NAME;

    static {
        // We want to use SLF4J as the logging framework, so install the bridge for it.
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        // Enable only >= FINE logs in the JUL Logger since we want to control things via SLF4J. JUL Logger will filter out
        // messages before it gets to SLF4J if it set to a restrictive level.
        LOGGER.setLevel(Level.FINE);

        APPLICATION_NAME = getApplicationName();
        APP_NAME_SUFFIX = " [" + APPLICATION_NAME + "]";
        LOGGER.finer("Name of the application using the driver: " + APP_NAME_SUFFIX);

        // Load the driver version from the associated pom file.
        int majorVersion = 0;
        int minorVersion = 0;
        String fullVersion = "";
        try (InputStream is = TimestreamDriver.class.getResourceAsStream("version.properties")) {
            final Properties p = new Properties();
            p.load(is);
            majorVersion = Integer.parseInt(p.getProperty("driver.major.version"));
            minorVersion = Integer.parseInt(p.getProperty("driver.minor.version"));
            fullVersion = p.getProperty("driver.version");
        } catch (Exception e) {
            LOGGER.severe("Error loading driver version: " + e.getMessage());
        }

        DRIVER_MAJOR_VERSION = majorVersion;
        DRIVER_MINOR_VERSION = minorVersion;
        DRIVER_VERSION = fullVersion;
        try {
            DriverManager.registerDriver(new TimestreamDriver());
        } catch (SQLException e) {
            LOGGER.severe("Error registering driver: " + e.getMessage());
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        if (null == url) {
            LOGGER.warning(Warning.lookup(Warning.NULL_URL));
            return false;
        }

        url = url.trim();
        if (!url.startsWith(Constants.URL_PREFIX)) {
            LOGGER.finer(Warning.lookup(Warning.UNSUPPORTED_URL_PREFIX, url));
            return false;
        }

        url = url.substring(Constants.URL_PREFIX.length());

        // Accept the URL if it's only the prefix, or has a bridge to potentially more properties.
        return url.isEmpty() || url.startsWith(Constants.URL_BRIDGE);
    }

    @Override
    public TimestreamConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            LOGGER.finer("Unsupported URL: " + url);
            return null;
        }

        LOGGER.fine("Timestream JDBC driver version: " + DRIVER_VERSION);
        LOGGER.finer("Instantiating a TimestreamConnection from TimestreamDriver.");
        return createTimestreamConnection(parseUrl(url, info),
            new ClientConfiguration().withUserAgentSuffix(getUserAgentSuffix()));
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public Logger getParentLogger() {
        return LOGGER;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final String tempUrl;
        if (acceptsURL(url)) {
            tempUrl = url;
        } else {
            LOGGER.warning("Unsupported input URL: \""
              + url
              + "\", the default URL \""
              + Constants.URL_PREFIX
              + "\" will be used.");
            tempUrl = Constants.URL_PREFIX;
        }
        final Properties finalInfo = parseUrl(tempUrl, info);

        return Stream.of(TimestreamConnectionProperty.values())
          .map(value -> {
              final DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(
                value.getConnectionProperty(),
                finalInfo.getProperty(value.getConnectionProperty()));

              driverPropertyInfo.description = value.getDescription();
              return driverPropertyInfo;
          })
          .toArray(DriverPropertyInfo[]::new);
    }

    /**
     * Get the User Agent Suffix.
     *
     * @return The User Agent Suffix.
     */
    static String getUserAgentSuffix() {
        return Constants.UA_ID_PREFIX + DRIVER_VERSION + APP_NAME_SUFFIX;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Get the name of the currently running application.
     *
     * @return the name of the currently running application.
     */
    private static String getApplicationName() {
        // Currently not supported.
        // Need to implement logic to get the process ID of the current process, then check the set of running processes and pick out
        // the one that matches the current process. From there we can grab the name of what is running the process.
        return "Unknown";
    }

    /**
     * Creates an {@link TimestreamConnection} instance.
     *
     * @param properties    The properties to be used to create a connection.
     * @param configuration The client configuration to be used to create a connection.
     * @return an {@link TimestreamConnection} instance.
     * @throws SQLException if property is not supported by the driver.
     */
    protected TimestreamConnection createTimestreamConnection(final Properties properties,
        final ClientConfiguration configuration)
        throws SQLException {
        return new TimestreamConnection(properties, configuration);
    }

    /**
     * Parse the given URL into key value pairs and combine with the connection properties.
     *
     * @param url The JDBC connection URL.
     * @param info The JDBC connection properties.
     * @return The combined key/value pairs to make a connection.
     * @throws SQLException if the URL cannot be properly parsed.
     */
    private Properties parseUrl(String url, Properties info) throws SQLException {
        if (null == url) {
            // This should not occur, as there is a guard of acceptsURL before this call.
            LOGGER.warning(
                "The URL passed to TimestreamDriver#parseUrl is null. This should not be possible.");
            return new Properties();
        }
        String trimmedUrl = url.trim().substring(Constants.URL_PREFIX.length());
        if (!Strings.isNullOrEmpty(trimmedUrl)) {
            trimmedUrl = trimmedUrl.substring(Constants.URL_BRIDGE.length());
        }

        final Properties allProperties = new Properties();
        if (null != info) {
            info.forEach((key, value) -> {
                final String keyString = key.toString();
                if (TimestreamConnectionProperty.isSupportedProperty(keyString)) {
                  allProperties.put(keyString, value);
                } else {
                  LOGGER.warning("Ignored unsupported property: " + keyString);
                }
            });
        }

        if (!Strings.isNullOrEmpty(trimmedUrl)) {
            for (final String keyValueStr : trimmedUrl.split(";")) {
                final String[] keyValue = keyValueStr.trim().split("=");
                if ((2 != keyValue.length) || keyValue[0].isEmpty()) {
                    throw Error.createSQLException(LOGGER, Error.INVALID_CONNECTION_PROPERTIES, url);
                }
                final String trimmedKeyStr = keyValue[0].trim();
                if (TimestreamConnectionProperty.isSupportedProperty(trimmedKeyStr)) {
                    allProperties.put(trimmedKeyStr, keyValue[1]);
                } else {
                    LOGGER.warning("Ignored unsupported property: " + trimmedKeyStr);
                }
            }
        }

        return allProperties;
    }
}
