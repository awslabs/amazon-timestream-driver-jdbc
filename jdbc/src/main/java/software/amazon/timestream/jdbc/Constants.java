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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Constants used within the Timestream JDBC driver.
 */
final class Constants {
  static final String URL_PREFIX = "jdbc:timestream";
  static final String URL_BRIDGE = "://";
  static final String UA_ID_PREFIX = "ts-jdbc.";
  static final String PROPERTIES_FILE_CREDENTIALS_PROVIDER_CLASSNAME = "propertiesfilecredentialsprovider";
  static final String INSTANCE_PROFILE_CREDENTIALS_PROVIDER_CLASSNAME = "instanceprofilecredentialsprovider";
  static final String OKTA_IDP_NAME = "okta";
  static final String OKTA_AWS_APP_NAME = "amazon_aws";
  static final String AAD_IDP_NAME = "azuread";
  static final int MAX_FETCH_SIZE = 1000;
  static final int NUM_MILLISECONDS_IN_SECOND = 1000;

  static final String TABLE_TYPE = "TABLE";
  static final String YES_STRING = "YES";
  static final String NO_STRING = "NO";
  static final String INDEX = "INDEX";
  static final String VALUE = "VALUE";
  static final String DELIMITER = ", ";

  // SQLSTATE code representing operation canceled.
  static final String OPERATION_CANCELED_SQL_STATE = "HY008";

  // SQLSTATE code representing error occurred while creating a connection.
  static final String CONNECTION_EXCEPTION_SQL_STATE = "08000";

  // SQLSTATE code representing error occurred while validating the connection.
  static final String CONNECTION_FAILURE_SQL_STATE = "08006";

  static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .optionalStart()
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .optionalEnd()
    .toFormatter();

  static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd")
    .optionalStart()
    .appendPattern(" HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .optionalEnd()
    .toFormatter();

  static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
    .appendOptional(DATE_TIME_FORMATTER)
    .appendOptional(new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .toFormatter())
    .toFormatter();

  private Constants() { }
}
