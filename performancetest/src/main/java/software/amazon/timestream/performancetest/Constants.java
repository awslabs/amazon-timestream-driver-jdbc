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

package software.amazon.timestream.performancetest;

/**
 * Constants used in the performance test.
 * <p>
 * Since the queries in {@link TimestreamPerformanceTest} are time sensitive, some constants may
 * need to be modified before running the tests.
 */
final class Constants {
  static final String URL_PREFIX = "jdbc:timestream";

  static final String DATABASE_NAME = "perf07";
  static final String TABLE_NAME = "perf";
  static final String START_TIME_60H = "ago(60h)";
  static final String START_TIME_48H = "ago(48h)";
  static final String END_TIME = "now()";
  static final String REGION = "us-east-1";
  static final String CELL = REGION + "-cell-1";
  static final String SILO = CELL + "-silo-1";
  static final String AVAILABILITY_ZONE1 = REGION + "-1";
  static final String MICROSERVICE_NAME = "apollo";
  static final String INSTANCE_TYPE = "r5.4xlarge";
  static final String OS_VERSION = "AL2";
  static final String INSTANCE_NAME0 = "i-zaZswmJk-apollo-0000.amazonaws.com";
  static final String THRESHOLD_VALUE = "30";
  static final String PROCESS_NAME_SERVER = "server";
  static final String JDK_11 = "JDK_11";

  // Private constructor to prevent instantiation.
  Constants() {
  }
}
