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

import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Constants used within the integration test.
 */
final class Constants {
  static final String URL = "jdbc:timestream";
  static final String DATABASE_NAME = "JDBC_Integration07_Test_DB";
  static final String TABLE_NAME = "Integration_Test_Table_07";
  static final String NON_EXISTENT_TABLE_NAME = "NotExistTableName";
  static final String[] DATABASES_NAMES = new String[]{
      "FAST_PATH_TEST_DB",
      "JDBC_Integration07_Test_DB",
      "TestV1",
      "devops",
      "grafanaPerfDB",
      "grafana_db",
      "perf07",
      "performance07",
      "sampleDB",
      "testDB"
  };
  static final String[] COLUMN_NAMES = new String[]{
      "hostname",
      "az",
      "region",
      "measure_value::bigint",
      "measure_value::double",
      "measure_value::boolean",
      "measure_value::varchar",
      "measure_name", "time"
  };
  static final String[] MEASURE_VALUE_COLUMNS = new String[]{
      "measure_value::bigint",
      "measure_value::double",
      "measure_value::boolean",
      "measure_value::varchar",
  };

  static final int TABLE_COLUMN_NUM = 9;
  static final int TABLE_ROW_SIZE = 4;
  static final long HT_TTL_HOURS = 24L;
  static final long CT_TTL_DAYS = 7L;
  static final String DOUBLE_VALUE = "13.5";
  static final String BIGINT_VALUE = "9223372036";
  static final String VARCHAR_VALUE = "foo";
  static final String BOOLEAN_VALUE = "true";

  static final Map<MeasureValueType, String> DATATYPE_VALUE = new ImmutableMap.Builder<MeasureValueType, String>()
      .put(MeasureValueType.DOUBLE, DOUBLE_VALUE)
      .put(MeasureValueType.BIGINT, BIGINT_VALUE)
      .put(MeasureValueType.VARCHAR, VARCHAR_VALUE)
      .put(MeasureValueType.BOOLEAN, BOOLEAN_VALUE)
      .build();

  private Constants() { }
}
