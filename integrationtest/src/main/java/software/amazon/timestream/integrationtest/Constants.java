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
      "JDBC_Integration07_Test_DB",
      "Integration_Test_DB01",
      "Integration_Test_DB02"
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

  static final String NO_DB_NO_TB_REGION = "ap-southeast-2";
  static final String[] NO_DB_DATABASE_NAMES = new String[]{};
  static final String[] NO_TB_TABLE_NAMES = new String[]{};

  static final String ONE_DB_NO_TB_REGION = "ap-northeast-1";
  static final String[] ONE_DB_NO_TB_DATABASE_NAME = new String[]{
      "EmptyDb_1_2.34"
  };
  static final String[] ONE_DB_NO_TB_TABLE_NAMES = new String[]{};

  static final String ONE_DB_ONE_TB_REGION = "eu-central-1";
  static final String[] ONE_DB_ONE_TB_DATABASE_NAME = new String[]{
      "JDBC_.IntegrationTestDB0088"
  };
  static final String[] ONE_DB_ONE_TB_TABLE_NAME = new String[]{
      "IntegrationTestTable0888",
  };

  static final String ONE_DB_MUTLI_TB_REGION = "eu-west-1";
  static final String[] ONE_DB_MUTLI_TB_DATABASES_NAME = new String[]{
      "JDBC_Inte.gration_Te.st_DB_01"
  };
  static final String[] ONE_DB_MUTLI_TB_TABLE_NAMES = new String[]{
      "Inte.gration_Tes_t_Tab_le_03",
      "Integ.ration_Te_st_T_able_01",
      "Integr.ation_Test_Ta_ble_02"
  };

  static final String[] MULTI_DB_MUTLI_TB_DATABASES_NAMES = new String[]{
      "JD_BC_Int.egration_Test_DB_001",
      "JDB.C_Integration-Test_DB_002",
      "JD-BC_Integration.Test_DB_003"
  };
  static final String[] MULTI_DB_MUTLI_TB_TABLE_NAMES1 = new String[]{
      "Inte-gration_Tes1t_Table_01_01",
      "Inte-gration2_Te-st_Table_01_02",
      "Inte-gration_Test_3Ta-ble_01_03"
  };
  static final String[] MULTI_DB_MUTLI_TB_TABLE_NAMES2 = new String[]{
      "Integration-Test_Ta1ble_02_01",
      "Integration.-Te-st_Table_02_02"
  };
  static final String[] MULTI_DB_MUTLI_TB_TABLE_NAMES3 = new String[]{
      "JD-BC_Integration-Test_Ta1ble_03_01",
      "JD-BC_Integration.-Te-st_Table_03_02",
      "JD-BC_Integration--Test2_Table_03_03",
      "JD-BC_Integration0-Te-st_Ta.ble_03_04"
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

  private Constants() {
  }
}
