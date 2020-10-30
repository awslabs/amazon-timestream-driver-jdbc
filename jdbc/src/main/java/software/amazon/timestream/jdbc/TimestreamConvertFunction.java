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

import com.amazonaws.services.timestreamquery.model.Datum;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.function.Consumer;

/**
 * Lambda expressions parsing the given {@link com.amazonaws.services.timestreamquery.model.Datum}
 * object as a specific data type.
 *
 * @param <R> A specific Java or SQL data type to convert to.
 */
@FunctionalInterface
interface TimestreamConvertFunction<R> {
    /**
     * Convert the Datum value to a target type.
     *
     * @param data        The Timestream data.
     * @param postWarning The lambda that posts SQL warnings to {@link TimestreamBaseResultSet} when
     *                    values are truncated during the conversion.
     * @return the converted data.
     * @throws SQLException if there is an error converting the data.
     */
    R convert(Datum data, Consumer<SQLWarning> postWarning) throws SQLException;
}
