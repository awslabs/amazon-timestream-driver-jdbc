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

import com.amazonaws.services.securitytoken.model.AssumeRoleWithSAMLRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.http.impl.client.CloseableHttpClient;

import java.util.Map;

/**
 * Test subclass for TimestreamOktaCredentialsProvider that overrides the method that fetches the {@link
 * Credentials} from AWS to avoid a real connection.
 */
class MockTimestreamOktaCredentialsProvider extends TimestreamOktaCredentialsProvider {

  MockTimestreamOktaCredentialsProvider(final CloseableHttpClient mockHttpClient, final Map<String, String> oktaFieldsMap) {
    super(mockHttpClient, oktaFieldsMap);
  }

  @Override
  protected Credentials fetchCredentialsWithSAMLAssertion(AssumeRoleWithSAMLRequest samlRequest) {
    return new Credentials()
        .withAccessKeyId("mockAccessKeyID")
        .withSecretAccessKey("mockSecretAccessKey")
        .withSessionToken("mockSessionToken");
  }
}
