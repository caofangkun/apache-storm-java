/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm;

import java.util.Map;

import javax.security.auth.Subject;

/**
 * mock implementation of INimbusCredentialPlugin,IAutoCredentials and
 * ICredentialsRenewer for testing only.
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.MockAutoCred")
public class MockAutoCred implements INimbusCredentialPlugin, IAutoCredentials,
    ICredentialsRenewer {

  public static final String nimbusCredKey = "nimbusCredTestKey";
  public static final String nimbusCredVal = "nimbusTestCred";
  public static final String nimbusCredRenewVal = "renewedNimbusTestCred";
  public static final String gatewayCredKey = "gatewayCredTestKey";
  public static final String gatewayCredVal = "gatewayTestCred";
  public static final String gatewayCredRenewVal = "renewedGatewayTestCred";

  @SuppressWarnings("rawtypes")
  @Override
  public void renew(Map<String, String> cred, Map conf) {
    cred.put(nimbusCredKey, nimbusCredRenewVal);
    cred.put(gatewayCredKey, gatewayCredRenewVal);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf) {
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void populateCredentials(Map<String, String> creds, Map conf) {
    creds.put(nimbusCredKey, nimbusCredVal);
  }

  @Override
  public void populateCredentials(Map<String, String> creds) {
    creds.put(gatewayCredKey, gatewayCredVal);
  }

  @Override
  public void populateSubject(Subject subject, Map<String, String> credentials) {
    subject.getPublicCredentials().add(credentials.get(nimbusCredKey));
    subject.getPublicCredentials().add(credentials.get(gatewayCredKey));
  }

  @Override
  public void updateSubject(Subject subject, Map<String, String> credentials) {
    populateSubject(subject, credentials);
  }

  @Override
  public void shutdown() {

  }
}
