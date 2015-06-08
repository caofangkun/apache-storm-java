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
package org.apache.storm.util;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;

import junit.framework.Assert;

import org.junit.Test;

public class NetWorkUtilsTest {

  @Test
  public void testHostname() {
    String hostname;
    try {
      hostname = NetWorkUtils.hostname();
      Assert.assertNotNull(hostname);
    } catch (UnknownHostException e) {
      fail(CoreUtil.stringifyError(e));
    }

  }

  @Test
  public void testAssignServerPort() {
    try {
      int newPort = NetWorkUtils.assignServerPort(2234);
      Assert.assertEquals(2234, newPort);
    } catch (IOException e) {
      fail(CoreUtil.stringifyError(e));
    }

    try {
      int newPort = NetWorkUtils.assignServerPort(-1);
      System.out.println(newPort);
      boolean flag = (newPort > 0);
      Assert.assertTrue(flag);
    } catch (IOException e) {
      fail(CoreUtil.stringifyError(e));
    }

    // TODO port in 2181
  }

}
