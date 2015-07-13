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
package org.apache.storm.config;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.ConfigValidation;
import backtype.storm.ConfigValidation.FieldValidator;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.config-test")
public class ConfigUtilTest {
  private static final String FieldValidator = null;
  private static Map conf = new HashMap<String, Object>();
  private static String tmpPath = "/tmp/now";

  @BeforeClass
  public static void setUp() {
    conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.05);
    conf.put(Config.STORM_LOCAL_DIR, tmpPath);
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-validity")
  public void testValidity() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_DEBUG, true);
    conf.put("q", "asasdasd");
    conf.put("aaa", new Integer("123"));
    conf.put("bbb", new Long("456"));
    conf.put("eee", Arrays.asList(1, 2, new Integer("3"), new Long("4")));
    Assert.assertTrue(Utils.isValidConf(conf));

    conf = new HashMap<String, Object>();
    conf.put("qqq", backtype.storm.utils.Utils.class);
    Assert.assertFalse(Utils.isValidConf(conf));
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-power-of-2-validator")
  public void testPowerOf2Validator() {
    FieldValidator validator =
        (FieldValidator) ConfigValidation.PowerOf2Validator;
    List testValues =
        Arrays.asList(42.42, 42, 23423423423L, -33, -32, -1, -0.00001, 0, -0,
            "Forty-two");

    boolean flag = true;
    try {
      for (Object x : testValues) {
        validator.validateField("test", x);
        Assert.assertTrue(false);
      }
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(flag);
    }

    testValues = Arrays.asList(64, 4294967296L, 1, null);

    flag = true;
    try {
      for (Object x : testValues) {
        validator.validateField("test", x);
      }
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(flag);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-list-validator")
  public void testListValidator() {

    FieldValidator validator =
        (FieldValidator) ConfigValidation.StringsValidator;
    List xParameter1 = Arrays.asList("Forty-two", 42);
    List xParameter3 = Arrays.asList(true, "false");
    List xParameter5 = Arrays.asList(null, "null");
    List x = Arrays.asList(xParameter1, 42, xParameter3, null, xParameter5);

    boolean flag = true;
    try {
      for (Object oneOfX : x) {
        validator.validateField("test", oneOfX);
        Assert.assertTrue(false);
      }
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(flag);
    }

    List xStr = Arrays.asList("not a list at all");
    boolean flag1 = true;
    try {
      for (Object oneOfXStr : xStr) {
        validator.validateField("test", oneOfXStr);
        Assert.assertFalse(false);
      }
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(flag);
    }

    List oneOfXMutilParameter1 = Arrays.asList("one", "two", "three");
    List oneOfXMutilParameter2 = new ArrayList();
    List oneOfXMutilParameter3 = Arrays.asList("42", "64");
    List xMuti =
        Arrays.asList(oneOfXMutilParameter1, oneOfXMutilParameter2,
            oneOfXMutilParameter3, null);
    boolean flag3 = false;
    try {
      for (Object oneOfXMti : xMuti) {
        validator.validateField("test", oneOfXMti);
      }
    } catch (IllegalArgumentException e) {
      flag3 = true;
    }
    Assert.assertTrue(flag);

  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-integer-validator")
  public void testIntegerValidator() {
    FieldValidator validator =
        (FieldValidator) ConfigValidation.IntegerValidator;
    validator.validateField("test", null);
    validator.validateField("test", 1000);
    boolean flag = false;
    try {
      validator.validateField("test", 1.34);
    } catch (Throwable e) {
      flag =
          Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, e);
    }
    Assert.assertTrue(flag);
    /*
     * System.out.println("hello"); boolean flag1 = false; try {
     * validator.validateField("test", Integer.MAX_VALUE + 1); } catch
     * (Throwable e) { flag1 =
     * Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, e); }
     * Assert.assertTrue(flag1);
     */
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-integers-validator")
  public void testIntegersValidator() {
    FieldValidator validator =
        (FieldValidator) ConfigValidation.IntegersValidator;
    validator.validateField("test", null);
    validator.validateField("test", Arrays.asList(1000, 0, -1000));

    boolean flag1 = false;
    try {
      validator.validateField("test", Arrays.asList(0, 10, 1.34));
    } catch (Throwable e) {
      flag1 =
          Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, e);
    }
    Assert.assertTrue(flag1);

    boolean flag2 = false;
    try {
      validator.validateField("test", Arrays.asList(0, null));
    } catch (Throwable e) {
      flag2 =
          Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, e);
    }
    Assert.assertTrue(flag2);
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-double-validator")
  public void testDoubleValidator() {
    FieldValidator validator =
        (FieldValidator) ConfigValidation.IntegersValidator;
    validator.validateField("test", null);
    validator.validateField("test", 10);
    // we can provide lenient way to convert int/long to double with losing
    // precision
    validator.validateField("test", Integer.MAX_VALUE);
    validator.validateField("test", Integer.MAX_VALUE + 1);
    validator.validateField("test", Double.MAX_VALUE);
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-topology-workers-is-integer")
  public void testTopologyWorkersIsInteger() throws SecurityException,
      IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
    Map<Object, FieldValidator> configSchemaMap = ConfigUtil.configSchemaMap();

    FieldValidator validator = configSchemaMap.get(Config.TOPOLOGY_WORKERS);
    validator.validateField("test", new Integer(42));

    boolean flag = false;
    try {
      validator.validateField("test", new Double(3.14159));
    } catch (Throwable e) {
      flag =
          Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, e);
    }
    // Assert.assertTrue(flag);
  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-topology-stats-sample-rate-is-float")
  public void testTopologyStatsSampleRateIsFloat() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-isolation-scheduler-machines-is-map")
  public void testIsolationSchedulerMachinesIsMap() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.config-test#test-worker-childopts-is-string-or-string-list")
  public void testWorkerChildoptsIsStringOrStringList() {
    List listParameter = Arrays.asList("some", "string", "list");
    String strParameter = new String("some string");
    List passToCases = Arrays.asList(null, strParameter, listParameter);

  }

  @Test
  public void testClusterMode() {
    conf.put(Config.STORM_CLUSTER_MODE, "local");
    Assert.assertEquals("local", ConfigUtil.clusterMode(conf));
    Assert.assertTrue(ConfigUtil.isLocalMode(conf));
    conf.put(Config.STORM_CLUSTER_MODE, "distributed");
    Assert.assertEquals("distributed", ConfigUtil.clusterMode(conf));
    Assert.assertFalse(ConfigUtil.isLocalMode(conf));
  }

  @Test
  public void testSamplingRate() {
    Assert.assertEquals(20, ConfigUtil.samplingRate(conf).intValue());
  }

  @Test
  public void testWorkerRoot() {
    try {
      String workerRoot = ConfigUtil.workerRoot(conf, "topology_test");
      if (!CoreUtil.existsFile(workerRoot)) {
        FileUtils.forceMkdir(new File(workerRoot));
      }
      Assert.assertEquals(tmpPath + "/workers/topology_test", workerRoot);
    } catch (IOException e) {
      fail(CoreUtil.stringifyError(e));
    }
  }

  @Test
  public void testWorkerPidsRoot() {
    try {
      String pidRoot = ConfigUtil.workerPidsRoot(conf, "topology_test");
      Assert.assertEquals(tmpPath + "/workers/topology_test/pids", pidRoot);
    } catch (IOException e) {
      fail(CoreUtil.stringifyError(e));
    }
  }

  @AfterClass
  public static void cleanUp() {
    try {
      CoreUtil.rmr(tmpPath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
