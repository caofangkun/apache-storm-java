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

import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.collections.SetUtils;
import org.apache.storm.ClojureClass;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.MutableInt;

@ClojureClass(className = "backtype.storm.utils-test")
public class CoreUtilTest {
  private static Map conf;

  @BeforeClass
  public static void setUp() {
    try {
      conf = new HashMap<String, Object>();
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.utils-test#test-new-curator-uses-exponential-backoff")
  public void testNewCuratorUsesExponentialBackoff() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.utils-test#test-getConfiguredClient-throws-RunTimeException-on-bad-config")
  public void testGetConfiguredClientThrowsRunTimeExceptionOnBadConfig() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.utils-test#test-getConfiguredClient-throws-RunTimeException-on-bad-args")
  public void testGetConfiguredClientThrowsRunTimeExceptionOnBadArgs() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.utils-test#test-secs-to-millis-long")
  public void testSecsToMillisLong() {

  }

  public static String getTestFilePath(String fileName) {
    URL url = CoreUtilTest.class.getClass().getResource("/" + fileName);
    return url.getPath();
  }

  @Test
  public void testZipContainsDir() {
    String pt = getTestFilePath("test.jar");
    boolean isExists = CoreUtil.zipContainsDir(pt, "resources");
    Assert.assertEquals(true, isExists);
  }

  @Test
  public void testAcquireRandomRangeId() {
    List<Integer> list = new ArrayList<Integer>();
    list.add(1);
    list.add(2);
    list.add(3);
    Integer res =
        CoreUtil.acquireRandomRangeId(new MutableInt(-1), list, new Random());
    Assert.assertTrue(res != null);
  }

  @Test
  public void testParseList() {
    List<String> result =
        CoreUtil.parseList(conf.get(Config.STORM_ZOOKEEPER_SERVERS),
            Arrays.asList("localhost"));
    Assert.assertEquals(Arrays.asList("localhost"), result);
    conf.put(Config.STORM_ZOOKEEPER_SERVERS,
        Arrays.asList("server1", "server2"));
    result =
        CoreUtil.parseList(conf.get(Config.STORM_ZOOKEEPER_SERVERS),
            Arrays.asList("localhost"));
    Assert.assertEquals(Arrays.asList("server1", "server2"), result);
  }

  @Test
  public void testParseInt() {
    int defaultValue = (Integer) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
    Assert.assertEquals(30, defaultValue);
    int secs = CoreUtil.parseInt(defaultValue, 40);
    Assert.assertEquals(30, secs);
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, null);
    secs =
        CoreUtil.parseInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 40);
    Assert.assertEquals(40, secs);
  }

  @Test
  public void testParseBoolean() {
    boolean defaultValue = (Boolean) conf.get(Config.TOPOLOGY_DEBUG);
    Assert.assertFalse(defaultValue);
    boolean flag = CoreUtil.parseBoolean(defaultValue, true);
    Assert.assertFalse(flag);
    conf.put(Config.TOPOLOGY_DEBUG, null);
    flag = CoreUtil.parseBoolean(conf.get(Config.TOPOLOGY_DEBUG), true);
    Assert.assertTrue(flag);
  }

  @Test
  public void testmap_diff() {
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("1", "a");
    m1.put("2", "a");
    m1.put("3", "b");
    Map<String, String> ret = CoreUtil.map_diff(m1, m1);
    Assert.assertEquals(0, ret.size());

    Map<String, String> m2 = new HashMap<String, String>();
    m2.put("1", "a");
    m2.put("4", "a");

    ret = CoreUtil.map_diff(m1, m2);
    Assert.assertEquals(1, ret.size());
    Assert.assertEquals("a", ret.get("4"));

    ret = CoreUtil.map_diff(m2, m1);
    Assert.assertEquals(2, ret.size());
    Assert.assertEquals("a", ret.get("2"));
    Assert.assertEquals("b", ret.get("3"));
  }

  @Test
  public void testPartitionFixed() {
    List<Integer> aseq = new ArrayList<Integer>();
    for (int i = 0; i < 10; i++) {
      aseq.add((Integer) i);
    }
    List<List<Integer>> ret = CoreUtil.partitionFixed(5, aseq);
    System.out.println(ret.toString());
    Assert.assertEquals(5, ret.size());
    Assert.assertEquals(2, ret.get(0).size());
    Assert.assertEquals(2, ret.get(1).size());
    Assert.assertEquals(2, ret.get(2).size());
    Assert.assertEquals(2, ret.get(3).size());
    Assert.assertEquals(2, ret.get(4).size());

    ret = CoreUtil.partitionFixed(3, aseq);
    Assert.assertEquals(3, ret.size());
    Assert.assertEquals(4, ret.get(0).size());
    Assert.assertEquals(3, ret.get(1).size());
    Assert.assertEquals(3, ret.get(2).size());

    ret = CoreUtil.partitionFixed(4, aseq);
    Assert.assertEquals(4, ret.size());

    Assert.assertEquals(3, ret.get(0).size());
    Assert.assertEquals((Integer) 0, ret.get(0).get(0));
    Assert.assertEquals((Integer) 1, ret.get(0).get(1));
    Assert.assertEquals((Integer) 2, ret.get(0).get(2));

    Assert.assertEquals(3, ret.get(1).size());
    Assert.assertEquals((Integer) 3, ret.get(1).get(0));
    Assert.assertEquals((Integer) 4, ret.get(1).get(1));
    Assert.assertEquals((Integer) 5, ret.get(1).get(2));

    Assert.assertEquals(2, ret.get(2).size());
    Assert.assertEquals((Integer) 6, ret.get(2).get(0));
    Assert.assertEquals((Integer) 7, ret.get(2).get(1));

    Assert.assertEquals(2, ret.get(3).size());
    Assert.assertEquals((Integer) 8, ret.get(3).get(0));
    Assert.assertEquals((Integer) 9, ret.get(3).get(1));
  }

  @Test
  public void testTokenize_path() {
    String test = new String("home/xiong/jstorm");
    List<String> expected = new ArrayList<String>();

    expected.add("home");
    expected.add("xiong");
    expected.add("jstorm");
    Assert.assertEquals(expected, CoreUtil.tokenizePath(test));
  }

  @Test
  public void testToks_to_path() {
    String test = new String("/home/xiong/jstorm");
    List<String> expected = new ArrayList<String>();

    expected.add("home");
    expected.add("xiong");
    expected.add("jstorm");

    Assert.assertEquals(test, CoreUtil.toksToPath(expected));
  }

  @Test
  public void testSelect_keys() {
    Map<Integer, String> exepcted = new HashMap<Integer, String>();
    Set<Integer> filter = new HashSet<Integer>();

    exepcted.put(1, "a");
    exepcted.put(2, "b");
    exepcted.put(3, "c");

    filter.add(1);
    filter.add(2);
    filter.add(3);

    Assert.assertEquals(3, CoreUtil.select_keys(exepcted, filter).size());

    Map<Integer, String> exepcted1 = new HashMap<Integer, String>();
    Set<Integer> filter1 = new HashSet<Integer>();

    exepcted1.put(1, "a");
    exepcted1.put(2, "b");
    exepcted1.put(3, "c");

    filter1.add(1);
    filter1.add(2);
    filter1.add(4);
    Assert.assertEquals(2, CoreUtil.select_keys(exepcted1, filter1).size());
  }

  @Test
  public void testSelect_keys_pred() {
    Map<Integer, String> exepcted = new HashMap<Integer, String>();
    Set<Integer> filter = new HashSet<Integer>();

    exepcted.put(1, "a");
    exepcted.put(2, "b");
    exepcted.put(3, "c");

    filter.add(1);
    filter.add(2);

    Assert.assertEquals(1, CoreUtil.select_keys_pred(filter, exepcted).size());
  }

  @Test
  public void testInterleaveAll() {
    List<Integer> list1 = Arrays.asList(1, 2, 3, 7);
    List<Integer> list3 = Arrays.asList(4, 5, 6);
    List<Integer> list2 = Arrays.asList(8);

    List<List<Integer>> inputs = new ArrayList<List<Integer>>();
    inputs.add(list1);
    inputs.add(list2);
    inputs.add(list3);

    List<Integer> result = CoreUtil.interleaveAll(inputs);
    Assert.assertTrue(SetUtils.isEqualSet(
        Arrays.asList(1, 8, 4, 2, 5, 3, 6, 7), result));

  }

  @Test
  public void testAnyIntersection() {
    List<String> list = new ArrayList<String>();
    List<String> result = null;
    list.add("a");
    list.add("b");

    result = CoreUtil.anyIntersection(list);
    Assert.assertTrue(result.isEmpty());

    list.add("b");
    result = CoreUtil.anyIntersection(list);
    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void testLocalHostname() {
    String hostname;
    try {
      hostname = CoreUtil.localHostname();
      Assert.assertNotNull(hostname);
    } catch (UnknownHostException e) {
      fail(CoreUtil.stringifyError(e));
    }

  }

  @Test
  public void testSetDifference() {
    Set<String> strSet1 = new HashSet<String>();
    strSet1.add("a");
    strSet1.add("b");
    Set<String> strSet2 = new HashSet<String>();
    strSet2.add("a");
    strSet2.add("c");
    Set<String> result = CoreUtil.set_difference(strSet1, strSet2);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.contains("b"));

    WorkerSlot ws1 =
        new WorkerSlot("1162f9d6-90f5-4487-98fe-5f1693ec2b50", 38977);
    Set<WorkerSlot> currentConnections = new HashSet<WorkerSlot>();
    currentConnections.add(ws1);
    WorkerSlot ws2 =
        new WorkerSlot("1162f9d6-90f5-4487-98fe-5f1693ec2b50", 38977);
    Set<WorkerSlot> neededConnections = new HashSet<WorkerSlot>();
    neededConnections.add(ws2);
    Set<WorkerSlot> removeConnections =
        CoreUtil.set_difference(currentConnections, neededConnections);
    Assert.assertEquals(0, removeConnections.size());
    Set<WorkerSlot> newConnections =
        CoreUtil.set_difference(neededConnections, currentConnections);
    Assert.assertEquals(0, newConnections.size());
  }

  @Test
  public void testSetIntersection() {
    Set<String> strSet1 = new HashSet<String>();
    strSet1.add("a");
    strSet1.add("b");

    Set<String> intersection =
        CoreUtil.SetIntersection(strSet1, new HashSet<String>());
    Assert.assertTrue(intersection.isEmpty());

    Set<String> strSet2 = new HashSet<String>();
    strSet2.add("a");
    strSet2.add("c");

    intersection = CoreUtil.SetIntersection(strSet1, strSet2);
    Assert.assertEquals(1, intersection.size());
    Assert.assertTrue(intersection.contains("a"));
  }

  @Test
  public void testDistinctList() {
    // user=> (distinct [1 2 1 3 1 4 1 5])
    // (1 2 3 4 5)
    List<Integer> input = Arrays.asList(1, 2, 1, 3, 1, 4, 1, 5);
    Assert.assertTrue(SetUtils.isEqualSet(Arrays.asList(1, 2, 3, 4, 5),
        CoreUtil.distinctList(input)));
  }

  @Test
  public void testMultiply() {
    Double value1 = 12d;
    Integer value2 = 10;
    Assert.assertEquals(120d, CoreUtil.multiply(value1, value2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMergeWith() {
    // merge two maps using the addition function
    Map<String, Integer> map1 = new HashMap<String, Integer>();
    map1.put("a", 1);
    map1.put("b", 2);

    Map<String, Integer> map2 = new HashMap<String, Integer>();
    map2.put("a", 9);
    map2.put("b", 98);
    map2.put("c", 0);

    Map<String, Integer> mergeWith = CoreUtil.mergeWith(map1, map2);
    // System.out.println(mergeWith.toString());
    Assert.assertEquals(3, mergeWith.size());
    Assert.assertEquals(0, mergeWith.get("c").intValue());
    Assert.assertEquals(100, mergeWith.get("b").intValue());
    Assert.assertEquals(10, mergeWith.get("a").intValue());

    // 'merge-with' works with an arbitrary number of maps
    Map<String, Integer> map3 = new HashMap<String, Integer>();
    map3.put("a", 10);
    map3.put("b", 100);
    map3.put("c", 10);
    Map<String, Integer> map4 = new HashMap<String, Integer>();
    map4.put("a", 5);
    Map<String, Integer> map5 = new HashMap<String, Integer>();
    map5.put("d", 42);
    map5.put("c", 5);

    mergeWith = CoreUtil.mergeWith(map1, map2, map3, map4, map5);
    // System.out.println(mergeWith.toString());
    // {:d 42, :c 15, :a 25, :b 200}
    Assert.assertEquals(4, mergeWith.size());
    Assert.assertEquals(25, mergeWith.get("a").intValue());
    Assert.assertEquals(200, mergeWith.get("b").intValue());
    Assert.assertEquals(15, mergeWith.get("c").intValue());
    Assert.assertEquals(42, mergeWith.get("d").intValue());

  }
}
