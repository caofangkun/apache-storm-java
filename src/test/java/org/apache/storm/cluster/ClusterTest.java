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
package org.apache.storm.cluster;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.NetWorkUtils;
import org.apache.storm.zk.InprocessZookeeper;
import org.apache.storm.zookeeper.Watcher.Event.EventType;
import org.apache.storm.zookeeper.ZooDefs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.SupervisorInfo;
import backtype.storm.generated.TopologyStatus;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.cluster-test")
public class ClusterTest {
  private static InprocessZookeeper zookeeper;
  private static Map stormConf;

  @BeforeClass
  public static void setUp() {
    stormConf = ConfigUtil.readDefaultConfig();
    stormConf.put(Config.STORM_LOCAL_DIR, CoreUtil.localTempPath());
    zookeeper = new InprocessZookeeper(stormConf);
    try {
      zookeeper.start();
      Thread.sleep(3000);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-basics")
  public void testBasics() {

    try {
      DistributedClusterState state = new DistributedClusterState(stormConf);

      state.setData("/root", new byte[] { 1, 2, 3 },
          ZooDefs.Ids.OPEN_ACL_UNSAFE);

      Assert.assertTrue(Arrays.equals(new byte[] { 1, 2, 3 },
          state.getData("/root", false)));
      Assert.assertNull(state.getData("/a", false));

      state
          .setData("/root/a", new byte[] { 1, 2 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      state.setData("/root", new byte[] { 1 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);

      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state.getData("/root", false)));
      Assert.assertTrue(Arrays.equals(new byte[] { 1, 2 },
          state.getData("/root/a", false)));

      state.setData("/a/b/c/d", new byte[] { 99 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertTrue(Arrays.equals(new byte[] { 99 },
          state.getData("/a/b/c/d", false)));

      state.mkdirs("/lalala", ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertTrue(state.getChildren("/lalala", false).isEmpty());

      HashSet<String> childrenSet = new HashSet<String>();
      childrenSet.add("a");
      childrenSet.add("root");
      childrenSet.add("lalala");
      Assert.assertEquals(childrenSet,
          new HashSet<String>(state.getChildren("/", false)));

      state.deleteNode("/a");
      childrenSet.remove("a");
      Assert.assertEquals(childrenSet,
          new HashSet<String>(state.getChildren("/", false)));
      Assert.assertNull(state.getData("/a/b/c/d", false));

      state.close();
    } catch (Exception e) {
      Assert.assertNull("testBasics failed", e);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-multi-state")
  public void testMultiState() {

    try {
      DistributedClusterState state1 = new DistributedClusterState(stormConf);
      DistributedClusterState state2 = new DistributedClusterState(stormConf);

      state1.setData("/root", new byte[] { 1 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state1.getData("/root", false)));
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state2.getData("/root", false)));

      state2.deleteNode("/root");
      Assert.assertNull(state1.getData("/root", false));
      Assert.assertNull(state2.getData("/root", false));

      state1.close();
      state2.close();
    } catch (Exception e) {
      Assert.assertNull("testMultiState failed", e);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-ephemeral")
  public void testEphemeral() {

    try {
      DistributedClusterState state1 = new DistributedClusterState(stormConf);
      DistributedClusterState state2 = new DistributedClusterState(stormConf);
      DistributedClusterState state3 = new DistributedClusterState(stormConf);

      state1.setEphemeralNode("/a", new byte[] { 1 },
          ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state1.getData("/a", false)));
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state2.getData("/a", false)));

      state3.close();
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state1.getData("/a", false)));
      Assert.assertTrue(Arrays.equals(new byte[] { 1 },
          state2.getData("/a", false)));

      state1.close();
      Assert.assertNull(state2.getData("/a", false));

      state2.close();
    } catch (Exception e) {
      Assert.assertNull("testEphemeral failed", e);
    }
  }

  @ClojureClass(className = "backtype.storm.cluster-test#read-and-reset!")
  private Object readAndReset(AtomicReference atom) throws InterruptedException {
    long time = System.currentTimeMillis();
    Object val = null;
    while (true) {
      if ((val = atom.get()) != null) {
        atom.set(null);
        return val;
      } else {
        if (System.currentTimeMillis() - time > 30000) {
          throw new RuntimeException("Waited too long for atom to change state");
        }
        Thread.sleep(10);
      }
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-callbacks")
  public void testCallbacks() {
    try {
      DistributedClusterState[] state = new DistributedClusterState[2];
      final AtomicReference<Map>[] stateLastCallback = new AtomicReference[2];
      for (int i = 0; i < 2; i++) {
        state[i] = new DistributedClusterState(stormConf);
        stateLastCallback[i] = new AtomicReference<Map>();
        final AtomicReference<Map> tmp = stateLastCallback[i];

        state[i].register(new ClusterStateCallback() {
          public <T> Object execute(T... vals) {
            Assert.assertNotNull(vals);
            Assert.assertEquals(2, vals.length);

            EventType type = (EventType) vals[0];
            String path = (String) vals[1];

            HashMap<String, Object> m = new HashMap<String, Object>();
            m.put("type", type);
            m.put("path", path);
            tmp.set(m);

            return null;
          }
        });
      }

      state[0].setData("/root", new byte[] { 1 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      state[1].getData("/root", true);

      Assert.assertNull(stateLastCallback[0].get());
      Assert.assertNull(stateLastCallback[1].get());

      state[1].setData("/root", new byte[] { 2 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      HashMap<String, Object> actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      HashMap<String, Object> expected = new HashMap<String, Object>();
      expected.put("type", EventType.NodeDataChanged);
      expected.put("path", "/root");
      Assert.assertEquals(expected, actual);
      Assert.assertNull(stateLastCallback[0].get());

      state[1].setData("/root", new byte[] { 3 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertNull(stateLastCallback[1].get());
      state[1].getData("/root", true);
      state[1].getData("/root", false);
      state[0].deleteNode("/root");
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeDeleted);
      Assert.assertEquals(expected, actual);

      state[1].getData("/root", true);
      state[0].setEphemeralNode("/root", new byte[] { 1, 2, 3, 4 },
          ZooDefs.Ids.OPEN_ACL_UNSAFE);
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeCreated);
      Assert.assertEquals(expected, actual);

      state[0].getChildren("/", true);
      state[1].setData("/a", new byte[] { 9 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertNull(stateLastCallback[1].get());
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[0]);
      expected.put("type", EventType.NodeChildrenChanged);
      expected.put("path", "/");
      Assert.assertEquals(expected, actual);

      state[1].getData("/root", true);
      state[0].setEphemeralNode("/root", new byte[] { 1, 2 },
          ZooDefs.Ids.OPEN_ACL_UNSAFE);
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeDataChanged);
      expected.put("path", "/root");

      state[0].mkdirs("/ccc", ZooDefs.Ids.OPEN_ACL_UNSAFE);
      state[0].getChildren("/ccc", true);
      state[1].getData("/ccc/b", true);
      state[1].setData("/ccc/b", new byte[] { 8 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeCreated);
      expected.put("path", "/ccc/b");
      Assert.assertEquals(expected, actual);
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[0]);
      expected.put("type", EventType.NodeChildrenChanged);
      expected.put("path", "/ccc");
      Assert.assertEquals(expected, actual);

      state[1].getData("/root", true);
      state[1].getData("/root2", true);
      state[0].close();

      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeDeleted);
      expected.put("path", "/root");
      Assert.assertEquals(expected, actual);

      state[1].setData("/root2", new byte[] { 9 }, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      actual =
          (HashMap<String, Object>) this.readAndReset(stateLastCallback[1]);
      expected.put("type", EventType.NodeCreated);
      expected.put("path", "/root2");
      Assert.assertEquals(expected, actual);
      state[1].close();

    } catch (Exception e) {
      Assert.assertNull("testCallbacks failed", e);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-storm-cluster-state-basics")
  public void testStormClusterStateBasics() {
    try {
      StormZkClusterState state = new StormZkClusterState(stormConf);
      Map<ExecutorInfo, WorkerSlot> executorToNodeport1 =
          new HashMap<ExecutorInfo, WorkerSlot>();
      executorToNodeport1
          .put(new ExecutorInfo(1, 1), new WorkerSlot("2", 2002));
      Assignment assignment1 =
          new Assignment("/aaa", executorToNodeport1,
              new HashMap<String, String>(),
              new HashMap<ExecutorInfo, Integer>());
      Map<ExecutorInfo, WorkerSlot> executorToNodeport2 =
          new HashMap<ExecutorInfo, WorkerSlot>();
      executorToNodeport2
          .put(new ExecutorInfo(1, 1), new WorkerSlot("2", 2002));
      Assignment assignment2 =
          new Assignment("/aaa", executorToNodeport2,
              new HashMap<String, String>(),
              new HashMap<ExecutorInfo, Integer>());
      StormBase base1 = new StormBase("/tmp/storm1", TopologyStatus.ACTIVE, 2);
      base1.set_launch_time_secs(1);
      base1.set_component_executors(new HashMap<String, Integer>());
      base1.set_owner("unknown");
      StormBase base2 = new StormBase("/tmp/storm2", TopologyStatus.ACTIVE, 2);
      base1.set_launch_time_secs(2);
      base1.set_component_executors(new HashMap<String, Integer>());
      base1.set_owner("unknown");

      Assert.assertTrue(state.assignments(null).isEmpty());
      state.setAssignment("storm1", assignment1);
      Assert.assertEquals(assignment1, state.assignmentInfo("storm1", null));
      Assert.assertNull(state.assignmentInfo("storm3", null));

      state.setAssignment("storm1", assignment2);
      state.setAssignment("storm3", assignment1);
      Assert.assertEquals(
          new HashSet<String>(Arrays.asList("storm1", "storm3")),
          new HashSet<String>(state.assignments(null)));
      Assert.assertEquals(assignment2, state.assignmentInfo("storm1", null));
      Assert.assertEquals(assignment1, state.assignmentInfo("storm3", null));

      Assert.assertTrue(state.activeStorms().isEmpty());
      state.activateStorm("storm1", base1);
      Assert.assertEquals(Arrays.asList("storm1"), state.activeStorms());
      Assert.assertEquals(base1, state.stormBase("storm1", null));
      Assert.assertNull(state.stormBase("storm2", null));

      state.activateStorm("storm2", base2);
      Assert.assertEquals(base1, state.stormBase("storm1", null));
      Assert.assertEquals(base2, state.stormBase("storm2", null));
      Assert.assertEquals(
          new HashSet<String>(Arrays.asList("storm1", "storm2")),
          new HashSet<String>(state.activeStorms()));

      state.removeStormBase("storm1");
      Assert.assertEquals(base2, state.stormBase("storm2", null));
      Assert.assertEquals(new HashSet<String>(Arrays.asList("storm2")),
          new HashSet<String>(state.activeStorms()));

      state.disconnect();

    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
    }
  }

  @ClojureClass(className = "backtype.storm.cluster-test#validate-errors!")
  private void validateErrors(StormZkClusterState state, String stormId,
      String componentId, List<String> errorsList) throws Exception {

    List<ErrorInfo> errors = state.errors(stormId, componentId);
    // System.out.println(errors);
    Assert.assertEquals(errors.size(), errorsList.size());

    Iterator<ErrorInfo> i = errors.iterator();
    Iterator<String> j = errorsList.iterator();
    while (i.hasNext()) {
      ErrorInfo error = i.next();
      String target = j.next();
      if (!error.get_error().equals(target)) {
        System.out.println(target + "=>" + error.get_error());
      }
      Assert.assertEquals(error.get_error(), target);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-storm-cluster-state-errors")
  public void testStormClusterStateErrors() {

    Time.startSimulating();

    try {
      StormZkClusterState state = new StormZkClusterState(stormConf);

      Exception exception1 = new RuntimeException();
      state.reportError("a", "1", NetWorkUtils.hostname(), 6700, exception1);
      this.validateErrors(state, "a", "1",
          Arrays.asList(CoreUtil.stringifyError(exception1)));
      Time.advanceTime(1000);

      Exception exception2 = new IllegalArgumentException();
      state.reportError("a", "1", NetWorkUtils.hostname(), 6700, exception2);
      this.validateErrors(
          state,
          "a",
          "1",
          Arrays.asList(CoreUtil.stringifyError(exception1),
              CoreUtil.stringifyError(exception2)));

      List<String> errorsList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        Exception exp = new RuntimeException();
        state.reportError("a", "2", NetWorkUtils.hostname(), 6700, exp);
        errorsList.add(CoreUtil.stringifyError(exp));
        Time.advanceTime(2000);
      }
      this.validateErrors(state, "a", "2", errorsList);

      for (int i = 0; i < 5; i++) {
        Exception exp = new IllegalArgumentException();
        state.reportError("a", "2", NetWorkUtils.hostname(), 6700, exp);
        errorsList.add(CoreUtil.stringifyError(exp));
        Time.advanceTime(2000);
      }
      errorsList = errorsList.subList(5, 15);
      this.validateErrors(state, "a", "2", errorsList);

      state.disconnect();

    } catch (Exception e) {
      Assert.assertNull("testStormClusterStateErrors failed", e);
    }

    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-supervisor-state")
  public void testSupervisorState() {
    try {
      StormZkClusterState state1 = new StormZkClusterState(stormConf);
      StormZkClusterState state2 = new StormZkClusterState(stormConf);

      Assert.assertTrue(state1.supervisors(null).isEmpty());

      SupervisorInfo supervisorInfo1 = new SupervisorInfo(10L,  "hostname-1");
      supervisorInfo1.set_assignment_id("id1");
      supervisorInfo1.set_used_ports( Arrays.asList(1L,2L));
      supervisorInfo1.set_uptime_secs(1000L);
      supervisorInfo1.set_version("0.9.2");
      
      SupervisorInfo supervisorInfo2 = new SupervisorInfo(10L,  "hostname-2");
      supervisorInfo2.set_assignment_id("id2");
      supervisorInfo2.set_used_ports( Arrays.asList(1L,2L));
      supervisorInfo2.set_uptime_secs(1000L);
      supervisorInfo2.set_version("0.9.2");
      
      state2.supervisorHeartbeat("2", supervisorInfo2);
      state1.supervisorHeartbeat("1", supervisorInfo1);

      Assert.assertEquals(supervisorInfo2, state1.supervisorInfo("2"));
      Assert.assertEquals(supervisorInfo1, state1.supervisorInfo("1"));

      Assert.assertEquals(new HashSet<String>(Arrays.asList("1", "2")),
          new HashSet<String>(state1.supervisors(null)));
      Assert.assertEquals(new HashSet<String>(Arrays.asList("1", "2")),
          new HashSet<String>(state2.supervisors(null)));

      state2.disconnect();
      Assert.assertEquals(new HashSet<String>(Arrays.asList("1")),
          new HashSet<String>(state1.supervisors(null)));

      state1.disconnect();

    } catch (Exception e) {
      Assert.assertNull("testSupervisorState failed", e);
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.cluster-test#test-storm-state-callbacks")
  public void testStormStateCallbacks() {
    // TODO finish
  }

  @Test
  public void testCommon() {
    Assert.assertEquals("/", Cluster.ZK_SEPERATOR);
    Assert.assertEquals("nimbus", Cluster.MASTER_ROOT);
    Assert.assertEquals("masterlock", Cluster.MASTER_LOCK_ROOT);
    Assert.assertEquals("assignments", Cluster.ASSIGNMENTS_ROOT);
    Assert.assertEquals("code", Cluster.CODE_ROOT);
    Assert.assertEquals("storms", Cluster.STORMS_ROOT);
    Assert.assertEquals("supervisors", Cluster.SUPERVISORS_ROOT);
    Assert.assertEquals("errors", Cluster.ERRORS_ROOT);
  }

  @Test
  public void testCommonSubtree() {
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.ASSIGNMENTS_ROOT,
        Cluster.ASSIGNMENTS_SUBTREE);
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.STORMS_ROOT,
        Cluster.STORMS_SUBTREE);
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.SUPERVISORS_ROOT,
        Cluster.SUPERVISORS_SUBTREE);
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.ERRORS_ROOT,
        Cluster.ERRORS_SUBTREE);
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.MASTER_ROOT,
        Cluster.MASTER_SUBTREE);
    Assert.assertEquals(Cluster.ZK_SEPERATOR + Cluster.MASTER_LOCK_ROOT,
        Cluster.MASTER_LOCK_SUBTREE);
  }

  @Test
  public void testCommonPaths() throws UnsupportedEncodingException {
    String id = "test";
    String node = "node";
    int port = 8888;
    int taskid = 1;
    String componentId = "c";
    Assert.assertEquals(
        Cluster.SUPERVISORS_SUBTREE + Cluster.ZK_SEPERATOR + id,
        Cluster.supervisorPath(id));
    Assert.assertEquals(
        Cluster.ASSIGNMENTS_SUBTREE + Cluster.ZK_SEPERATOR + id,
        Cluster.assignmentPath(id));
    Assert.assertEquals(Cluster.STORMS_SUBTREE + Cluster.ZK_SEPERATOR + id,
        Cluster.stormPath(id));
    Assert.assertEquals(Cluster.ERRORS_SUBTREE + Cluster.ZK_SEPERATOR + id,
        Cluster.errorStormRoot(id));
    Assert.assertEquals(Cluster.errorStormRoot(id) + Cluster.ZK_SEPERATOR
        + componentId, Cluster.errorPath(id, componentId));
  }


  @AfterClass
  public static void cleanUp() {
    if (zookeeper != null) {
      zookeeper.stop();
    }
  }
}
