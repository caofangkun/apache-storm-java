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
package org.apache.storm.testing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.LocalCluster;
import org.apache.storm.cluster.ClusterState;
import org.apache.storm.cluster.DistributedClusterState;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.common.DaemonCommon;
import org.apache.storm.daemon.nimbus.ServiceHandler;
import org.apache.storm.daemon.nimbus.StandaloneNimbus;
import org.apache.storm.daemon.supervisor.StandaloneSupervisor;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.daemon.supervisor.SupervisorManager;
import org.apache.storm.daemon.supervisor.psim.ProcessSimulator;
import org.apache.storm.daemon.worker.messaging.local.LocalContext;
import org.apache.storm.daemon.worker.messaging.local.LocalUtils;
import org.apache.storm.thrift.Thrift;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zk.InprocessZookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.FixedTuple;
import backtype.storm.testing.FixedTupleSpout;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.testing.TupleCaptureBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.testing")
public class Testing {
  private static final Logger LOG = LoggerFactory.getLogger(Testing.class);

  @ClojureClass(className = "backtype.storm.testing#feeder-spout")
  public static FeederSpout feederSpout(Fields fields) {
    return new FeederSpout(fields);
  }

  @ClojureClass(className = "backtype.storm.testing#local-temp-path")
  public static String localTempPath() {
    return System.getProperty("java.io.tmpdir") + "/" + CoreUtil.uuid();
  }

  @ClojureClass(className = "backtype.storm.testing#delete-all")
  public static void deleteAll(List<String> paths) throws IOException {
    for (String path : paths) {
      File file = new File(path);
      if (file.exists()) {
        FileUtils.forceDelete(file);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.testing#start-simulating-time!")
  public static void startSimulatingTime() {
    Time.startSimulating();
  }

  @ClojureClass(className = "backtype.storm.testing#start-simulating-time!")
  public static void stopSimulatingTime() {
    Time.stopSimulating();
  }

  @ClojureClass(className = "backtype.storm.testing#advance-time-ms!")
  public static void advanceTimeMs(long ms) {
    Time.advanceTime(ms);
  }

  @ClojureClass(className = "backtype.storm.testing#advance-time-secs!")
  public static void advanceTimeSecs(int secs) {
    advanceTimeMs(secs * 1000L);
  }

  @ClojureClass(className = "backtype.storm.testing#add-supervisor")
  public static void addSupervisor(Map clusterMap, int ports, Map conf,
      String id) throws Exception {
    String tmpDir = CoreUtil.localTempPath();
    List<Integer> portIds = new ArrayList<Integer>();
    for (int i = 0; i < ports; i++) {
      portIds.add(6700 + i);
    }

    Map<String, Object> supervisorConf = new HashMap<String, Object>();
    supervisorConf.putAll((Map<String, Object>) clusterMap.get("daemon-conf"));
    supervisorConf.putAll(conf);
    supervisorConf.put(Config.STORM_LOCAL_DIR, tmpDir);
    supervisorConf.put(Config.SUPERVISOR_SLOTS_PORTS, portIds);

    ISupervisor iSupervisor = new StandaloneSupervisor();
    Supervisor supervisor = new Supervisor();
    LocalContext sharedContext =
        (LocalContext) clusterMap.get("shared-context");
    SupervisorManager daemon =
        supervisor.mkSupervisor(supervisorConf, sharedContext, iSupervisor);

    Vector<SupervisorManager> supervisorDaemons =
        (Vector<SupervisorManager>) clusterMap.get("supervisors");

    supervisorDaemons.add(daemon);

    // TODO

  }

  @ClojureClass(className = "backtype.storm.testing#add-supervisor")
  public static void addSupervisor(LocalCluster cluster, int ports, Map conf,
      String id) throws Exception {
    String supTmp = CoreUtil.localTempPath();
    Map<String, Object> supConf = new HashMap<String, Object>();
    supConf.putAll(cluster.getDaemonConf());
    supConf.putAll(conf);
    supConf.put(Config.STORM_LOCAL_DIR, supTmp);

    List<Integer> portIds = new ArrayList<Integer>();
    int portCounter = CoreUtil.parseInt(cluster.getPortCounter(), 1024);
    for (int j = 0; j < ports; j++) {
      portIds.add(portCounter);
      portCounter++;
    }
    cluster.setPortCounter(portCounter);
    supConf.put(Config.SUPERVISOR_SLOTS_PORTS, portIds);

    ISupervisor iSupervisor = new StandaloneSupervisor();
    Supervisor supervisor = new Supervisor();
    IContext sharedContext = cluster.getSharedContext();
    SupervisorManager daemon =
        supervisor.mkSupervisor(supConf, sharedContext, iSupervisor);

    Vector<SupervisorManager> supervisorDaemons =
        cluster.getSupervisorDaemons();
    supervisorDaemons.add(daemon);
    cluster.addTmpDir(supTmp);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.testing#mk-shared-context")
  public static LocalContext mkSharedContext(Map conf) {
    LocalContext context = null;
    if (!CoreUtil.parseBoolean(conf.get(Config.STORM_LOCAL_MODE_ZMQ), false)) {
      context = LocalUtils.mkContext();
    }
    return context;
  }

  /**
   * 
   * @param supervisors
   * @param portsPerSupervisor
   * @param daemonConf
   * @param inimbus
   * @param supervisorSlotPortMin
   * @return returns map containing cluster info
   * @throws Exception
   */
  @ClojureClass(className = "backtype.storm.testing#mk-local-storm-cluster")
  public static Map<String, Object> mkLocalStormCluster(int supervisors,
      int portsPerSupervisor, Map daemonConf, INimbus inimbus,
      int supervisorSlotPortMin) throws Exception {
    Vector<String> tmpDirs = new Vector<String>();
    Map<String, Object> conf = Utils.readStormConfig();

    String zkTmp = localTempPath();
    List<String> stormZookeeperServers =
        CoreUtil
            .parseList(daemonConf.get(Config.STORM_ZOOKEEPER_SERVERS), null);
    InprocessZookeeper zkServer = null;
    if (stormZookeeperServers == null) {
      Map<String, Object> zkConf = ConfigUtil.readDefaultConfig();
      zkServer = new InprocessZookeeper(zkConf);
      zkServer.start();
      tmpDirs.add(zkTmp);
      conf.put(Config.STORM_ZOOKEEPER_PORT, zkServer.port());
      conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
    }

    conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
    conf.put(Config.ZMQ_LINGER_MILLIS, 0);
    conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
    conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
    conf.put(Config.STORM_CLUSTER_MODE, "local");

    conf.putAll(daemonConf);
    daemonConf = conf;

    String nimbusTmp = localTempPath();

    Map<String, Object> nimbusConf = new HashMap<String, Object>();
    nimbusConf.putAll(daemonConf);
    nimbusConf.put(Config.STORM_LOCAL_DIR, nimbusTmp);
    tmpDirs.add(nimbusTmp);

    if (inimbus == null) {
      inimbus = new StandaloneNimbus();
    }
    ServiceHandler nimbus = new ServiceHandler(nimbusConf, inimbus);

    LocalContext context = mkSharedContext(daemonConf);

    Map<String, Object> clusterMap = new HashMap<String, Object>();
    clusterMap.put("nimbus", nimbus);
    clusterMap.put("daemon-conf", daemonConf);
    clusterMap.put("supervisors", new Vector<SupervisorManager>());
    clusterMap.put("state", new DistributedClusterState(daemonConf));
    clusterMap.put("storm-cluster-state", new StormZkClusterState(daemonConf));
    clusterMap.put("tmp-dirs", tmpDirs);
    if (zkServer != null) {
      clusterMap.put("zookeeper", zkServer);
    }
    clusterMap.put("shared-context", context);

    return clusterMap;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.testing#get-supervisor")
  public static SupervisorManager getSupervisor(Map clusterMap,
      String supervisorId) {
    return (SupervisorManager) clusterMap.get(supervisorId);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.testing#kill-supervisor")
  public static void killSupervisor(Map clusterMap, String supervisorId) {
    SupervisorManager sup = (SupervisorManager) clusterMap.get(supervisorId);
    // tmp-dir will be taken care of by shutdown
    clusterMap.remove(supervisorId);
    sup.shutdown();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.testing#kill-local-storm-cluster")
  public static void killLocalStormCluster(Map clusterMap) throws Exception {
    ServiceHandler nimbus = (ServiceHandler) clusterMap.get("nimbus");
    nimbus.shutdown();

    ClusterState state = (ClusterState) clusterMap.get("state");
    state.close();

    StormClusterState stormClusterState =
        (StormClusterState) clusterMap.get("storm-cluster-state");
    stormClusterState.disconnect();

    Vector<SupervisorManager> supervisors =
        (Vector<SupervisorManager>) clusterMap.get("supervisors");
    for (SupervisorManager supervisor : supervisors) {
      supervisor.shutdownAllWorkers();
      // race condition here? will it launch the workers again?
      supervisor.shutdown();
    }
    ProcessSimulator.killAllProcesses();

    InprocessZookeeper zookeeper =
        (InprocessZookeeper) clusterMap.get("zookeeper");
    if (zookeeper != null) {
      LOG.info("Shutting down in process zookeeper");
      zookeeper.stop();
    }
    LOG.info("Done shutting down in process zookeeper");

    Vector<String> tmpDirs = (Vector<String>) clusterMap.get("tmp-dirs");
    for (String dir : tmpDirs) {
      try {
        LOG.info("Deleting temporary path {}", dir);
        CoreUtil.rmr(dir);
        // on windows, the host process still holds lock on the logfile
      } catch (Exception e) {
        LOG.warn("Failed to clean up LocalCluster tmp dirs for {}",
            CoreUtil.stringifyError(e));
      }
    }
  }

  public final static long TEST_TIMEOUT_MS = 20000; // JStorm needs more time to

  @ClojureClass(className = "backtype.storm.testing#TEST-TIMEOUT-MS")
  public static int testTimeOutMs() {
    Integer timeOut = Integer.valueOf(System.getenv("STORM_TEST_TIMEOUT_MS"));
    return timeOut != null ? timeOut : 5000;
  }

  public static TupleImpl testTuple(List<Object> values, String stream,
      String component, List<String> fields) {
    StreamInfo streamInfo = new StreamInfo(fields, true);
    Map<String, StreamInfo> outputs = new HashMap<String, StreamInfo>();
    outputs.put(stream, streamInfo);
    SpoutSpec spoutSpec =
        Thrift.mkSpoutSpec(new TestWordSpout(), outputs, null, null);
    Map<String, SpoutSpec> spouts = null;
    StormTopology topology = new StormTopology(spouts, null, null);
    Map<Integer, String> taskToComponent = new HashMap<Integer, String>();
    taskToComponent.put(1, component);
    Map<String, List<Integer>> componentToSortedTasks =
        new HashMap<String, List<Integer>>();
    List<Integer> tmp = new ArrayList<Integer>();
    tmp.add(1);
    componentToSortedTasks.put(component, tmp);
    Map<String, Map<String, Fields>> componentToStreamToFields =
        new HashMap<String, Map<String, Fields>>();
    Map<String, Fields> tmp1 = new HashMap<String, Fields>();
    tmp1.put(stream, new Fields(fields));
    componentToStreamToFields.put(component, tmp1);
    List<Integer> workerTasks = new ArrayList<Integer>();
    workerTasks.add(1);
    TopologyContext context =
        new TopologyContext(topology, Utils.readStormConfig(), taskToComponent,
            componentToSortedTasks, componentToStreamToFields, "test-storm-id",
            null, null, 1, null, workerTasks, null, null, new HashMap(),
            new HashMap(), new clojure.lang.Atom(false));
    return new TupleImpl(context, values, 1, null);
  }

  @ClojureClass(className = "backtype.storm.testing#capture-topology")
  public static Map<String, Object> captureTopology(StormTopology topology) {
    topology = topology.deepCopy();
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    Map<String, Bolt> bolts = topology.get_bolts();

    Map<GlobalStreamId, Boolean> allStreams =
        new HashMap<GlobalStreamId, Boolean>();
    for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
      String id = entry.getKey();
      SpoutSpec spec = entry.getValue();
      Map<String, StreamInfo> streams = spec.get_common().get_streams();
      for (Map.Entry<String, StreamInfo> e : streams.entrySet()) {
        String stream = e.getKey();
        StreamInfo info = e.getValue();
        allStreams.put(new GlobalStreamId(id, stream), info.is_direct());
      }
    }
    for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
      String id = entry.getKey();
      Bolt spec = entry.getValue();
      Map<String, StreamInfo> streams = spec.get_common().get_streams();
      for (Map.Entry<String, StreamInfo> e : streams.entrySet()) {
        String stream = e.getKey();
        StreamInfo info = e.getValue();
        allStreams.put(new GlobalStreamId(id, stream), info.is_direct());
      }
    }

    TupleCaptureBolt capturer = new TupleCaptureBolt();

    Map<GlobalStreamId, Grouping> inputs =
        new HashMap<GlobalStreamId, Grouping>();
    for (Map.Entry<GlobalStreamId, Boolean> entry : allStreams.entrySet()) {
      if (entry.getValue()) {
        inputs.put(entry.getKey(), Thrift.mkDirectGrouping());
      } else {
        inputs.put(entry.getKey(), Thrift.mkGlobalGrouping());
      }
    }
    bolts.put(
        UUID.randomUUID().toString(),
        new Bolt(Thrift.serializeComponentObject(capturer), Thrift
            .mkPlainComponentCommon(inputs, new HashMap<String, StreamInfo>(),
                null, null)));
    topology.set_bolts(bolts);

    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Testing.TOPOLOGY, topology);
    ret.put(Testing.CAPTURER, capturer);
    return ret;
  }

  // TODO: mock-sources needs to be able to mock out state spouts as well
  @ClojureClass(className = "backtype.storm.testing#complete-topology")
  public static Map<String, List<FixedTuple>> completeTopology(
      LocalCluster cluster, StormTopology topology,
      Map<String, List<Object>> mockSources, Map stormConf,
      Boolean cleanupState, String topologyName, Long timeoutMs)
      throws Exception {

    // TODO: the idea of mocking for transactional topologies should be done an
    // abstraction level above... should have a complete-transactional-topology
    // for this
    if (cleanupState == null) {
      cleanupState = true;
    }
    if (timeoutMs == null) {
      timeoutMs = Testing.TEST_TIMEOUT_MS;
    }
    Map<String, Object> res = captureTopology(topology);
    topology = (StormTopology) res.get(Testing.TOPOLOGY);
    TupleCaptureBolt capturer = (TupleCaptureBolt) res.get(Testing.CAPTURER);
    String stormName;
    if (topologyName != null) {
      stormName = topologyName;
    } else {
      stormName = "topologytest-" + UUID.randomUUID();
    }

    StormClusterState state = cluster.getStormClusterState();
    Map<String, SpoutSpec> spouts = topology.get_spouts();

    Map<String, FixedTupleSpout> replacements =
        new HashMap<String, FixedTupleSpout>();
    for (Map.Entry<String, List<Object>> entry : mockSources.entrySet()) {
      String id = entry.getKey();
      List<Object> value = entry.getValue();
      List<Object> tuples = new ArrayList<Object>(value.size());
      for (Object tup : value) {
        if (tup instanceof Map) {
          Map tupMap = (Map) tup;
          tuples.add(new FixedTuple((String) tupMap.get("stream"),
              (List) tupMap.get("values")));
        } else {
          tuples.add(tup);
        }
      }
      replacements.put(id, new FixedTupleSpout(tuples));
    }

    for (Map.Entry<String, FixedTupleSpout> entry : replacements.entrySet()) {
      String id = entry.getKey();
      FixedTupleSpout spout = entry.getValue();
      SpoutSpec spoutSpec = spouts.get(id);
      spoutSpec.set_spout_object(Thrift.serializeComponentObject(spout));
    }

    List<Object> spoutObjects = new ArrayList<Object>();
    for (SpoutSpec spoutSpec : spouts.values()) {
      spoutObjects.add(Thrift.deserializedComponentObject(spoutSpec
          .get_spout_object()));
    }
    for (Object spout : spoutObjects) {
      if (!(spout instanceof CompletableSpout)) {
        throw new RuntimeException(
            "Cannot complete topology unless every spout is a CompletableSpout (or mocked to be)");
      }
    }

    for (Object spout : spoutObjects) {
      ((CompletableSpout) spout).startup();
    }

    cluster.submitTopology(stormName, stormConf, topology);

    String stormId = Common.getStormId(state, stormName);

    /*
     * long endTime = System.currentTimeMillis() + timeoutMs; while
     * (!isEveryExhausted(spoutObjects)) { if (System.currentTimeMillis() >
     * endTime) { throw new AssertionError("Test timed out (" + timeoutMs +
     * "ms)"); } simulateWait(cluster); }
     */
    Thread.sleep(20000);

    KillOptions killOptions = new KillOptions();
    killOptions.set_wait_secs(0);
    cluster.getNimbus().killTopologyWithOpts(stormName, killOptions);

    /*
     * endTime = System.currentTimeMillis() + timeoutMs; while
     * (state.assignment_info(stormId, null) != null) { if
     * (System.currentTimeMillis() > endTime) { throw new
     * AssertionError("Test timed out (" + timeoutMs + "ms)"); }
     * simulateWait(cluster); }
     */
    Thread.sleep(10000);

    if (cleanupState) {
      for (Object spout : spoutObjects) {
        ((CompletableSpout) spout).cleanup();
      }
      return capturer.getAndRemoveResults();
    } else {
      return capturer.getAndClearResults();
    }
  }

  @ClojureClass(className = "backtype.storm.testing#simulate-wait")
  public static void simulateWait(LocalCluster cluster)
      throws InterruptedException {
    if (Time.isSimulating()) {
      advanceClusterTime(cluster, 10);
      Thread.sleep(100);
    }
  }

//  private static boolean isEveryExhausted(List<Object> spoutObjects) {
//    for (Object spout : spoutObjects) {
//      CompletableSpout realSpout = (CompletableSpout) spout;
//      if (!(realSpout.isExhausted())) {
//        return false;
//      }
//    }
//    return true;
//  }

  @ClojureClass(className = "backtype.storm.testing#advance-cluster-time")
  public static void advanceClusterTime(LocalCluster cluster, long secs,
      long incrementSecs) throws InterruptedException {
    long left = secs;
    while (left > 0) {
      long diff = Math.min(left, incrementSecs);
      Time.advanceTime(diff);
      waitUntilClusterWaiting(cluster);
      left -= diff;
    }
  }

  @ClojureClass(className = "backtype.storm.testing#advance-cluster-time")
  public static void advanceClusterTime(LocalCluster cluster, long secs)
      throws InterruptedException {
    advanceClusterTime(cluster, secs, 1);
  }

  /**
   * "Wait until the cluster is idle. Should be used with time simulation."
   * 
   * @throws InterruptedException
   */
  @ClojureClass(className = "backtype.storm.testing#wait-until-cluster-waiting")
  public static void waitUntilClusterWaiting(LocalCluster cluster)
      throws InterruptedException {
    waitUntilClusterWaiting(cluster, Testing.TEST_TIMEOUT_MS);
  }

  @ClojureClass(className = "backtype.storm.testing#wait-until-cluster-waiting")
  public static void waitUntilClusterWaiting(LocalCluster cluster,
      long timeoutMs) throws InterruptedException {

    // wait until all workers, supervisors, and nimbus is waiting
    List<DaemonCommon> daemons = new ArrayList<DaemonCommon>();
    daemons.add(cluster.getNimbus());
    daemons.addAll(cluster.getSupervisorDaemons());

    // because a worker may already be dead
    Collection<Shutdownable> allProcesses =
        ProcessSimulator.getAllProcessHandles();
    for (Shutdownable process : allProcesses) {
      if (process instanceof DaemonCommon) {
        daemons.add((DaemonCommon) process);
      }
    }

    long endTime = System.currentTimeMillis() + timeoutMs;
    while (!isEveryWaiting(daemons)) {
      if (System.currentTimeMillis() > endTime) {
        throw new AssertionError("Test timed out (" + timeoutMs + "ms)");
      }
      if (Time.isSimulating()) {
        Thread.sleep(10);
        // for (DaemonCommon d : daemons) {
        // if (!(d.waiting()))
        // System.out.println(d);
        // }
      }
    }
  }

  private static boolean isEveryWaiting(List<DaemonCommon> daemons) {
    for (DaemonCommon daemon : daemons) {
      if (!daemon.waiting())
        return false;
    }
    return true;
  }

  @ClojureClass(className = "backtype.storm.testing#read-tuples")
  public static List<Object> readTuples(Map<String, List<FixedTuple>> results,
      String componentId, String streamId) {

    List<Object> ret = new ArrayList<Object>();
    List<FixedTuple> fixedTuples = results.get(componentId);
    if (fixedTuples != null) {
      for (FixedTuple ft : fixedTuples) {
        if (ft.stream.equals(streamId)) {
          ret.add(new ArrayList<Object>(ft.values));
        }
      }
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.testing#read-tuples")
  public static List<Object> readTuples(Map<String, List<FixedTuple>> results,
      String componentId) {

    return readTuples(results, componentId, Utils.DEFAULT_STREAM_ID);
  }

  @ClojureClass(className = "backtype.storm.testing#ms=")
  public static boolean multiSetEquals(List<Object>... args) {
    int len = args.length;
    HashMap<Object, Integer> first = CoreUtil.multiSet(args[0]);
    for (int i = 1; i < len; i++) {
      HashMap<Object, Integer> other = CoreUtil.multiSet(args[i]);
      if (!first.equals(other)) {
        return false;
      }
    }
    return true;
  }

  public static void cleanupLocalCluster(LocalCluster cluster) {
    if (null != cluster) {
      cluster.shutdown();
    }
  }

  final static String TOPOLOGY = "topology";
  final static String CAPTURER = "capturer";
  final static String STORM_CLUSTER_STATE = "storm-cluster-state";
  // final static long TEST_TIMEOUT_MS = 5000;
  // start-up
}
