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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.cluster.ClusterState;
import org.apache.storm.cluster.DistributedClusterState;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.nimbus.NimbusData;
import org.apache.storm.daemon.nimbus.ServiceHandler;
import org.apache.storm.daemon.nimbus.StandaloneNimbus;
import org.apache.storm.daemon.nimbus.threads.CleanInboxRunnable;
import org.apache.storm.daemon.nimbus.threads.MonitorRunnable;
import org.apache.storm.daemon.supervisor.SupervisorManager;
import org.apache.storm.daemon.supervisor.psim.ProcessSimulator;
import org.apache.storm.daemon.worker.messaging.local.LocalContext;
import org.apache.storm.testing.Testing;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zk.InprocessZookeeper;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Credentials;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.INimbus;
import backtype.storm.security.auth.ThriftServer;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.LocalCluster")
public class LocalCluster implements ILocalCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
  private ServiceHandler serviceHandler;
  private NimbusData nimbusData;
  private Vector<String> tmpDirs = new Vector<String>();
  private ThriftServer thriftServer;
  private InprocessZookeeper zkServer;
  private IContext context;
  private ClusterState clusterState = null;
  private StormClusterState stormClusterState;
  private Vector<SupervisorManager> supervisorDaemons =
      new Vector<SupervisorManager>();
  private Map state;

  private int supervisors = 2;
  private int portsPerSupervisor = 3;
  private Map daemonConf = new HashMap();
  private INimbus inimbus = null;
  private int supervisorSlotPortMin = 1024;

  public LocalCluster() {
    this.daemonConf = new HashMap();
    this.daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS_SCHEMA, true);

    try {
      state = this.init();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.testing#mk-local-storm-cluster")
  public LocalCluster(int supervisors, int portsPerSupervisor, Map daemonConf,
      INimbus inimbus, int supervisorSlotPortMin) {
    this.supervisors = supervisors;
    this.portsPerSupervisor = portsPerSupervisor;
    this.daemonConf = daemonConf;
    this.inimbus = inimbus;
    this.supervisorSlotPortMin = supervisorSlotPortMin;

    try {
      state = this.init();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.LocalCluster#-init")
  protected Map init() throws Exception {
    Map<String, Object> zkConf = ConfigUtil.readDefaultConfig();
    String zkTmp = CoreUtil.localTempPath();
    zkConf.put(Config.STORM_LOCAL_DIR, zkTmp);
    this.zkServer = new InprocessZookeeper(zkConf);
    this.zkServer.start();
    this.tmpDirs.add(zkTmp);
    Thread.sleep(3000);

    Map<Object, Object> conf = Utils.readStormConfig();
    conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
    conf.put(Config.ZMQ_LINGER_MILLIS, 0);
    conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
    conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
    conf.put(Config.STORM_CLUSTER_MODE, "local");
    conf.put(Config.STORM_ZOOKEEPER_PORT, zkServer.port());
    conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
    conf.putAll(this.daemonConf);
    this.daemonConf = conf;

    INimbus iNimbus = new StandaloneNimbus();
    this.launchLocalServer(iNimbus);

    this.context = mkSharedContext(this.daemonConf);
    this.clusterState = new DistributedClusterState(daemonConf);
    this.stormClusterState = new StormZkClusterState(daemonConf);

    for (int i = 0; i < this.supervisors; i++) {
      Testing.addSupervisor(this, this.portsPerSupervisor, new HashMap(), null);
    }

    state = new HashMap<String, Object>();
    state.put("nimbus", this.serviceHandler);
    state.put("port-counter", this.supervisorSlotPortMin);
    state.put("daemon-conf", this.daemonConf);
    state.put("supervisors", this.supervisorDaemons);
    state.put("cluster-state", this.clusterState);
    state.put("storm-cluster-state", this.stormClusterState);
    state.put("tmp-dirs", this.tmpDirs);
    state.put("zookeeper", this.zkServer);
    state.put("shared-context", this.context);
    return state;
  }

  @Override
  public void submitTopology(String topologyName, Map conf,
      StormTopology topology) {
    SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
    submitTopologyWithOpts(topologyName, conf, topology, options);
  }

  @Override
  public void submitTopologyWithOpts(String topologyName, Map conf,
      StormTopology topology, SubmitOptions submitOpts) {
    try {
      if (!Utils.isValidConf(conf))
        throw new IllegalArgumentException(
            "Topology conf is not json-serializable");

      serviceHandler.submitTopologyWithOpts(topologyName, null,
          CoreUtil.to_json(conf), topology, submitOpts);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void killTopology(String topologyName) {
    killTopologyWithOpts(topologyName, new KillOptions());
  }

  @Override
  public void killTopologyWithOpts(String name, KillOptions options) {
    try {
      serviceHandler.killTopologyWithOpts(name, options);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void activate(String topologyName) throws NotAliveException {
    try {
      serviceHandler.activate(topologyName);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void deactivate(String topologyName) throws NotAliveException {
    try {
      serviceHandler.deactivate(topologyName);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void rebalance(String name, RebalanceOptions options)
      throws NotAliveException {
    try {
      serviceHandler.rebalance(name, options);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void shutdown() {
    if (state == null) {
      return;
    }
    if (serviceHandler != null) {
      serviceHandler.shutdown();
    }
    if (nimbusData != null) {
      nimbusData.cleanup();
    }
    if (this.clusterState != null) {
      this.clusterState.close();
    }
    if (this.stormClusterState != null) {
      try {
        this.stormClusterState.disconnect();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    Vector<SupervisorManager> supervisorDaemons =
        (Vector<SupervisorManager>) state.get("supervisors");
    for (SupervisorManager daemon : supervisorDaemons) {
      daemon.shutdownAllWorkers();
      daemon.shutdown();
    }

    ProcessSimulator.killAllProcesses();

    if (zkServer != null) {
      LOG.info("Shutting down in process zookeeper");
      zkServer.stop();
      LOG.info("Done shutting down in process zookeeper");
    }

    // delete all tmp path
    for (String dir : tmpDirs) {
      try {
        LOG.info("Deleting temporary path {}", dir);
        CoreUtil.rmr(dir);
      } catch (Exception e) {
        LOG.warn("Failed to clean up LocalCluster tmp dirs for {}",
            CoreUtil.stringifyError(e));
      }
    }

    this.state = null;
    // Runtime.getRuntime().exit(0);
  }

  @Override
  public String getTopologyConf(String id) {
    String ret = null;
    try {
      ret = this.serviceHandler.getTopologyConf(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public StormTopology getTopology(String id) {
    StormTopology ret = null;
    try {
      ret = this.serviceHandler.getTopology(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public ClusterSummary getClusterInfo() {
    ClusterSummary ret = null;
    try {
      ret = this.serviceHandler.getClusterInfo();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public TopologyInfo getTopologyInfo(String id) {
    TopologyInfo ret = null;
    try {
      ret = this.serviceHandler.getTopologyInfo(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public Map getState() {
    return state;
  }

  @SuppressWarnings("unchecked")
  public void launchLocalServer(INimbus inimbus) throws Exception {
    initLocalShutdownHook();

    String nimbusTmp = CoreUtil.localTempPath();
    Map<String, Object> nimbusConf = new HashMap<String, Object>();
    nimbusConf.putAll(this.daemonConf);
    nimbusConf.put(Config.STORM_LOCAL_DIR, nimbusTmp);
    this.tmpDirs.add(nimbusTmp);

    LOG.info("Begin to start nimbus with conf {} ", nimbusConf.toString());

    this.nimbusData = new LocalNimbusData(nimbusConf, inimbus);
    this.serviceHandler = new ServiceHandler(nimbusConf, inimbus);
    scheduleLocalNimbusMonitor(nimbusConf);
    scheduleLocalNimbusInboxCleaner(nimbusConf);
  }

  private void scheduleLocalNimbusMonitor(Map conf) throws Exception {
    // Schedule Nimbus monitor
    int monitorFreqSecs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
    final ScheduledExecutorService scheduExec = nimbusData.getScheduExec();
    MonitorRunnable r1 = new MonitorRunnable(nimbusData);
    scheduExec.scheduleAtFixedRate(r1, 0, monitorFreqSecs, TimeUnit.SECONDS);
    LOG.info("Successfully init Monitor thread");
  }

  /**
   * Right now, every 600 seconds, nimbus will clean jar under
   * /LOCAL-DIR/nimbus/inbox, which is the uploading topology directory
   * 
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  private void scheduleLocalNimbusInboxCleaner(Map conf) throws IOException {
    final ScheduledExecutorService scheduExec = nimbusData.getScheduExec();
    // Schedule Nimbus inbox cleaner
    String dir_location = ConfigUtil.masterInbox(conf);
    int inbox_jar_expiration_secs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS),
            3600);
    CleanInboxRunnable clean_inbox =
        new CleanInboxRunnable(dir_location, inbox_jar_expiration_secs);
    int cleanup_inbox_freq_secs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);
    scheduExec.scheduleAtFixedRate(clean_inbox, 0, cleanup_inbox_freq_secs,
        TimeUnit.SECONDS);
    LOG.info("Successfully init " + dir_location + " cleaner");
  }

  private void initLocalShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LocalCluster.this.shutdown();
      }
    });
  }

  @ClojureClass(className = "backtype.storm.testing#mk-shared-context")
  public static IContext mkSharedContext(Map conf) {
    Boolean isLocalModeZMQ =
        CoreUtil.parseBoolean(conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
    LocalContext context = null;
    if (!isLocalModeZMQ) {
      context = new LocalContext(null, null);
      context.prepare(conf);
      return context;
    }
    return null;
  }

  public Vector<String> getTmpDirs() {
    return this.tmpDirs;
  }

  public void addTmpDir(String dir) {
    this.tmpDirs.add(dir);
  }

  public Vector<SupervisorManager> getSupervisorDaemons() {
    return this.supervisorDaemons;
  }

  public void setSupervisorsDaemons(Vector<SupervisorManager> daemons) {
    this.supervisorDaemons = daemons;
  }

  public IContext getSharedContext() {
    return this.context;
  }

  public void setSharedContext(IContext sharedContext) {
    this.context = sharedContext;
  }

  public Map getDaemonConf() {
    return this.daemonConf;
  }

  public void setDaemonConf(Map conf) {
    this.daemonConf = conf;
  }

  public int getPortCounter() {
    return this.supervisorSlotPortMin;
  }

  public void setPortCounter(int counter) {
    this.supervisorSlotPortMin = counter;
  }

  public StormClusterState getStormClusterState() {
    return this.stormClusterState;
  }

  public void setStormClusterState(StormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public ClusterState getClusterState() {
    return this.clusterState;
  }

  public void setClusterState(ClusterState clusterState) {
    this.clusterState = clusterState;
  }

  public ServiceHandler getNimbus() {
    return this.serviceHandler;
  }

  public void setNimbus(ServiceHandler nimbus) {
    this.serviceHandler = nimbus;
  }

  public InprocessZookeeper getZookeeper() {
    return this.zkServer;
  }

  public void setZookeeper(InprocessZookeeper zk) {
    this.zkServer = zk;
  }

  @Override
  public void uploadNewCredentials(String topologyName, Credentials creds) {
    // TODO Auto-generated method stub

  }

}
