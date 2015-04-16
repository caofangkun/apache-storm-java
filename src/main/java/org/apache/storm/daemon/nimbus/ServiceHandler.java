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
package org.apache.storm.daemon.nimbus;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.common.SupervisorInfo;
import org.apache.storm.daemon.worker.executor.ExecutorCache;
import org.apache.storm.http.auth.Credentials;
import org.apache.storm.util.CoreUtil;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.daemon.common.DaemonCommon;
import backtype.storm.daemon.common.StormBase;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.BufferFileInputStream;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;

@ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler")
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
  private final static Logger LOG = LoggerFactory
      .getLogger(ServiceHandler.class);
  public final static int THREAD_NUM = 64;
  private NimbusData nimbus;
  @SuppressWarnings("rawtypes")
  private Map conf;

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler")
  public ServiceHandler(Map conf, INimbus inimbus) throws Exception {
    this.conf = conf;
    inimbus.prepare(conf, ConfigUtil.masterInimbusDir(conf));
    LOG.info("Starting Nimbus with conf {} ", conf.toString());
    this.nimbus = new NimbusData(conf, inimbus);
    registerLeaderHost(conf);
    nimbus.getValidator().prepare(conf);
    // clean up old topologies
    NimbusUtils.cleanupCorruptTopologies(nimbus);
    initTopologyStatus();
    scheduleNimbusMonitor(conf);
    scheduleNimbusInboxCleaner(conf);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Map mkStormConf(String jsonConf, StormTopology topology,
      String stormId, String stormName) throws InvalidTopologyException {
    Map serializedConf = (Map) CoreUtil.from_json(jsonConf);
    if (serializedConf == null) {
      serializedConf = new HashMap();
    }
    serializedConf.put(Config.STORM_ID, stormId);
    serializedConf.put(Config.TOPOLOGY_NAME, stormName);
    Map result = null;
    try {
      result = NimbusUtils.normalizeConf(conf, serializedConf, topology);
    } catch (Exception e) {
      throw new InvalidTopologyException(CoreUtil.stringifyError(e));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private String mkStormId(String stormName, String uploadedJarLocation) {
    int counter = nimbus.getSubmittedCount().incrementAndGet();
    if (ConfigUtil.isLocalMode(conf)) {
      stormName = "Local-" + stormName;
      uploadedJarLocation = "--local--";
    }
    String stormId =
        stormName + "-" + counter + "-" + CoreUtil.current_time_secs();
    return stormId;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#submitTopologyWithOpts")
  public void submitTopologyWithOpts(String stormName,
      String uploadedJarLocation, String serializedConf,
      StormTopology topology, SubmitOptions submitOptions)
      throws AlreadyAliveException, InvalidTopologyException {
    LOG.info("Received topology submission for " + stormName
        + ", uploadedJarLocation:" + uploadedJarLocation + " with conf "
        + serializedConf);

    try {
      assert submitOptions != null;

      NimbusUtils.ValidTopologyName(stormName);
      // NimbusUtils.checkAuthorization(nimbus, stormName, null,
      // "submitTopology");
      NimbusUtils.checkStormActive(nimbus, stormName, false);
      Map topoConf = (Map) CoreUtil.from_json(serializedConf);
      nimbus.getValidator().validate(stormName, topoConf, topology);

      String stormId = mkStormId(stormName, uploadedJarLocation);
      Map stormConf = mkStormConf(serializedConf, topology, stormId, stormName);
      Map totalStormConf = new HashMap(conf);
      totalStormConf.putAll(stormConf);

      StormTopology normalizedTopology =
          NimbusUtils.normalizeTopology(stormConf, topology);
      normalizedTopology = NimbusUtils.optimizeTopology(normalizedTopology);
      // this validates the structure of the topology
      Common.systemTopology(totalStormConf, normalizedTopology);
      StormClusterState stormClusterState = nimbus.getStormClusterState();
      LOG.info("Received topology submission for " + stormId + " with conf "
          + stormConf);

      synchronized (nimbus.getSubmitLock()) {
        NimbusUtils.setupStormCode(conf, stormId, uploadedJarLocation,
            stormConf, normalizedTopology);
        stormClusterState.setupHeartbeats(stormId);
        TopologyStatus status =
            NimbusUtils.thriftStatusToKwStatus(submitOptions
                .get_initial_status());
        NimbusUtils.startStorm(nimbus, stormName, stormId, status);
        NimbusUtils.mkAssignments(nimbus, null);
      }
    } catch (Throwable e) {
      LOG.error("Topology submission exception. (topology name='{}') for {}",
          stormName, CoreUtil.stringifyError(e));
      throw new InvalidTopologyException(CoreUtil.stringifyError(e));
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#submitTopology")
  public void submitTopology(String stormName, String uploadedJarLocation,
      String serializedConf, StormTopology topology)
      throws AlreadyAliveException, InvalidTopologyException, TException {
    SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
    this.submitTopologyWithOpts(stormName, uploadedJarLocation, serializedConf,
        topology, options);
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#killTopology")
  public void killTopology(String name) throws NotAliveException, TException {
    this.killTopologyWithOpts(name, new KillOptions());
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#killTopologyWithOpts")
  public void killTopologyWithOpts(String stormName, KillOptions options)
      throws NotAliveException, TException {
    try {
      NimbusUtils.checkStormActive(nimbus, stormName, true);
      Integer wait_amt = null;
      if (options.is_set_wait_secs()) {
        wait_amt = options.get_wait_secs();
      }
      NimbusUtils.transitionName(nimbus, stormName, true, StatusType.kill,
          wait_amt);
    } catch (Throwable e) {
      String errMsg = "Failed to kill topology " + stormName;
      LOG.error(errMsg, e);
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#rebalance")
  public void rebalance(String stormName, RebalanceOptions options)
      throws NotAliveException, TException, InvalidTopologyException {
    try {
      NimbusUtils.checkStormActive(nimbus, stormName, true);
      Integer waitAmt = null;
      Integer numWorkers = null;
      Map<String, Integer> executorOverrides = null;
      if (options != null) {
        if (options.is_set_wait_secs()) {
          waitAmt = options.get_wait_secs();
        }
        if (options.is_set_num_workers()) {
          numWorkers = options.get_num_workers();
        }
        if (options.is_set_num_executors()) {
          executorOverrides = options.get_num_executors();
        }
      }
      if (executorOverrides != null) {
        for (Integer numExecutor : executorOverrides.values()) {
          if (numExecutor <= 0) {
            throw new InvalidTopologyException(
                "Number of executors must be greater than 0");
          }
        }
      }
      NimbusUtils.transitionName(nimbus, stormName, true, StatusType.rebalance,
          waitAmt, numWorkers, executorOverrides);
    } catch (Throwable e) {
      String errMsg = "Failed to rebalance topology " + stormName;
      LOG.error(errMsg, e);
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#activate")
  public void activate(String stormName) throws NotAliveException, TException {
    try {
      NimbusUtils.transitionName(nimbus, stormName, true, StatusType.activate);
    } catch (Throwable e) {
      String errMsg = "Failed to active topology " + stormName;
      LOG.error(errMsg, e);
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#deactivate")
  public void deactivate(String stormName) throws NotAliveException, TException {
    try {
      NimbusUtils
          .transitionName(nimbus, stormName, true, StatusType.inactivate);
    } catch (Throwable e) {
      String errMsg = "Failed to deactivate topology " + stormName;
      LOG.error(errMsg, e);
      throw new TException(errMsg);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#uploadNewCredentials")
  public void uploadNewCredentials(String stormName, Credentials credentials)
      throws NotAliveException, InvalidTopologyException,
      AuthorizationException, TException {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    try {
      String stormId = Common.getStormId(stormClusterState, stormName);
      Map topologyConf = NimbusUtils.tryReadStormConf(conf, stormId);
      Credentials creds = new Credentials();
      if (credentials != null) {
        creds = credentials;
      }
      // NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
      // "uploadNewCredentials");
      synchronized (nimbus.getCredUpdateLock()) {
        stormClusterState.setCredentials(stormId, creds, topologyConf);
      }
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#beginFileUpload")
  public String beginFileUpload() throws TException {
    String fileLoc = null;
    try {
      fileLoc =
          ConfigUtil.masterInbox(conf) + "/stormjar-" + UUID.randomUUID()
              + ".jar";
      nimbus.getUploaders().put(fileLoc,
          Channels.newChannel(new FileOutputStream(fileLoc)));
      LOG.info("Uploading file from client to " + fileLoc);
    } catch (FileNotFoundException e) {
      LOG.error(" file not found " + fileLoc);
      throw new TException(e);
    } catch (IOException e) {
      LOG.error(" IOException  " + fileLoc, e);
      throw new TException(e);
    }
    return fileLoc;
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#uploadChunk")
  public void uploadChunk(String location, ByteBuffer chunk) throws TException {
    FileCacheMap<Object, Object> uploaders = nimbus.getUploaders();
    Object obj = uploaders.get(location);
    if (obj == null) {
      throw new TException(
          "File for that location does not exist (or timed out) " + location);
    }
    try {
      if (obj instanceof WritableByteChannel) {
        WritableByteChannel channel = (WritableByteChannel) obj;
        channel.write(chunk);
        uploaders.put(location, channel);
      } else {
        throw new TException("Object isn't WritableByteChannel for " + location);
      }
    } catch (IOException e) {
      String errMsg =
          " WritableByteChannel write filed when uploadChunk " + location;
      LOG.error(errMsg);
      throw new TException(e);
    }
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#finishFileUpload")
  public void finishFileUpload(String location) throws TException {
    FileCacheMap<Object, Object> uploaders = nimbus.getUploaders();
    Object obj = uploaders.get(location);
    if (obj == null) {
      throw new TException(
          "File for that location does not exist (or timed out)");
    }
    try {
      if (obj instanceof WritableByteChannel) {
        WritableByteChannel channel = (WritableByteChannel) obj;
        channel.close();
        uploaders.remove(location);
        LOG.info("Finished uploading file from client: " + location);
      } else {
        throw new TException("Object isn't WritableByteChannel for " + location);
      }
    } catch (IOException e) {
      LOG.error(" WritableByteChannel close failed when finishFileUpload "
          + location);
    }
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#beginFileDownload")
  public String beginFileDownload(String file) throws TException {
    // NimbusUtils.checkAuthorization(nimbus, null, null, "fileDownload");
    // NimbusUtils.checkFileAccess(nimbus.getConf(), file);
    BufferFileInputStream is = null;
    String id = null;
    try {
      is = new BufferFileInputStream(file);
      id = UUID.randomUUID().toString();
      nimbus.getDownloaders().put(id, is);
    } catch (FileNotFoundException e) {
      LOG.error(e + "file:" + file + " not found");
      throw new TException(e);
    }
    return id;
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#downloadChunk")
  public ByteBuffer downloadChunk(String id) throws TException {
    FileCacheMap<Object, Object> downloaders = nimbus.getDownloaders();
    Object obj = downloaders.get(id);
    if (obj == null || !(obj instanceof BufferFileInputStream)) {
      throw new TException("Could not find input stream for that id");
    }
    BufferFileInputStream is = (BufferFileInputStream) obj;
    byte[] ret = null;
    try {
      ret = is.read();
      if (ret != null) {
        downloaders.put(id, (BufferFileInputStream) is);
      } else {
        downloaders.remove(id);
        return null;
      }
    } catch (IOException e) {
      LOG.error(e + "BufferFileInputStream read failed when downloadChunk ");
      throw new TException(e);
    }
    return ByteBuffer.wrap(ret);
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getClusterInfo")
  public ClusterSummary getClusterInfo() throws TException {
    try {
      // NimbusUtils.checkAuthorization(nimbus, null, null, "getClusterInfo");
      StormClusterState stormClusterState = nimbus.getStormClusterState();
      Map<String, SupervisorInfo> supervisorInfos =
          NimbusUtils.allSupervisorInfo(stormClusterState);
      // TODO: need to get the port info about supervisors...
      // in standalone just look at metadata, otherwise just say N/A?
      List<SupervisorSummary> supervisorSummaries =
          NimbusUtils.mkSupervisorSummaries(stormClusterState, supervisorInfos);
      int nimbusUptime = nimbus.uptime();
      Map<String, StormBase> bases = Common.topologyBases(stormClusterState);
      List<TopologySummary> topologySummaries =
          NimbusUtils.mkTopologySummaries(nimbus, stormClusterState, bases);

      return new ClusterSummary(supervisorSummaries, nimbusUptime,
          topologySummaries);
    } catch (Exception e) {
      LOG.error("Failed to get ClusterSummary ", e);
      throw new TException(e);
    }
  }

  @SuppressWarnings({ "rawtypes" })
  public SupervisorWorkers getSupervisorWorkers(String supervisorId)
      throws NotAliveException, TException {
    try {
      if (supervisorId == null) {
        throw new TException("No supervisor of " + supervisorId);
      }
      StormClusterState stormClusterState = nimbus.getStormClusterState();
      SupervisorInfo supervisorInfo =
          stormClusterState.supervisorInfo(supervisorId);
      Map<String, Assignment> assignments = new HashMap<String, Assignment>();
      // get all active topology's StormBase
      Map<String, StormBase> bases = Common.topologyBases(stormClusterState);
      for (Entry<String, StormBase> entry : bases.entrySet()) {
        String topologyId = entry.getKey();
        Assignment assignment =
            stormClusterState.assignmentInfo(topologyId, null);
        if (assignment == null) {
          LOG.error("Failed to get assignment of " + topologyId);
          continue;
        }
        assignments.put(topologyId, assignment);
      }

      Map<Integer, WorkerSummary> portWorkerSummarys =
          new TreeMap<Integer, WorkerSummary>();
      for (Entry<String, Assignment> entry : assignments.entrySet()) {
        String topologyId = entry.getKey();
        Assignment assignment = entry.getValue();
        Map stormConf = NimbusUtils.readStormConf(conf, topologyId);
        StormTopology userTopology =
            NimbusUtils.readStormTopology(conf, topologyId);
        Map<Integer, String> taskToComponent =
            Common.stormTaskInfo(userTopology, stormConf);
        List<ExecutorSummary> executorSummaries =
            NimbusUtils.mkExecutorSummaries(stormClusterState, assignment,
                taskToComponent, topologyId);

        Map<ExecutorInfo, WorkerSlot> executorInfoToWorkerSlot =
            assignment.getExecutorToNodeport();
        for (Entry<ExecutorInfo, WorkerSlot> resourceEntry : executorInfoToWorkerSlot
            .entrySet()) {
          ExecutorInfo executorInfo = resourceEntry.getKey();
          WorkerSlot workerSlot = resourceEntry.getValue();
          if (supervisorId.equals(workerSlot.getNodeId()) == false) {
            continue;
          }
          Integer port = workerSlot.getPort();
          WorkerSummary workerSummary = portWorkerSummarys.get(port);
          if (workerSummary == null) {
            workerSummary = new WorkerSummary();
            workerSummary.set_port(port);
            workerSummary.set_topology(topologyId);
            workerSummary.set_tasks(new ArrayList<ExecutorSummary>());
            portWorkerSummarys.put(port, workerSummary);
          }
          List<ExecutorSummary> tasks = workerSummary.get_tasks();
          for (ExecutorSummary es : executorSummaries) {
            if (es.get_executor_info().equals(executorInfo)) {
              tasks.add(es);
            }
          }
        }
      }
      List<WorkerSummary> wokersList = new ArrayList<WorkerSummary>();
      wokersList.addAll(portWorkerSummarys.values());
      SupervisorSummary supervisorSummary =
          NimbusUtils.mkSupervisorSummary(supervisorInfo, supervisorId);
      return new SupervisorWorkers(supervisorSummary, wokersList);
    } catch (TException e) {
      LOG.info("Failed to get ClusterSummary ", e);
      throw e;
    } catch (Exception e) {
      LOG.info("Failed to get ClusterSummary ", e);
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getTopologyInfo")
  public TopologyInfo getTopologyInfo(String stormId) throws NotAliveException,
      TException {
    GetInfoOptions options = new GetInfoOptions();
    options.set_num_err_choice(NumErrorsChoice.ALL);
    return this.getTopologyInfoWithOpts(stormId, options);
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getTopologyConf")
  public String getTopologyConf(String id) throws NotAliveException, TException {
    Map topologyConf = NimbusUtils.tryReadStormConf(conf, id);
    return CoreUtil.to_json(topologyConf);
  }

  public String getSupervisorConf(String id) throws NotAliveException,
      TException {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    SupervisorInfo supervisorInfo = null;
    try {
      supervisorInfo = stormClusterState.supervisorInfo(id);
    } catch (Exception e) {
      throw new TException(e);
    }
    return CoreUtil.to_json(supervisorInfo.getSupervisorConf());
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getTopology")
  public StormTopology getTopology(String id) throws NotAliveException,
      TException {
    Map topologyConf = NimbusUtils.tryReadStormConf(conf, id);
    // String stormName =
    // CoreUtil.parseString(topologyConf.get(Config.TOPOLOGY_NAME),
    // "unknown-topology-name");
    // NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
    // "getTopology");
    StormTopology stormtopology = NimbusUtils.tryReadStormTopology(conf, id);
    return Common.systemTopology(topologyConf, stormtopology);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void shutdown() {
    LOG.info("Shutting down master");
    try {
      nimbus.getScheduExec().shutdown();
      nimbus.getStormClusterState().disconnect();
      nimbus.getDownloaders().cleanup();
      nimbus.getUploaders().cleanup();
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
    LOG.info("Shut down master");
  }

  @Override
  public boolean waiting() {
    return false;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getNimbusConf")
  public String getNimbusConf() throws TException {
    return CoreUtil.to_json(conf);
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getUserTopology")
  public StormTopology getUserTopology(String id) throws NotAliveException,
      TException {
    Map topologyConf = NimbusUtils.tryReadStormConf(conf, id);
    // String stormName =
    // CoreUtil.parseString(topologyConf.get(Config.TOPOLOGY_NAME),
    // "unknown-topology-name");
    // NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
    // "getUserTopology");
    return NimbusUtils.tryReadStormTopology(topologyConf, id);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void registerLeaderHost(Map conf) throws Exception,
      UnknownHostException, IOException {
    // set nimbus host and port
    String host = InetAddress.getLocalHost().getHostName().toString();
    Long port = CoreUtil.parseLong(conf.get(Config.NIMBUS_THRIFT_PORT), -1);
    port = Long.valueOf(NetWorkUtils.assignServerPort(port.intValue()));
    conf.put(Config.NIMBUS_HOST, host);
    conf.put(Config.NIMBUS_THRIFT_PORT, port);

    // write to zk
    Set<Long> ports = new HashSet<Long>();
    ports.add(port);
    nimbus.getStormClusterState().registerLeaderHost(new NodeInfo(host, ports));
    LOG.info("{}:{} register successfully!", host, port);
  }

  private void initTopologyStatus() throws Exception {
    Set<String> activeStormIds = nimbus.getStormClusterState().activeStorms();
    if (activeStormIds != null) {
      for (String stormId : activeStormIds) {
        NimbusUtils.transition(nimbus, stormId, false, StatusType.startup);
      }
    }
    LOG.info("Successfully init topology status");
  }

  /**
   * Schedule Nimbus monitor
   * 
   * @param conf
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  private void scheduleNimbusMonitor(Map conf) throws Exception {
    int monitorFreqSecs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
    final ScheduledExecutorService scheduExec = nimbus.getScheduExec();
    MonitorRunnable r1 = new MonitorRunnable(nimbus);
    scheduExec.scheduleAtFixedRate(r1, 0, monitorFreqSecs, TimeUnit.SECONDS);
    LOG.info("Successfully init Monitor thread");
  }

  /**
   * Schedule Nimbus inbox cleaner Right now, every 600 seconds, nimbus will
   * clean jar under /LOCAL-DIR/nimbus/inbox, which is the uploading topology
   * directory
   * 
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  private void scheduleNimbusInboxCleaner(Map conf) throws IOException {
    final ScheduledExecutorService scheduExec = nimbus.getScheduExec();
    // Schedule Nimbus inbox cleaner
    String dir_location = ConfigUtil.masterInbox(conf);
    int inbox_jar_expiration_secs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS),
            3600);
    CleanInboxRunnable clean_inbox =
        new CleanInboxRunnable(dir_location, inbox_jar_expiration_secs);
    int cleanup_inbox_freq_secs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS),
            600);
    scheduExec.scheduleAtFixedRate(clean_inbox, 0, cleanup_inbox_freq_secs,
        TimeUnit.SECONDS);
    LOG.info("Successfully init " + dir_location + " cleaner");
  }

  @SuppressWarnings({ "rawtypes" })
  @Override
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getTopologyInfoWithOpts")
  public TopologyInfo getTopologyInfoWithOpts(String stormId,
      GetInfoOptions options) throws NotAliveException, AuthorizationException,
      TException {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    Map topologyConf = NimbusUtils.tryReadStormConf(conf, stormId);
    String stormName = (String) topologyConf.get(Config.TOPOLOGY_NAME);
    // NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
    // "getTopologyInfo");
    StormBase base = null;
    Assignment assignment = null;
    try {
      Map<Integer, String> taskToComponent =
          Common.stormTaskInfo(NimbusUtils.tryReadStormTopology(conf, stormId),
              topologyConf);
      base = stormClusterState.stormBase(stormId, null);
      int launchTimeSecs = 0;
      if (base != null) {
        launchTimeSecs = base.get_launch_time_secs();
      } else {
        throw new NotAliveException(stormId);
      }
      assignment = stormClusterState.assignmentInfo(stormId, null);
      Map<ExecutorInfo, ExecutorCache> beats =
          nimbus.getExecutorHeartbeatsCache().get(stormId);
      Set<String> allComponents =
          CoreUtil.reverse_map(taskToComponent).keySet();

      NumErrorsChoice numErrChoice =
          options.get_num_err_choice() != null ? options.get_num_err_choice()
              : NumErrorsChoice.ALL;
      Map<String, List<ErrorInfo>> errors =
          new HashMap<String, List<ErrorInfo>>();
      for (String c : allComponents) {
        errors.put(c,
            NimbusUtils.errorsFn(numErrChoice, stormClusterState, stormId, c));
      }

      Map<ExecutorInfo, WorkerSlot> executorToNodeports =
          assignment.getExecutorToNodeport();
      List<ExecutorSummary> executorSummaries =
          new ArrayList<ExecutorSummary>();
      for (Map.Entry<ExecutorInfo, WorkerSlot> executorToNodeport : executorToNodeports
          .entrySet()) {
        ExecutorInfo executor = executorToNodeport.getKey();
        WorkerSlot nodeToPort = executorToNodeport.getValue();
        String host = assignment.getNodeHost().get(nodeToPort.getNodeId());
        ExecutorCache heartbeat = beats.get(executor);
        StatsData stats = heartbeat.getHeartbeat().getStats();
        ExecutorStats executorStats = new ExecutorStats();
        if (stats != null) {
          executorStats = Stats.thriftifyExecutorStats(stats);
        }
        ExecutorSummary executorSummary =
            new ExecutorSummary(executor, taskToComponent.get(executor
                .get_task_start()), host, nodeToPort.getPort(),
                heartbeat.getNimbusTime());
        executorSummary.set_stats(executorStats);
        executorSummaries.add(executorSummary);
      }

      TopologyInfo topoInfo =
          new TopologyInfo(stormId, stormName,
              CoreUtil.timeDelta(launchTimeSecs), executorSummaries,
              NimbusUtils.extractStatusStr(base), errors);
      topoInfo.set_owner(base.get_owner());
      String schedStatus = nimbus.getIdToSchedStatus().get(stormId);
      if (schedStatus != null) {
        topoInfo.set_sched_status(schedStatus);
      }
      return topoInfo;
    } catch (Exception e) {
      throw new TException(e);
    }
  }
}
