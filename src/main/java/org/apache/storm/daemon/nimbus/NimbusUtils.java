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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.common.SupervisorInfo;
import org.apache.storm.daemon.nimbus.threads.OlderFileFilter;
import org.apache.storm.daemon.nimbus.transitions.StatusType;
import org.apache.storm.daemon.worker.executor.ExecutorCache;
import org.apache.storm.daemon.worker.executor.heartbeat.ExecutorHeartbeat;
import org.apache.storm.daemon.worker.stats.StatsData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.ReflectionUtils;
import org.apache.storm.util.thread.RunnableCallback;
import org.apache.storm.zookeeper.ZooDefs;
import org.apache.storm.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.Credentials;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.NumErrorsChoice;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.generated.TopologyStatus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.nimbus.DefaultTopologyValidator;
import backtype.storm.nimbus.ITopologyValidator;
import backtype.storm.scheduler.DefaultScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ICredentialsRenewer;
import backtype.storm.security.auth.ReqContext;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;

public class NimbusUtils {
  private static Logger LOG = LoggerFactory.getLogger(NimbusUtils.class);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#mapify-serializations")
  private static Map mapifySerializations(List sers) {
    Map rtn = new HashMap();
    if (sers != null) {
      int size = sers.size();
      for (int i = 0; i < size; i++) {
        if (sers.get(i) instanceof Map) {
          rtn.putAll((Map) sers.get(i));
        } else {
          rtn.put(sers.get(i), null);
        }
      }
    }
    return rtn;
  }

  /**
   * ensure that serializations are same for all tasks no matter what's on the
   * supervisors. this also allows you to declare the serializations as a
   * sequence
   * 
   * @param conf
   * @param stormConf
   * @param topology
   * @return
   * @throws Exception
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#normalize-conf")
  public static Map normalizeConf(Map conf, Map stormConf,
      StormTopology topology) throws Exception {
    Set<String> componentIds = ThriftTopologyUtils.getComponentIds(topology);
    List kryoRegisterList = new ArrayList();
    List kryoDecoratorList = new ArrayList();
    for (String componentId : componentIds) {
      ComponentCommon common =
          ThriftTopologyUtils.getComponentCommon(topology, componentId);
      String json = common.get_json_conf();
      if (json == null) {
        continue;
      }
      Map mtmp = (Map) CoreUtil.from_json(json);
      if (mtmp == null) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed to deserilaize " + componentId);
        sb.append(" json configuration: ");
        sb.append(json);
        LOG.info(sb.toString());
        throw new Exception(sb.toString());
      }
      Object componentKryoRegister = mtmp.get(Config.TOPOLOGY_KRYO_REGISTER);
      if (componentKryoRegister != null) {
        LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
            + ", componentId:" + componentId + ", TOPOLOGY_KRYO_REGISTER"
            + componentKryoRegister.getClass().getName());
        CoreUtil.mergeList(kryoRegisterList, componentKryoRegister);
      }
      Object componentDecorator = mtmp.get(Config.TOPOLOGY_KRYO_DECORATORS);
      if (componentDecorator != null) {
        LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
            + ", componentId:" + componentId + ", TOPOLOGY_KRYO_DECORATOR"
            + componentDecorator.getClass().getName());
        CoreUtil.mergeList(kryoDecoratorList, componentDecorator);
      }
    }

    /**
     * topology level serialization registrations take priority that way, if
     * there's a conflict, a user can force which serialization to use append
     * component conf to storm-conf
     */

    Map totalConf = new HashMap();
    totalConf.putAll(conf);
    totalConf.putAll(stormConf);
    Object totalRegister = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
    if (totalRegister != null) {
      LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
          + ", TOPOLOGY_KRYO_REGISTER" + totalRegister.getClass().getName());
      CoreUtil.mergeList(kryoRegisterList, totalRegister);
    }
    Object totalDecorator = totalConf.get(Config.TOPOLOGY_KRYO_DECORATORS);
    if (totalDecorator != null) {
      LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
          + ", TOPOLOGY_KRYO_DECORATOR" + totalDecorator.getClass().getName());
      CoreUtil.mergeList(kryoDecoratorList, totalDecorator);
    }

    Map kryoRegisterMap = mapifySerializations(kryoRegisterList);
    List decoratorList = CoreUtil.distinctList(kryoDecoratorList);
    Map rtn = new HashMap();
    rtn.putAll(stormConf);
    rtn.put(Config.TOPOLOGY_KRYO_DECORATORS, decoratorList);
    rtn.put(Config.TOPOLOGY_KRYO_REGISTER, kryoRegisterMap);
    rtn.put(Config.TOPOLOGY_ACKER_EXECUTORS,
        totalConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
    rtn.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,
        totalConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
    return rtn;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#component-parallelism")
  public static Integer componentParallelism(Map stormConf,
      ComponentCommon component) {
    Map mergeMap = new HashMap();
    mergeMap.putAll(stormConf);
    String jsonConfString = component.get_json_conf();
    if (jsonConfString != null) {
      Map componentMap = (Map) CoreUtil.from_json(jsonConfString);
      mergeMap.putAll(componentMap);
    }
    Integer taskNum = null;
    Object taskNumObject = mergeMap.get(Config.TOPOLOGY_TASKS);
    if (taskNumObject != null) {
      taskNum = CoreUtil.parseInt(taskNumObject);
    } else {
      taskNum =
          component.is_set_parallelism_hint() ? component
              .get_parallelism_hint() : Integer.valueOf(1);
    }
    Object maxParallelismObject =
        mergeMap.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
    if (maxParallelismObject == null) {
      return taskNum;
    } else {
      int maxParallelism = CoreUtil.parseInt(maxParallelismObject);
      return Math.min(maxParallelism, taskNum);
    }
  }

  public static StormTopology optimizeTopology(StormTopology normalizedTopology) {
    // TODO
    // create new topology by collapsing bolts into CompoundSpout
    // and CompoundBolt
    // need to somehow maintain stream/component ids inside tuples
    return normalizedTopology;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#normalize-topology")
  public static StormTopology normalizeTopology(Map stormConf,
      StormTopology topology) {
    StormTopology ret = topology.deepCopy();
    Map<String, Object> components = Common.allComponents(ret);
    for (Entry<String, Object> entry : components.entrySet()) {
      Object component = entry.getValue();
      ComponentCommon common = null;
      if (component instanceof Bolt) {
        common = ((Bolt) component).get_common();
      }
      if (component instanceof SpoutSpec) {
        common = ((SpoutSpec) component).get_common();
      }
      if (component instanceof StateSpoutSpec) {
        common = ((StateSpoutSpec) component).get_common();
      }
      Map componentConf = new HashMap();
      String jsonConfString = common.get_json_conf();
      if (jsonConfString != null) {
        componentConf.putAll((Map) CoreUtil.from_json(jsonConfString));
      }
      Integer taskNum = componentParallelism(stormConf, common);
      componentConf.put(Config.TOPOLOGY_TASKS, taskNum);

      common.set_json_conf(CoreUtil.to_json(componentConf));
    }
    return ret;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#code-ids")
  public static Set<String> codeIds(Map conf) throws IOException {
    String masterStormdistRoot = ConfigUtil.masterStormdistRoot(conf);
    return CoreUtil.readDirContents(masterStormdistRoot);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#cleanup-storm-ids")
  public static Set<String> cleanupStormIds(Map conf,
      StormClusterState stormClusterState) throws Exception {
    Set<String> heartbeatIds = stormClusterState.heartbeatStorms();
    Set<String> errorIds = stormClusterState.errorTopologies();
    Set<String> codeIds = codeIds(conf);
    Set<String> assignedIds = stormClusterState.activeStorms();
    Set<String> toCleanUpIds = new HashSet<String>();
    if (heartbeatIds != null) {
      toCleanUpIds.addAll(heartbeatIds);
    }
    if (errorIds != null) {
      toCleanUpIds.addAll(errorIds);
    }
    if (codeIds != null) {
      toCleanUpIds.addAll(codeIds);
    }
    if (assignedIds != null) {
      toCleanUpIds.removeAll(assignedIds);
    }
    return toCleanUpIds;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#extract-status-str")
  public static String extractStatusStr(StormBase base) {
    TopologyStatus t = base.get_status();
    return t.name().toUpperCase();
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#do-cleanup")
  public static void doCleanup(NimbusData data) throws Exception {
    StormClusterState clusterState = data.getStormClusterState();

    Set<String> toCleanupIds = new HashSet<String>();
    synchronized (data.getSubmitLock()) {
      toCleanupIds.addAll(cleanupStormIds(data.getConf(),
          data.getStormClusterState()));
    }

    for (String id : toCleanupIds) {
      LOG.info("Cleaning up " + id);
      clusterState.teardownHeartbeats(id);
      clusterState.teardownTopologyErrors(id);

      String masterStormdistRoot =
          ConfigUtil.masterStormdistRoot(data.getConf(), id);
      try {
        CoreUtil.rmr(masterStormdistRoot);
      } catch (IOException e) {
        LOG.error("[cleanup for" + id + " ]Failed to delete "
            + masterStormdistRoot + ",", CoreUtil.stringifyError(e));
      }

      data.getExecutorHeartbeatsCache().remove(id);
    }
  }

  /**
   * Deletes jar files in dir older than seconds.
   * 
   * @param dirLocation
   * @param seconds
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#clean-inbox")
  public static void cleanInbox(String dirLocation, int seconds) {
    File inboxdir = new File(dirLocation);
    OlderFileFilter filter = new OlderFileFilter(seconds);
    File[] files = inboxdir.listFiles(filter);
    for (File f : files) {
      if (f.delete()) {
        LOG.info("Cleaning inbox ... deleted: " + f.getName());
      } else {
        // This should never happen
        LOG.error("Cleaning inbox ... error deleting: " + f.getName());
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#cleanup-corrupt-topologies!")
  public static void cleanupCorruptTopologies(NimbusData nimbusData)
      throws Exception {
    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    Set<String> code_ids = codeIds(nimbusData.getConf());
    // get topology in ZK /storms
    Set<String> active_ids = nimbusData.getStormClusterState().activeStorms();
    if (active_ids != null && active_ids.size() > 0) {
      if (code_ids != null) {
        // clean the topology which is in ZK but not in local dir
        active_ids.removeAll(code_ids);
      }
      for (String corrupt : active_ids) {
        LOG.info("Corrupt topology "
            + corrupt
            + " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...");
        // Just removing the /STORMS is enough
        stormClusterState.removeStorm(corrupt);
      }
    }
    LOG.info("Successfully cleanup all old toplogies");
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#get-errors")
  private static List<ErrorInfo> getErrors(StormClusterState stormClusterState,
      String stormId, String componentId) throws Exception {
    return stormClusterState.errors(stormId, componentId);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#get-last-error")
  private static ErrorInfo getLastError(StormClusterState stormClusterState,
      String stormId, String componentId) throws Exception {
    return stormClusterState.lastError(stormId, componentId);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getTopologyInfoWithOpts#errors-fn")
  public static List<ErrorInfo> errorsFn(NumErrorsChoice numErrChoice,
      StormClusterState stormClusterState, String stormId, String componentId)
      throws Exception {
    List<ErrorInfo> ret = new ArrayList<ErrorInfo>();
    if (numErrChoice.equals(NumErrorsChoice.NONE)) {
      // empty list only
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ONE)) {
      ret.add(getLastError(stormClusterState, stormId, componentId));
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ALL)) {
      ret = getErrors(stormClusterState, stormId, componentId);
      return ret;
    } else {
      LOG.warn("Got invalid NumErrorsChoice '" + numErrChoice + "'");
      ret = getErrors(stormClusterState, stormId, componentId);
      return ret;
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#transition-name!")
  public static <T> void transitionName(NimbusData nimbusData,
      String stormName, boolean errorOnNoTransition,
      StatusType transitionStatus, T... args) throws Exception {
    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    String stormId = Common.getStormId(stormClusterState, stormName);
    if (stormId == null) {
      throw new NotAliveException(stormName);
    }
    transition(nimbusData, stormId, errorOnNoTransition, transitionStatus, args);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#transition!")
  public static <T> void transition(NimbusData nimbusData, String stormId,
      StatusType transitionStatus, T... args) throws Exception {
    transition(nimbusData, stormId, false, transitionStatus, args);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#transition!")
  public static <T> void transition(NimbusData nimbusData, String stormId,
      boolean errorOnNoTransition, StatusType transitionStatus, T... args)
      throws Exception {
    nimbusData.getStatusTransition().transition(stormId, errorOnNoTransition,
        transitionStatus, args);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#mk-scheduler")
  public static IScheduler mkScheduler(@SuppressWarnings("rawtypes") Map conf,
      INimbus inimbus) {
    IScheduler forcedScheduler = inimbus.getForcedScheduler();
    IScheduler scheduler = null;
    if (forcedScheduler != null) {
      LOG.info("Using forced scheduler from INimbus "
          + inimbus.getForcedScheduler().getClass().getName());
      scheduler = forcedScheduler;
    } else if (conf.get(Config.STORM_SCHEDULER) != null) {
      String custormScheduler = (String) conf.get(Config.STORM_SCHEDULER);
      LOG.info("Using custom scheduler: " + custormScheduler);
      try {
        scheduler = (IScheduler) ReflectionUtils.newInstance(custormScheduler);

      } catch (Exception e) {
        LOG.error(custormScheduler
            + " : Scheduler initialize error! Using default scheduler", e);
        scheduler = new DefaultScheduler();
      }
    } else {
      LOG.info("Using default scheduler {}", DefaultScheduler.class.getName());
      scheduler = new DefaultScheduler();
    }
    scheduler.prepare(conf);
    return scheduler;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#NIMBUS-ZK-ACLS")
  public static List<ACL> nimbusZKAcls() {
    ACL first = ZooDefs.Ids.CREATOR_ALL_ACL.get(0);
    return Arrays.asList(first, new ACL(ZooDefs.Perms.READ
        ^ ZooDefs.Perms.CREATE, ZooDefs.Ids.ANYONE_ID_UNSAFE));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#nimbus-data#:validator")
  public static ITopologyValidator mkTopologyValidator(Map conf) {
    String topology_validator =
        (String) conf.get(Config.NIMBUS_TOPOLOGY_VALIDATOR);
    ITopologyValidator validator = null;
    if (topology_validator != null) {
      try {
        validator = ReflectionUtils.newInstance(topology_validator);
      } catch (ClassNotFoundException e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(
            "Fail to make configed NIMBUS_TOPOLOGY_VALIDATOR");
      }
    } else {
      validator = new DefaultTopologyValidator();
    }
    return validator;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getClusterInfo#topology-summaries")
  public static List<TopologySummary> mkTopologySummaries(NimbusData nimbus,
      StormClusterState stormClusterState, Map<String, StormBase> bases)
      throws Exception {
    List<TopologySummary> topologySummaries = new ArrayList<TopologySummary>();

    for (Entry<String, StormBase> entry : bases.entrySet()) {
      String id = entry.getKey();
      StormBase base = entry.getValue();
      Assignment assignment = stormClusterState.assignmentInfo(id, null);
      TopologySummary topoSumm = null;
      if (assignment == null) {
        LOG.warn("Assignment of " + id + " is NULL!");
        topoSumm =
            new TopologySummary(id, base.get_name(), 0, 0, 0,
                CoreUtil.timeDelta(base.get_launch_time_secs()),
                NimbusUtils.extractStatusStr(base));
      } else {
        Map<ExecutorInfo, WorkerSlot> executorToNodePort =
            assignment.getExecutorToNodeport();
        // num_tasks
        Set<ExecutorInfo> executorInfoSet = executorToNodePort.keySet();
        Set<Integer> tasks = new HashSet<Integer>();
        for (ExecutorInfo executorInfo : executorInfoSet) {
          List<Integer> taskIds = Common.executorIdToTasks(executorInfo);
          tasks.addAll(taskIds);
        }
        int numTasks = tasks.size();
        // num_executors
        Integer numExecutors = executorToNodePort.keySet().size();
        // num_workers
        Set<WorkerSlot> workerSlots = new HashSet<WorkerSlot>();
        for (WorkerSlot workerSlot : executorToNodePort.values()) {
          workerSlots.add(workerSlot);
        }
        Integer numWorkers = workerSlots.size();
        topoSumm =
            new TopologySummary(id, base.get_name(), numTasks, numExecutors,
                numWorkers, CoreUtil.timeDelta(base.get_launch_time_secs()),
                NimbusUtils.extractStatusStr(base));
        String owner = base.get_owner();
        if (owner != null) {
          topoSumm.set_owner(owner);
        }
        Map<String, String> idToSchedStatus = nimbus.getIdToSchedStatus();
        String schedStatus = idToSchedStatus.get(id);
        if (schedStatus != null) {
          topoSumm.set_sched_status(schedStatus);
        }
      }
      topologySummaries.add(topoSumm);
    }
    return topologySummaries;
  }

  @SuppressWarnings("unchecked")
  public static SupervisorSummary mkSupervisorSummary(
      SupervisorInfo supervisorInfo, String supervisorId) {
    Set<Integer> ports = (Set<Integer>) supervisorInfo.getMeta();
    SupervisorSummary summary =
        new SupervisorSummary(supervisorInfo.getHostName(),
            supervisorInfo.getUptimeSecs(), ports.size(), supervisorInfo
                .getUsedPorts().size(), supervisorId);
    return summary;
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#getClusterInfo#supervisor-summaries")
  public static List<SupervisorSummary> mkSupervisorSummaries(
      StormClusterState stormClusterState,
      Map<String, SupervisorInfo> supervisorInfos) throws Exception {
    List<SupervisorSummary> ret = new ArrayList<SupervisorSummary>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String supervisorId = entry.getKey();
      SupervisorInfo supervisorInfo = entry.getValue();
      int num_workers = 0;
      // TODO: need to get the port info about supervisors...
      // in standalone just look at metadata, otherwise just say N/A?
      Set<Integer> ports = (Set<Integer>) supervisorInfo.getMeta();

      if (ports != null) {
        num_workers = ports.size();
      }
      SupervisorSummary summary =
          new SupervisorSummary(supervisorInfo.getHostName(),
              supervisorInfo.getUptimeSecs(), num_workers, supervisorInfo
                  .getUsedPorts().size(), supervisorId);
      ret.add(summary);
    }
    Collections.sort(ret, new Comparator<SupervisorSummary>() {
      @Override
      public int compare(SupervisorSummary o1, SupervisorSummary o2) {
        return o1.get_host().compareTo(o2.get_host());
      }
    });
    return ret;
  }

  public static ExecutorSummary mkSimpleExecutorSummary(WorkerSlot resource,
      int taskId, String component, String host, int uptime) {
    ExecutorSummary ret = new ExecutorSummary();
    ExecutorInfo executor_info = new ExecutorInfo(taskId, taskId);
    ret.set_executor_info(executor_info);
    ret.set_component_id(component);
    ret.set_host(host);
    ret.set_port(resource.getPort());
    ret.set_uptime_secs(uptime);
    return ret;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#getTopologyInfo#executor-summaries")
  public static List<ExecutorSummary> mkExecutorSummaries(
      StormClusterState stormClusterState, Assignment assignment,
      Map<Integer, String> taskToComponent, String topologyId) throws Exception {
    List<ExecutorSummary> taskSummaries = new ArrayList<ExecutorSummary>();
    if (null != assignment) {
      Map<ExecutorInfo, WorkerSlot> executorToNodePort =
          assignment.getExecutorToNodeport();
      Map<ExecutorInfo, ExecutorHeartbeat> beats =
          stormClusterState.executorBeats(topologyId, executorToNodePort);
      for (Entry<ExecutorInfo, WorkerSlot> entry : executorToNodePort
          .entrySet()) {
        ExecutorInfo executor = entry.getKey();
        WorkerSlot nodeToPort = entry.getValue();
        Integer port = nodeToPort.getPort();
        String host = assignment.getNodeHost().get(nodeToPort.getNodeId());
        ExecutorHeartbeat heartbeat = beats.get(executor);
        if (heartbeat == null) {
          LOG.warn("Topology " + topologyId + " Executor "
              + executor.toString() + " hasn't been started");
          continue;
        }
        // Counters counters = heartbeat.getCounters();
        StatsData stats = heartbeat.getStats();
        String componentId = taskToComponent.get(executor.get_task_start());
        ExecutorSummary executorSummary =
            new ExecutorSummary(executor, componentId, host, port,
                CoreUtil.parseInt(heartbeat.getUptime(), 0));
        executorSummary.set_stats(stats.getExecutorStats());
        taskSummaries.add(executorSummary);
      }
    }
    return taskSummaries;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#read-storm-conf")
  public static Map readStormConf(Map conf, String stormId) throws Exception {
    Map<String, Object> ret = new HashMap<String, Object>(conf);
    String stormRoot = ConfigUtil.masterStormdistRoot(conf, stormId);
    ret.putAll(Utils.javaDeserialize(FileUtils.readFileToByteArray(new File(
        ConfigUtil.masterStormconfPath(stormRoot))), Map.class));
    return ret;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#DISALLOWED-TOPOLOGY-NAME-STRS")
  public static final String DISALLOWED_TOPOLOYG_NAME_STRS[] = { "@", "/", ".",
      ":", "\\" };

  @ClojureClass(className = "backtype.storm.daemon.nimbus#validate-topology-name!")
  public static void ValidTopologyName(String name)
      throws InvalidTopologyException {
    if (StringUtils.isBlank(name)) {
      throw new InvalidTopologyException("Topology name cannot be blank");
    }
    for (String invalidStr : DISALLOWED_TOPOLOYG_NAME_STRS) {
      if (name.contains(invalidStr)) {
        throw new InvalidTopologyException(
            "Topology name cannot contain any of the following: " + invalidStr);
      }
    }
  }

  /**
   * We will only file at storm_dist_root/topology_id/file to be accessed via
   * Thrift ex., storm-local/nimbus/stormdist/aa-1-1377104853/stormjar.jar
   * 
   * @param conf
   * @param filePath
   * @throws AuthorizationException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#check-file-access")
  public static void checkFileAccess(Map conf, String filePath)
      throws AuthorizationException {
    LOG.debug("check file access:" + filePath);

    try {
      if (new File(ConfigUtil.masterStormdistRoot(conf)).getCanonicalFile() != new File(
          filePath).getCanonicalFile().getParentFile().getParentFile()) {
        throw new AuthorizationException("Invalid file path: " + filePath);
      }

    } catch (Exception e) {
      throw new AuthorizationException("Invalid file path: " + filePath);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#try-read-storm-conf")
  public static Map tryReadStormConf(Map conf, String stormId)
      throws NotAliveException {
    Map topologyConf = null;
    try {
      topologyConf = readStormConf(conf, stormId);
    } catch (Exception e) {
      if (Utils.exceptionCauseIsInstanceOf(FileNotFoundException.class, e)) {
        throw new NotAliveException(stormId);
      }
    }
    return topologyConf;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#try-read-storm-conf-from-name")
  public static Map tryReadStormConfFromName(Map conf, String stormName,
      NimbusData nimbus) throws Exception {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    String id = Common.getStormId(stormClusterState, stormName);
    return tryReadStormConf(conf, id);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#try-read-storm-topology")
  public static StormTopology tryReadStormTopology(Map conf, String stormId)
      throws NotAliveException {
    StormTopology stormTopology = null;
    try {
      stormTopology = readStormTopology(conf, stormId);
    } catch (Exception e) {
      if (Utils.exceptionCauseIsInstanceOf(FileNotFoundException.class, e)) {
        throw new NotAliveException(stormId);
      }
    }
    return stormTopology;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#renew-credentials")
  public static void renewCredentials(NimbusData nimbus) throws Exception {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    Collection<ICredentialsRenewer> renewers = nimbus.getCredRenewers();
    Object updateLock = nimbus.getCredUpdateLock();
    Set<String> assignedIds = stormClusterState.activeStorms();
    if (!assignedIds.isEmpty()) {
      for (String id : assignedIds) {
        synchronized (updateLock) {
          Credentials origCreds = stormClusterState.credentials(id, null);
          Map topologyConf = tryReadStormConf(nimbus.getConf(), id);
          if (origCreds != null) {
            Credentials newCreds = new Credentials(origCreds);
            for (ICredentialsRenewer renewer : renewers) {
              LOG.info("Renewing Creds For " + id + " with "
                  + renewer.toString());
              renewer.renew(newCreds.get_creds(),
                  Collections.unmodifiableMap(topologyConf));
            }
            if (!origCreds.equals(newCreds)) {
              stormClusterState.setCredentials(id, newCreds, topologyConf);
            }
          }
        }
      }
    }
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-executors")
  private static List<ExecutorInfo> computeExecutors(NimbusData nimbusData,
      String stormId) throws Exception {
    Map conf = nimbusData.getConf();
    StormBase stormBase =
        nimbusData.getStormClusterState().stormBase(stormId, null);
    // componentToExecutors={count=12, __acker=3, spout=5, __system=0,
    // split=8}
    List<ExecutorInfo> executors = new ArrayList<ExecutorInfo>();
    if (null == stormBase) {
      return executors;
    }
    Map<String, Integer> componentToExecutors =
        stormBase.get_component_executors();

    Map topologyConf = readStormConf(conf, stormId);
    StormTopology topology = readStormTopology(conf, stormId);

    Map<Integer, String> taskIdToComponent =
        Common.stormTaskInfo(topology, topologyConf);
    Map<String, List<Integer>> componentToTaskIds =
        CoreUtil.reverse_map(taskIdToComponent);

    for (Entry<String, List<Integer>> entry : componentToTaskIds.entrySet()) {
      List<Integer> taskIds = entry.getValue();
      Collections.sort(taskIds);
    }

    // join-maps
    Set<String> allKeys = new HashSet<String>();
    allKeys.addAll(componentToExecutors.keySet());
    allKeys.addAll(componentToTaskIds.keySet());

    Map<String, Map<Integer, List<Integer>>> componentIdToExecutorNumsToTaskids =
        new HashMap<String, Map<Integer, List<Integer>>>();
    for (String componentId : allKeys) {
      if (componentToExecutors.containsKey(componentId)) {
        Map<Integer, List<Integer>> executorNumsToTaskIds =
            new HashMap<Integer, List<Integer>>();
        executorNumsToTaskIds.put(componentToExecutors.get(componentId),
            componentToTaskIds.get(componentId));
        componentIdToExecutorNumsToTaskids.put(componentId,
            executorNumsToTaskIds);
      }
    }

    List<List<Integer>> ret = new ArrayList<List<Integer>>();
    for (String componentId : componentIdToExecutorNumsToTaskids.keySet()) {
      Map<Integer, List<Integer>> executorNumToTaskIds =
          componentIdToExecutorNumsToTaskids.get(componentId);
      for (Integer count : executorNumToTaskIds.keySet()) {
        List<List<Integer>> partitionedList =
            CoreUtil.partitionFixed(count.intValue(),
                executorNumToTaskIds.get(count));
        if (partitionedList != null) {
          ret.addAll(partitionedList);
        }
      }
    }

    for (List<Integer> taskIds : ret) {
      if (taskIds != null && !taskIds.isEmpty()) {
        executors.add(toExecutorId(taskIds));
      }
    }
    return executors;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#to-executor-id")
  private static ExecutorInfo toExecutorId(List<Integer> taskIds) {
    return new ExecutorInfo(taskIds.get(0), taskIds.get(taskIds.size() - 1));

  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-executor->component")
  private static Map<ExecutorInfo, String> computeExecutorToComponent(
      NimbusData nimbusData, String stormId) throws Exception {
    Map conf = nimbusData.getConf();
    List<ExecutorInfo> executors = computeExecutors(nimbusData, stormId);
    StormTopology topology = readStormTopology(nimbusData.getConf(), stormId);
    Map topologyConf = readStormConf(conf, stormId);
    Map<Integer, String> taskToComponent =
        Common.stormTaskInfo(topology, topologyConf);
    Map<ExecutorInfo, String> executorToComponent =
        new HashMap<ExecutorInfo, String>();
    for (ExecutorInfo executor : executors) {
      Integer startTask = executor.get_task_start();
      String component = taskToComponent.get(startTask);
      executorToComponent.put(executor, component);
    }
    return executorToComponent;
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#read-topology-details")
  private static TopologyDetails readTopologyDetails(NimbusData nimbusData,
      String stormId) throws Exception {

    Map conf = nimbusData.getConf();
    StormBase stormBase =
        nimbusData.getStormClusterState().stormBase(stormId, null);
    Map topologyConf = NimbusUtils.readStormConf(conf, stormId);
    StormTopology topology = readStormTopology(nimbusData.getConf(), stormId);
    Map<ExecutorInfo, String> executorInfoToComponent =
        computeExecutorToComponent(nimbusData, stormId);
    Map<ExecutorDetails, String> executorToComponent =
        new HashMap<ExecutorDetails, String>();
    for (ExecutorInfo executor : executorInfoToComponent.keySet()) {
      int startTask = executor.get_task_start();
      int endTask = executor.get_task_end();
      ExecutorDetails detail = new ExecutorDetails(startTask, endTask);
      executorToComponent.put(detail, executorInfoToComponent.get(executor));
    }
    LOG.debug("New StormBase : " + stormBase.toString());
    TopologyDetails details =
        new TopologyDetails(stormId, topologyConf, topology,
            stormBase.get_num_workers(), executorToComponent);
    return details;
  }

  /**
   * compute a topology-id -> executors map
   * 
   * @param nimbusData
   * @param stormIDs
   * @return a topology-id -> executors map
   * @throws Exception
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-topology->executors")
  private static Map<String, Set<ExecutorInfo>> computeTopologyToExecutors(
      NimbusData nimbusData, Set<String> stormIDs) throws Exception {
    Map<String, Set<ExecutorInfo>> ret =
        new HashMap<String, Set<ExecutorInfo>>();

    for (String tid : stormIDs) {
      List<ExecutorInfo> executors = computeExecutors(nimbusData, tid);
      if (executors.size() > 0) {
        Set<ExecutorInfo> executorSet = new HashSet<ExecutorInfo>(executors);
        ret.put(tid, executorSet);
      } else {
        nimbusData.getStormClusterState().removeAssignment(tid);
      }
    }
    return ret;
  }

  /**
   * Does not assume that clocks are synchronized. Executor heartbeat is only
   * used so that nimbus knows when it's received a new heartbeat. All timing is
   * done by nimbus and tracked through heartbeat-cache
   * 
   * @param curr
   * @param hb
   * @param nimTaskTimeOutSecs
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#update-executor-cache")
  private static ExecutorCache updateExecutorCache(ExecutorCache curr,
      ExecutorHeartbeat hb, int nimTaskTimeOutSecs) {
    int reportedTime = 0;
    if (hb != null) {
      reportedTime = hb.getTimeSecs();
    }
    int lastNimbusTime = 0;
    int lastReportedTime = 0;
    if (curr != null) {
      lastNimbusTime = curr.getNimbusTime();
      lastReportedTime = curr.getExecutorReportedTime();
    }
    int newReportedTime;
    if (reportedTime != 0) {
      newReportedTime = reportedTime;
    } else if (lastReportedTime != 0) {
      newReportedTime = lastReportedTime;
    } else {
      newReportedTime = 0;
    }
    int nimbusTime;
    if (lastNimbusTime == 0 || lastReportedTime != newReportedTime) {
      nimbusTime = CoreUtil.current_time_secs();
    } else {
      nimbusTime = lastNimbusTime;
    }
    //
    boolean isTimeOut =
        (0 != nimbusTime && CoreUtil.timeDelta(nimbusTime) >= nimTaskTimeOutSecs);

    return new ExecutorCache(isTimeOut, nimbusTime, newReportedTime, hb);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#update-heartbeat-cache")
  public static Map<ExecutorInfo, ExecutorCache> updateHeartbeatCache(
      Map<ExecutorInfo, ExecutorCache> cache,
      Map<ExecutorInfo, ExecutorHeartbeat> executorBeats,
      Set<ExecutorInfo> allExecutors, int nimTaskTimeOutSecs) {
    Map<ExecutorInfo, ExecutorCache> selectedCache =
        new HashMap<ExecutorInfo, ExecutorCache>();
    if (cache != null) {
      for (Entry<ExecutorInfo, ExecutorCache> entry : cache.entrySet()) {
        ExecutorInfo executorInfo = entry.getKey();
        ExecutorCache executorCache = entry.getValue();
        if (allExecutors.contains(executorInfo)) {
          selectedCache.put(executorInfo, executorCache);
        }
      }
    }
    Map<ExecutorInfo, ExecutorCache> ret =
        new HashMap<ExecutorInfo, ExecutorCache>();
    for (ExecutorInfo executorInfo : allExecutors) {
      ExecutorCache curr = selectedCache.get(executorInfo);
      ret.put(
          executorInfo,
          updateExecutorCache(curr, executorBeats.get(executorInfo),
              nimTaskTimeOutSecs));
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#update-heartbeats!")
  public static void updateHeartbeats(NimbusData nimbus, String stormId,
      Set<ExecutorInfo> allExecutors, Assignment existingAssignment)
      throws Exception {
    LOG.debug("Updating heartbeats for {}:{}", stormId, allExecutors.toString());
    StormClusterState stormClusterState = nimbus.getStormClusterState();

    Map<ExecutorInfo, ExecutorHeartbeat> executorBeats =
        stormClusterState.executorBeats(stormId,
            existingAssignment.getExecutorToNodeport());

    Map<ExecutorInfo, ExecutorCache> cache =
        nimbus.getExecutorHeartbeatsCache().get(stormId);

    Map conf = nimbus.getConf();
    int taskTimeOutSecs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_TASK_TIMEOUT_SECS), 30);
    Map<ExecutorInfo, ExecutorCache> newCache =
        updateHeartbeatCache(cache, executorBeats, allExecutors,
            taskTimeOutSecs);

    ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorCache =
        nimbus.getExecutorHeartbeatsCache();
    executorCache.put(stormId, newCache);
    nimbus.setExecutorHeartbeatsCache(executorCache);
  }

  /**
   * update all the heartbeats for all the topologies's executors
   * 
   * @param nimbus
   * @param existingAssignments
   * @param topologyToExecutors
   * @throws Exception
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#update-all-heartbeats!")
  private static void updateAllHeartbeats(NimbusData nimbus,
      Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors) throws Exception {
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      updateHeartbeats(nimbus, tid, allExecutors, assignment);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#alive-executors")
  private static Set<ExecutorInfo> aliveExecutors(NimbusData nimbus,
      TopologyDetails topologyDetails, Set<ExecutorInfo> allExecutors,
      Assignment existingAssignment) {
    LOG.debug("Computing alive executors for " + topologyDetails.getId() + "\n"
        + "Executors: " + allExecutors.toString() + "\n" + "Assignment: "
        + existingAssignment.toString() + "\n" + "Heartbeat cache: "
        + nimbus.getExecutorHeartbeatsCache().get(topologyDetails.getId()));

    // TODO: need to consider all executors associated with a dead executor
    // (in same slot) dead as well, don't just rely on heartbeat being the
    // same
    Map conf = nimbus.getConf();
    String stormId = topologyDetails.getId();
    Set<ExecutorInfo> allAliveExecutors = new HashSet<ExecutorInfo>();
    Map<ExecutorInfo, Integer> executorStartTimes =
        existingAssignment.getExecutorToStartTimeSecs();
    Map<ExecutorInfo, ExecutorCache> heartbeatsCache =
        nimbus.getExecutorHeartbeatsCache().get(stormId);
    int taskLaunchSecs =
        CoreUtil.parseInt(conf.get(Config.NIMBUS_TASK_LAUNCH_SECS), 30);

    for (ExecutorInfo executor : allExecutors) {
      Integer startTime = executorStartTimes.get(executor);
      boolean isTaskLaunchNotTimeOut =
          CoreUtil.timeDelta(null != startTime ? startTime : 0) < taskLaunchSecs;
      //
      boolean isExeTimeOut = heartbeatsCache.get(executor).isTimedOut();

      if (startTime != null && (isTaskLaunchNotTimeOut || !isExeTimeOut)) {
        allAliveExecutors.add(executor);
      } else {
        LOG.info("Executor {}:{} not alive", stormId, executor.toString());
      }
    }
    return allAliveExecutors;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-topology->alive-executors")
  private static Map<String, Set<ExecutorInfo>> computeTopologyToAliveExecutors(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Topologies topologies,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      String scratchTopologyID) {
    // "compute a topology-id -> alive executors map"
    Map<String, Set<ExecutorInfo>> ret =
        new HashMap<String, Set<ExecutorInfo>>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      TopologyDetails topologyDetails = topologies.getById(tid);
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      Set<ExecutorInfo> aliveExecutors = new HashSet<ExecutorInfo>();
      if (scratchTopologyID != null && scratchTopologyID == tid) {
        // TODO never execute this code, see mkExistingAssignments
        aliveExecutors = allExecutors;
      } else {
        aliveExecutors =
            aliveExecutors(nimbusData, topologyDetails, allExecutors,
                assignment);
      }
      ret.put(tid, aliveExecutors);
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-supervisor->dead-ports")
  public static Map<String, Set<Integer>> computeSupervisorToDeadports(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {
    Set<WorkerSlot> deadSlots =
        filterDeadSlots(existingAssignments, topologyToExecutors,
            topologyToAliveExecutors);

    Map<String, Set<Integer>> supervisorToDeadports =
        new HashMap<String, Set<Integer>>();
    for (WorkerSlot workerSlot : deadSlots) {
      String nodeId = workerSlot.getNodeId();
      Integer port = (Integer) workerSlot.getPort();
      if (supervisorToDeadports.containsKey(nodeId)) {
        Set<Integer> deadportsList = supervisorToDeadports.get(nodeId);
        deadportsList.add(port);
      } else {
        Set<Integer> deadportsList = new HashSet<Integer>();
        deadportsList.add(port);
        supervisorToDeadports.put(nodeId, deadportsList);
      }
    }
    return supervisorToDeadports;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-supervisor->dead-ports#dead-slots")
  private static Set<WorkerSlot> filterDeadSlots(
      Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {
    Set<WorkerSlot> deadSlots = new HashSet<WorkerSlot>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      Set<ExecutorInfo> aliveExecutors = topologyToAliveExecutors.get(tid);
      Set<ExecutorInfo> deadExecutors =
          CoreUtil.set_difference(allExecutors, aliveExecutors);

      Map<ExecutorInfo, WorkerSlot> allExecutorToNodeport =
          assignment.getExecutorToNodeport();
      for (Entry<ExecutorInfo, WorkerSlot> item : allExecutorToNodeport
          .entrySet()) {
        ExecutorInfo executor = item.getKey();
        WorkerSlot deadSlot = item.getValue();
        if (deadExecutors.contains(executor)) {
          deadSlots.add(deadSlot);
        }
      }
    }
    return deadSlots;
  }

  /**
   * convert assignment information in zk to SchedulerAssignment, so it can be
   * used by scheduler api.
   * 
   * @param nimbusData
   * @param existingAssignments
   * @param topologyToAliveExecutors
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-topology->scheduler-assignment")
  private static Map<String, SchedulerAssignmentImpl> computeTopologyToSchedulerAssignment(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {

    Map<String, SchedulerAssignmentImpl> ret =
        new HashMap<String, SchedulerAssignmentImpl>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> aliveExecutors = topologyToAliveExecutors.get(tid);
      Map<ExecutorInfo, WorkerSlot> executorToNodeport =
          assignment.getExecutorToNodeport();
      Map<ExecutorDetails, WorkerSlot> executorToSlot =
          new HashMap<ExecutorDetails, WorkerSlot>();

      for (Entry<ExecutorInfo, WorkerSlot> item : executorToNodeport.entrySet()) {
        ExecutorInfo executor = item.getKey();
        if (aliveExecutors.contains(executor)) {
          ExecutorDetails executorDetails =
              new ExecutorDetails(executor.get_task_start(),
                  executor.get_task_end());
          WorkerSlot workerSlot = item.getValue();
          executorToSlot.put(executorDetails, workerSlot);
        }
      }

      ret.put(tid, new SchedulerAssignmentImpl(tid, executorToSlot));
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#num-used-workers")
  public static int numUsedWorkers(SchedulerAssignment schedulerAssignment) {
    if (schedulerAssignment != null) {
      return schedulerAssignment.getSlots().size();
    }
    return 0;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#all-scheduling-slots")
  private static Collection<WorkerSlot> allSchedulingSlots(
      NimbusData nimbusData, Topologies topologies,
      List<String> missingAssignmentTopologies) throws Exception {
    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    INimbus inimbus = nimbusData.getInimubs();

    Map<String, SupervisorInfo> supervisorInfos =
        allSupervisorInfo(stormClusterState);

    List<SupervisorDetails> supervisorDetails =
        new ArrayList<SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String id = entry.getKey();
      SupervisorInfo info = entry.getValue();
      supervisorDetails.add(new SupervisorDetails(id, info.getMeta()));
    }

    Collection<WorkerSlot> ret =
        inimbus.allSlotsAvailableForScheduling(supervisorDetails, topologies,
            new HashSet<String>(missingAssignmentTopologies));
    return ret;
  }

  /**
   * 
   * @param nimbusData
   * @param allSchedulingSlots
   * @param supervisorToDeadPorts
   * @return return a map: {topology-id SupervisorDetails}
   * @throws Exception
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#read-all-supervisor-details")
  private static Map<String, SupervisorDetails> readAllSupervisorDetails(
      NimbusData nimbusData, Map<String, Set<Integer>> allSchedulingSlots,
      Map<String, Set<Integer>> supervisorToDeadPorts) throws Exception {

    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    Map<String, SupervisorInfo> supervisorInfos =
        NimbusUtils.allSupervisorInfo(stormClusterState);

    Map<String, Set<Integer>> nonexistentSupervisorSlots =
        new HashMap<String, Set<Integer>>();
    nonexistentSupervisorSlots.putAll(allSchedulingSlots);
    for (String sid : supervisorInfos.keySet()) {
      if (nonexistentSupervisorSlots.containsKey(sid)) {
        nonexistentSupervisorSlots.remove(sid);
      }
    }

    Map<String, SupervisorDetails> allSupervisorDetails =
        new HashMap<String, SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String sid = entry.getKey();
      SupervisorInfo supervisorInfo = entry.getValue();
      String hostname = supervisorInfo.getHostName();
      Set<Integer> deadPorts = new HashSet<Integer>();
      if (supervisorToDeadPorts.containsKey(sid)) {
        deadPorts = supervisorToDeadPorts.get(sid);
      }
      // hide the dead-ports from the all-ports
      // these dead-ports can be reused in next round of assignments
      Set<Integer> allPortSet = new HashSet<Integer>();
      Set<Integer> metaPorts = new HashSet<Integer>();

      if (allSchedulingSlots != null && allSchedulingSlots.containsKey(sid)) {
        allPortSet.addAll(allSchedulingSlots.get(sid));
        metaPorts.addAll(allSchedulingSlots.get(sid));
      }
      allPortSet.removeAll(deadPorts);

      List<Number> allPorts = new ArrayList<Number>();
      allPorts.addAll(allPortSet);
      allSupervisorDetails.put(sid, new SupervisorDetails(sid, hostname,
          supervisorInfo.getSchedulerMeta(), allPorts));
    }

    for (Entry<String, Set<Integer>> entry : nonexistentSupervisorSlots
        .entrySet()) {
      String sid = entry.getKey();
      Set<Number> ports = new HashSet<Number>();
      ports.addAll(entry.getValue());
      allSupervisorDetails.put(sid, new SupervisorDetails(sid, ports, ports));
    }

    return allSupervisorDetails;
  }

  /**
   * 
   * @param schedulerAssignments {topology-id -> SchedulerAssignment}
   * @return {topology-id -> {executor [node port]}}
   */
  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-topology->executor->node+port")
  private static Map<String, Map<ExecutorDetails, WorkerSlot>> computeTopologyToExecutorToNodeport(
      Map<String, SchedulerAssignment> schedulerAssignments) {

    Map<String, Map<ExecutorDetails, WorkerSlot>> ret =
        new HashMap<String, Map<ExecutorDetails, WorkerSlot>>();
    for (Entry<String, SchedulerAssignment> entry : schedulerAssignments
        .entrySet()) {
      String tid = entry.getKey();
      SchedulerAssignment assignment = entry.getValue();
      Map<ExecutorDetails, WorkerSlot> executorToSlot =
          assignment.getExecutorToSlot();
      ret.put(tid, executorToSlot);
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#compute-new-topology->executor->node+port")
  public static Map<String, Map<ExecutorDetails, WorkerSlot>> computeNewTopologyToExecutorToNodeport(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Topologies topologies, String scratchTopologyID) throws Exception {
    // topology->executors
    Map<String, Set<ExecutorInfo>> topologyToExecutor =
        computeTopologyToExecutors(nimbusData, existingAssignments.keySet());

    // update the executors heartbeats first.
    updateAllHeartbeats(nimbusData, existingAssignments, topologyToExecutor);

    // topology->alive-executors
    Map<String, Set<ExecutorInfo>> topologyToAliveExecutors =
        computeTopologyToAliveExecutors(nimbusData, existingAssignments,
            topologies, topologyToExecutor, scratchTopologyID);

    // supervisor->dead-ports
    Map<String, Set<Integer>> supervisorToDeadPorts =
        computeSupervisorToDeadports(nimbusData, existingAssignments,
            topologyToExecutor, topologyToAliveExecutors);

    // topology->scheduler-assignment
    Map<String, SchedulerAssignmentImpl> topologyToSchedulerAssignment =
        computeTopologyToSchedulerAssignment(nimbusData, existingAssignments,
            topologyToAliveExecutors);

    // missing-assignment-topologies
    List<String> missingAssignmentTopologies =
        mkMissingAssignmentTopologies(topologies, topologyToExecutor,
            topologyToAliveExecutors, topologyToSchedulerAssignment);

    // all-scheduling-slots
    Collection<WorkerSlot> allSchedulingSlotsList =
        allSchedulingSlots(nimbusData, topologies, missingAssignmentTopologies);

    Map<String, Set<Integer>> allSchedulingSlots =
        new HashMap<String, Set<Integer>>();
    for (WorkerSlot workerSlot : allSchedulingSlotsList) {
      String nodeId = workerSlot.getNodeId();
      Integer port = (Integer) workerSlot.getPort();
      if (allSchedulingSlots.containsKey(nodeId)) {
        Set<Integer> portsSet = allSchedulingSlots.get(nodeId);
        portsSet.add(port);
      } else {
        Set<Integer> portsSet = new HashSet<Integer>();
        portsSet.add(port);
        allSchedulingSlots.put(nodeId, portsSet);
      }
    }

    // supervisors
    Map<String, SupervisorDetails> supervisors =
        readAllSupervisorDetails(nimbusData, allSchedulingSlots,
            supervisorToDeadPorts);

    backtype.storm.scheduler.Cluster cluster =
        new backtype.storm.scheduler.Cluster(nimbusData.getInimubs(),
            supervisors, topologyToSchedulerAssignment);

    // call scheduler.schedule to schedule all the topologies
    // the new assignments for all the topologies are in the cluster object.
    nimbusData.getScheduler().schedule(topologies, cluster);

    // new-scheduler-assignments
    Map<String, SchedulerAssignment> newSchedulerAssignments =
        cluster.getAssignments();

    // add more information to convert SchedulerAssignment to Assignment
    Map<String, Map<ExecutorDetails, WorkerSlot>> newTopologyToExecutorToNodeport =
        computeTopologyToExecutorToNodeport(newSchedulerAssignments);
    // print some useful information
    for (Entry<String, Map<ExecutorDetails, WorkerSlot>> entry : newTopologyToExecutorToNodeport
        .entrySet()) {
      Map<ExecutorDetails, WorkerSlot> reassignment =
          new HashMap<ExecutorDetails, WorkerSlot>();
      String tid = entry.getKey();
      Map<ExecutorDetails, WorkerSlot> executorToNodeport = entry.getValue();

      Map<ExecutorInfo, WorkerSlot> oldExecutorToNodeport =
          new HashMap<ExecutorInfo, WorkerSlot>();
      if (existingAssignments.containsKey(tid)) {
        oldExecutorToNodeport =
            existingAssignments.get(tid).getExecutorToNodeport();
      }

      for (Entry<ExecutorDetails, WorkerSlot> item : executorToNodeport
          .entrySet()) {
        ExecutorDetails executorDetails = item.getKey();
        WorkerSlot workerSlot = item.getValue();
        ExecutorInfo execute =
            new ExecutorInfo(executorDetails.getStartTask(),
                executorDetails.getEndTask());
        if (oldExecutorToNodeport.containsKey(execute)
            && !workerSlot.equals(oldExecutorToNodeport.get(execute))) {
          reassignment.put(executorDetails, workerSlot);
        }
      }
      if (!reassignment.isEmpty()) {
        int newSlotCnt = executorToNodeport.values().size();
        Set<ExecutorDetails> reassignExecutors = reassignment.keySet();

        LOG.info("Reassigning " + tid + " to " + newSlotCnt + " slots");
        LOG.info("Reassign executors:  " + reassignExecutors.toString());
      }
    }
    return newTopologyToExecutorToNodeport;

  }

  private static List<String> mkMissingAssignmentTopologies(
      Topologies topologies,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors,
      Map<String, SchedulerAssignmentImpl> topologyToSchedulerAssignment) {

    Collection<TopologyDetails> topologyDetails = topologies.getTopologies();

    List<String> missingAssignmentTopologies = new ArrayList<String>();
    for (TopologyDetails topologyDetailsItem : topologyDetails) {
      String tid = topologyDetailsItem.getId();
      Set<ExecutorInfo> alle = topologyToExecutors.get(tid);
      Set<ExecutorInfo> alivee = topologyToAliveExecutors.get(tid);
      SchedulerAssignment assignment = topologyToSchedulerAssignment.get(tid);
      int numUsedWorkers = numUsedWorkers(assignment);
      int numWorkers = topologies.getById(tid).getNumWorkers();

      if (alle == null || alle.isEmpty() || !alle.equals(alivee)
          || numUsedWorkers < numWorkers) {
        missingAssignmentTopologies.add(tid);
      }
    }
    return missingAssignmentTopologies;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#changed-executors")
  public static List<ExecutorInfo> changedExecutors(
      Map<ExecutorInfo, WorkerSlot> executorToNodeport,
      Map<ExecutorInfo, WorkerSlot> newExecutorToNodeport) {
    // slot-assigned
    Map<WorkerSlot, List<ExecutorInfo>> slotAssigned =
        CoreUtil.reverse_map(executorToNodeport);
    // new-slot-assigned
    Map<WorkerSlot, List<ExecutorInfo>> newSlotAssigned =
        CoreUtil.reverse_map(newExecutorToNodeport);
    // brand-new-slots
    // Attenion! DO NOT USE Utils.map_diff
    Map<WorkerSlot, List<ExecutorInfo>> brandNewSlots =
        map_diff(slotAssigned, newSlotAssigned);
    List<ExecutorInfo> ret = new ArrayList<ExecutorInfo>();
    for (List<ExecutorInfo> executor : brandNewSlots.values()) {
      ret.addAll(executor);
    }
    return ret;
  }

  // "Returns mappings in m2 that aren't in m1"
  public static Map<WorkerSlot, List<ExecutorInfo>> map_diff(
      Map<WorkerSlot, List<ExecutorInfo>> m1,
      Map<WorkerSlot, List<ExecutorInfo>> m2) {

    Map<WorkerSlot, List<ExecutorInfo>> ret =
        new HashMap<WorkerSlot, List<ExecutorInfo>>();
    for (Entry<WorkerSlot, List<ExecutorInfo>> entry : m2.entrySet()) {
      WorkerSlot key = entry.getKey();
      List<ExecutorInfo> val = entry.getValue();
      if (!m1.containsKey(key)) {
        ret.put(key, val);
      } else if (m1.containsKey(key) && !isListEqual(m1.get(key), val)) {
        ret.put(key, val);
      }
    }
    return ret;
  }

  private static boolean isListEqual(List<ExecutorInfo> list,
      List<ExecutorInfo> val) {
    if (list.size() != val.size()) {
      return false;
    }
    for (ExecutorInfo e : val) {
      if (!list.contains(e)) {
        return false;
      }
    }
    return true;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#newly-added-slots")
  public static Set<WorkerSlot> newlyAddedSlots(Assignment existingAssignment,
      Assignment newAssignment) {
    Set<WorkerSlot> oldSlots = new HashSet<WorkerSlot>();
    if (existingAssignment != null) {
      oldSlots.addAll(existingAssignment.getExecutorToNodeport().values());
    }
    Set<WorkerSlot> newSlots =
        new HashSet<WorkerSlot>(newAssignment.getExecutorToNodeport().values());

    Set<WorkerSlot> newlyAddded = CoreUtil.set_difference(newSlots, oldSlots);
    return newlyAddded;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#basic-supervisor-details-map")
  public static Map<String, SupervisorDetails> basicSupervisorDetailsMap(
      StormClusterState stormClusterState) throws Exception {
    Map<String, SupervisorInfo> infos = allSupervisorInfo(stormClusterState);
    Map<String, SupervisorDetails> ret =
        new HashMap<String, SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : infos.entrySet()) {
      String id = entry.getKey();
      SupervisorInfo info = entry.getValue();
      ret.put(id,
          new SupervisorDetails(id, info.getHostName(),
              info.getSchedulerMeta(), null));
    }
    return ret;
  }

  /**
   * get existing assignment (just the executor->node+port map) -> default to {}
   * . filter out ones which have a executor timeout; figure out available slots
   * on cluster. add to that the used valid slots to get total slots. figure out
   * how many executors should be in each slot (e.g., 4, 4, 4, 5) only keep
   * existing slots that satisfy one of those slots. for rest, reassign them
   * across remaining slots edge case for slots with no executor timeout but
   * with supervisor timeout... just treat these as valid slots that can be
   * reassigned to. worst comes to worse the executor will timeout and won't
   * assign here next time around
   * 
   * @param nimbusData
   * @param scratchTopologyID
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#mk-assignments")
  public static void mkAssignments(NimbusData nimbusData,
      String scratchTopologyID) throws Exception {
    Map conf = nimbusData.getConf();
    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    INimbus inimbus = nimbusData.getInimubs();
    // read all the topologies
    Set<String> topologyIds = stormClusterState.activeStorms();
    if (topologyIds == null || topologyIds.isEmpty()) {
      return;
    }

    Map<String, TopologyDetails> topologieDetails =
        new HashMap<String, TopologyDetails>();
    for (String tid : topologyIds) {
      topologieDetails.put(tid, readTopologyDetails(nimbusData, tid));
    }

    Topologies topologies = new Topologies(topologieDetails);
    // read all the assignments
    Set<String> assignedTopologyIDs = stormClusterState.assignments(null);
    Map<String, Assignment> existingAssignments =
        mkExistingAssignments(stormClusterState, scratchTopologyID,
            assignedTopologyIDs);
    // make the new assignments for topologies
    Map<String, Map<ExecutorDetails, WorkerSlot>> topologyToExecutorToNodeport =
        computeNewTopologyToExecutorToNodeport(nimbusData, existingAssignments,
            topologies, scratchTopologyID);

    int nowSecs = CoreUtil.current_time_secs();

    Map<String, SupervisorDetails> basicSupervisorDetailsMap =
        basicSupervisorDetailsMap(stormClusterState);

    Map<String, Assignment> newAssignments =
        mkNewAssignments(nimbusData, conf, existingAssignments,
            topologyToExecutorToNodeport, nowSecs, basicSupervisorDetailsMap);

    // tasks figure out what tasks to talk to by looking at topology at
    // runtime
    // only log/set when there's been a change to the assignment

    for (Entry<String, Assignment> newAssignmentsEntry : newAssignments
        .entrySet()) {
      String topologyId = newAssignmentsEntry.getKey();
      Assignment assignment = newAssignmentsEntry.getValue();
      Assignment existingAssignment = existingAssignments.get(topologyId);

      if (existingAssignment != null && existingAssignment.equals(assignment)) {
        LOG.debug("Assignment for " + topologyId + " hasn't changed");
        LOG.debug("Assignment: " + assignment.toString());
      } else {
        LOG.info("Setting new assignment for topology id " + topologyId + ": "
            + assignment.toString());
        //
        Map<ExecutorInfo, WorkerSlot> tmpEx =
            assignment.getExecutorToNodeport();
        if (null != tmpEx && tmpEx.size() > 0) {
          stormClusterState.setAssignment(topologyId, assignment);
        } else {
          if (existingAssignments.containsKey(topologyId)) {
            // delete assignment
            stormClusterState.removeAssignment(topologyId);
          }
        }
      }
    }

    Map<String, Collection<WorkerSlot>> newSlotsByTopologyId =
        new HashMap<String, Collection<WorkerSlot>>();

    for (Entry<String, Assignment> newAssignmentsEntry : newAssignments
        .entrySet()) {
      String tid = newAssignmentsEntry.getKey();
      Assignment assignment = newAssignmentsEntry.getValue();
      Assignment existingAssignment = existingAssignments.get(tid);

      Set<WorkerSlot> newlyAddedSlots =
          newlyAddedSlots(existingAssignment, assignment);
      newSlotsByTopologyId.put(tid, newlyAddedSlots);
    }
    inimbus.assignSlots(topologies, newSlotsByTopologyId);
  }

  /**
   * construct the final Assignments by adding start-times etc into it
   * 
   * @param nimbusData
   * @param conf
   * @param existingAssignments
   * @param topologyToExecutorToNodeport
   * @param nowSecs
   * @param basicSupervisorDetailsMap
   * @return
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#mk-assignments#new-assignments")
  private static Map<String, Assignment> mkNewAssignments(
      NimbusData nimbusData,
      Map conf,
      Map<String, Assignment> existingAssignments,
      Map<String, Map<ExecutorDetails, WorkerSlot>> topologyToExecutorToNodeport,
      int nowSecs, Map<String, SupervisorDetails> basicSupervisorDetailsMap)
      throws IOException {
    // construct the final Assignments by adding start-times etc into it
    Map<String, Assignment> newAssignments = new HashMap<String, Assignment>();
    for (Entry<String, Map<ExecutorDetails, WorkerSlot>> entry : topologyToExecutorToNodeport
        .entrySet()) {

      String topologyId = entry.getKey();
      Map<ExecutorDetails, WorkerSlot> executorToNodeport = entry.getValue();
      // existing-assignment
      Assignment existingAssignment = existingAssignments.get(topologyId);
      // all-nodes
      Set<String> allNodes = new HashSet<String>();
      for (WorkerSlot workerSlot : executorToNodeport.values()) {
        allNodes.add(workerSlot.getNodeId());
      }
      // node->host
      Map<String, String> nodeToHost = new HashMap<String, String>();
      for (String node : allNodes) {
        String host =
            nimbusData.getInimubs()
                .getHostName(basicSupervisorDetailsMap, node);
        if (host != null) {
          nodeToHost.put(node, host);
        }
      }
      // all-node->host
      Map<String, String> allNodeToHost = new HashMap<String, String>();
      if (existingAssignment != null) {
        allNodeToHost.putAll(existingAssignment.getNodeHost());
      }
      allNodeToHost.putAll(nodeToHost);
      // reassign-executors
      Map<ExecutorInfo, WorkerSlot> executorInfoToNodeport =
          new HashMap<ExecutorInfo, WorkerSlot>();
      for (Entry<ExecutorDetails, WorkerSlot> item : executorToNodeport
          .entrySet()) {
        ExecutorDetails detail = item.getKey();
        executorInfoToNodeport.put(new ExecutorInfo(detail.getStartTask(),
            detail.getEndTask()), item.getValue());
      }
      Map<ExecutorInfo, WorkerSlot> existingExecutorToNodeport =
          new HashMap<ExecutorInfo, WorkerSlot>();
      if (existingAssignment != null) {
        existingExecutorToNodeport = existingAssignment.getExecutorToNodeport();
      }
      List<ExecutorInfo> reassignExecutors =
          changedExecutors(existingExecutorToNodeport, executorInfoToNodeport);
      // start-times
      Map<ExecutorInfo, Integer> existingExecutorToStartTime =
          new HashMap<ExecutorInfo, Integer>();
      if (existingAssignment != null) {
        existingExecutorToStartTime =
            existingAssignment.getExecutorToStartTimeSecs();
      }

      Map<ExecutorInfo, Integer> reassignExecutorToStartTime =
          new HashMap<ExecutorInfo, Integer>();
      for (ExecutorInfo executor : reassignExecutors) {
        reassignExecutorToStartTime.put(executor, nowSecs);
      }

      Map<ExecutorInfo, Integer> startTimes =
          new HashMap<ExecutorInfo, Integer>(existingExecutorToStartTime);
      for (Entry<ExecutorInfo, Integer> ressignEntry : reassignExecutorToStartTime
          .entrySet()) {
        startTimes.put(ressignEntry.getKey(), ressignEntry.getValue());
      }

      String codeDir = ConfigUtil.masterStormdistRoot(conf, topologyId);
      Map<String, String> nodeHost =
          CoreUtil.select_keys(allNodeToHost, allNodes);

      Assignment newAssignment =
          new Assignment(codeDir, executorInfoToNodeport, nodeHost, startTimes);
      newAssignments.put(topologyId, newAssignment);
    }
    return newAssignments;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#mk-assignments#existing-assignments")
  private static Map<String, Assignment> mkExistingAssignments(
      StormClusterState stormClusterState, String scratchTopologyId,
      Set<String> assignedTopologyIds) throws Exception {
    Map<String, Assignment> existingAssignments =
        new HashMap<String, Assignment>();
    for (String tid : assignedTopologyIds) {
      /*
       * for the topology which wants rebalance (specified by the
       * scratch-topology-id) we exclude its assignment, meaning that all the
       * slots occupied by its assignment will be treated as free slot in the
       * scheduler code.
       */
      if (scratchTopologyId == null || !tid.equals(scratchTopologyId)) {
        Assignment tmpAssignment = stormClusterState.assignmentInfo(tid, null);
        existingAssignments.put(tid, tmpAssignment);
      }
    }
    return existingAssignments;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#assigned-slots")
  private static Map<String, Set<Integer>> assignedSlots(
      StormClusterState stormClusterState) throws Exception {
    // Returns a map from node-id to a set of ports
    Map<String, Set<Integer>> rtn = new HashMap<String, Set<Integer>>();

    Set<String> assignments = stormClusterState.assignments(null);
    for (String a : assignments) {
      Assignment assignment = stormClusterState.assignmentInfo(a, null);
      Map<ExecutorInfo, WorkerSlot> executorToNodeport =
          assignment.getExecutorToNodeport();
      if (executorToNodeport != null) {
        for (Map.Entry<ExecutorInfo, WorkerSlot> entry : executorToNodeport
            .entrySet()) {
          WorkerSlot np = entry.getValue();
          if (rtn.containsKey(np.getNodeId())) {
            Set<Integer> ports = rtn.get(np.getNodeId());
            ports.add(np.getPort());
          } else {
            Set<Integer> tmp = new HashSet<Integer>();
            tmp.add(np.getPort());
            rtn.put(np.getNodeId(), tmp);
          }
        }
      }
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#all-supervisor-info")
  public static Map<String, SupervisorInfo> allSupervisorInfo(
      StormClusterState stormClusterState) throws Exception {
    return allSupervisorInfo(stormClusterState, null);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#all-supervisor-info")
  public static Map<String, SupervisorInfo> allSupervisorInfo(
      StormClusterState stormClusterState, RunnableCallback callback)
      throws Exception {
    Map<String, SupervisorInfo> rtn = new TreeMap<String, SupervisorInfo>();
    Set<String> supervisorIds = stormClusterState.supervisors(callback);
    if (supervisorIds != null) {
      for (String supervisorId : supervisorIds) {
        SupervisorInfo supervisorInfo =
            stormClusterState.supervisorInfo(supervisorId);
        if (supervisorInfo == null) {
          LOG.warn("Failed to get SupervisorInfo of " + supervisorId);
        } else {
          rtn.put(supervisorId, supervisorInfo);
        }
      }
    } else {
      LOG.info("No alive supervisor");
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#setup-storm-code")
  public static void setupStormCode(Map<Object, Object> conf,
      String topologyId, String tmpJarLocation, Map<Object, Object> stormConf,
      StormTopology topology) throws IOException {
    String stormroot = ConfigUtil.masterStormdistRoot(conf, topologyId);
    FileUtils.forceMkdir(new File(stormroot));
    FileUtils.cleanDirectory(new File(stormroot));
    setupJar(conf, tmpJarLocation, stormroot);
    FileUtils.writeByteArrayToFile(
        new File(ConfigUtil.masterStormcodePath(stormroot)),
        Utils.serialize(topology));
    FileUtils.writeByteArrayToFile(
        new File(ConfigUtil.masterStormconfPath(stormroot)),
        Utils.serialize(stormConf));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#read-storm-topology")
  public static StormTopology readStormTopology(Map conf, String stormId)
      throws IOException {
    String stormRoot = ConfigUtil.masterStormdistRoot(conf, stormId);
    return Utils.deserialize(FileUtils.readFileToByteArray(new File(ConfigUtil
        .masterStormcodePath(stormRoot))), StormTopology.class);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#set-topology-status!")
  public static void setTopologyStatus(NimbusData nimbus, String stormId,
      StormBase status) throws Exception {
    StormClusterState stormClusterState = nimbus.getStormClusterState();
    stormClusterState.updateStorm(stormId, status);
    LOG.info("Updated {} with status {} ", stormId, status.get_status().name());
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#validate-topology-size")
  public static void validateTopologySize(Map topoConf, Map nimbusConf,
      StormTopology topology) throws InvalidTopologyException {
    int workersCount =
        CoreUtil.parseInt(topoConf.get(Config.TOPOLOGY_WORKERS), 0);
    Integer workersAllowed =
        CoreUtil.parseInt(nimbusConf.get(Config.NIMBUS_SLOTS_PER_TOPOLOGY));
    Map<String, Integer> numExecutors =
        Common.numStartExecutors(Common.allComponents(topology));
    int executorsCount = numExecutors.values().size();
    Integer executorsAllowed =
        CoreUtil.parseInt(nimbusConf.get(Config.NIMBUS_EXECUTORS_PER_TOPOLOGY));

    if (executorsAllowed != null
        && executorsCount > executorsAllowed.intValue()) {
      throw new InvalidTopologyException(
          "Failed to submit topology. Topology requests more than "
              + executorsAllowed + " executors.");
    }

    if (workersAllowed != null && workersCount > workersAllowed.intValue()) {
      throw new InvalidTopologyException(
          "Failed to submit topology. Topology requests more than "
              + workersAllowed + " workers.");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#setup-jar")
  public static void setupJar(Map conf, String tmpJarLocation, String stormroot)
      throws IOException {
    if (ConfigUtil.isLocalMode(conf)) {
      setupLocalJar(conf, tmpJarLocation, stormroot);
    } else {
      setupDistributedJar(conf, tmpJarLocation, stormroot);
    }
  }

  /**
   * distributed implementation
   * 
   * @param conf
   * @param tmpJarLocation
   * @param stormroot
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#setup-jar:distributed")
  private static void setupDistributedJar(Map conf, String tmpJarLocation,
      String stormroot) throws IOException {
    File srcFile = new File(tmpJarLocation);
    if (!srcFile.exists()) {
      throw new IllegalArgumentException(tmpJarLocation + " to copy to "
          + stormroot + " does not exist!");
    }
    String path = ConfigUtil.masterStormjarPath(stormroot);
    File destFile = new File(path);
    FileUtils.copyFile(srcFile, destFile);
  }

  /**
   * local implementation
   * 
   * @param conf
   * @param tmpJarLocation
   * @param stormroot
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#setup-jar:local")
  private static void setupLocalJar(Map conf, String tmpJarLocation,
      String stormroot) throws IOException {
    return;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#thrift-status->kw-status")
  public static TopologyStatus thriftStatusToKwStatus(
      TopologyInitialStatus tStatus) {
    if (tStatus.equals(TopologyInitialStatus.ACTIVE)) {
      return TopologyStatus.ACTIVE;
    } else {
      return TopologyStatus.INACTIVE;
    }
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#start-storm")
  public static void startStorm(NimbusData nimbusData, String stormName,
      String stormId, TopologyStatus topologyInitialStatus) throws Exception {
    StormClusterState stormClusterState = nimbusData.getStormClusterState();
    Map conf = nimbusData.getConf();
    Map stormConf = NimbusUtils.readStormConf(conf, stormId);
    StormTopology topology =
        Common.systemTopology(stormConf,
            NimbusUtils.readStormTopology(conf, stormId));

    Map<String, Object> allComponents = Common.allComponents(topology);
    Map<String, Integer> componentToExecutors =
        Common.numStartExecutors(allComponents);

    int numWorkers =
        CoreUtil.parseInt(stormConf.get(Config.TOPOLOGY_WORKERS), 1);
    String owner =
        CoreUtil.parseString(stormConf.get(Config.TOPOLOGY_SUBMITTER_USER),
            "unknown");
    StormBase stormBase =
        new StormBase(stormName, topologyInitialStatus, numWorkers);
    stormBase.set_launch_time_secs(CoreUtil.current_time_secs());
    stormBase.set_owner(owner);
    stormBase.set_component_executors(componentToExecutors);
    LOG.info("Activating {}:{}", stormName, stormId);
    LOG.info("StormBase: " + stormBase.toString());
    stormClusterState.activateStorm(stormId, stormBase);
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#storm-active?")
  private static boolean isStormActive(StormClusterState stormClusterState,
      String topologyName) throws Exception {
    boolean rtn = false;
    if (Common.getStormId(stormClusterState, topologyName) != null) {
      rtn = true;
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#check-storm-active!")
  public static void checkStormActive(NimbusData nimbus, String stormName,
      boolean isActive) throws Exception {
    if (isStormActive(nimbus.getStormClusterState(), stormName) != isActive) {
      if (isActive) {
        throw new NotAliveException(stormName + " is not alive");
      } else {
        throw new AlreadyAliveException(stormName + " is already active");
      }
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#check-authorization!")
  public static void checkAuthorization(NimbusData nimbus, String stormName,
      Map stormConf, String operation, ReqContext context)
      throws AuthorizationException {
    IAuthorizer aclHandler = nimbus.getAuthorizationHandler();
    IAuthorizer impersonationAuthorizer =
        nimbus.getImpersonationAuthorizationHandler();
    ReqContext ctx = context != null ? context : ReqContext.context();
    Map checkConf = new HashMap();
    if (stormConf != null) {
      checkConf = stormConf;
    } else if (stormName != null) {
      checkConf.put(Config.TOPOLOGY_NAME, stormName);
    }
    LOG.info("[req " + ctx.requestID() + "] Access from: "
        + ctx.remoteAddress() + " principal:" + ctx.principal() + " op:"
        + operation);

    if (ctx.isImpersonating()) {
      LOG.warn("principal: " + ctx.realPrincipal()
          + " is trying to impersonate principal: " + ctx.principal());
      if (impersonationAuthorizer != null) {
        if (!impersonationAuthorizer.permit(ctx, operation, checkConf)) {
          throw new AuthorizationException(
              "principal "
                  + ctx.realPrincipal()
                  + " is not authorized to impersonate principal "
                  + ctx.principal()
                  + " from host "
                  + ctx.remoteAddress()
                  + " Please see SECURITY.MD to learnhow to configure impersonation acls.");
        } else {
          LOG.warn("impersonation attempt but "
              + Config.NIMBUS_IMPERSONATION_AUTHORIZER
              + " has no authorizer configured. potential security risk, please see SECURITY.MD to learn how to configure impersonation authorizer.");
        }
      }
    }

    if (aclHandler != null) {
      if (!aclHandler.permit(ctx, operation, checkConf)) {
        String msg = operation;
        if (stormName != null) {
          msg += " on topology " + stormName;
        }
        msg += " is not authorized";
        throw new AuthorizationException(msg);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#check-authorization!")
  public static void checkAuthorization(NimbusData nimbus, String stormName,
      Map stormConf, String operation) throws AuthorizationException {
    checkAuthorization(nimbus, stormName, stormConf, operation,
        ReqContext.context());
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#do-rebalance")
  public static void doRebalance(NimbusData nimbus, String stormId,
      TopologyStatus status, StormBase stormBase) {
    try {
      RebalanceOptions rebalanceOptions =
          stormBase.get_topology_action_options().get_rebalance_options();
      stormBase.set_topology_action_options(null);
      stormBase.set_component_executors(rebalanceOptions.get_num_executors());
      stormBase.set_num_workers(rebalanceOptions.get_num_workers());

      nimbus.getStormClusterState().updateStorm(stormId, stormBase);
      mkAssignments(nimbus, stormId);
    } catch (Exception e) {
      LOG.error("do-rebalance error!", e);
    }
  }
}
