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
package org.apache.storm.daemon.supervisor.sync;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.supervisor.ShutdownWork;
import org.apache.storm.daemon.supervisor.StateHeartbeat;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.worker.WorkerStatus;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeat;
import org.apache.storm.localstate.LocalStateUtil;
import org.apache.storm.util.CoreConfig;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.MutableLong;
import backtype.storm.utils.Time;

/**
 * 1. to kill are those in allocated that are dead or disallowed
 * 
 * 2. kill the ones that should be dead
 * 
 * - read pids, kill -9 and individually remove file
 * 
 * - rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log)
 * 
 * 3. of the rest, figure out what assignments aren't yet satisfied
 * 
 * 4. generate new worker ids, write new "approved workers" to LS
 * 
 * 5. create local dir for worker id
 * 
 * 6. launch new workers (give worker-id, port, and supervisor-id)
 * 
 * 7. wait for workers launch
 * 
 * @author caokun
 *
 */
@ClojureClass(className = "backtype.storm.daemon.supervisor#sync-processes")
public class SyncProcessesEvent extends ShutdownWork {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(SyncProcessesEvent.class);
  @SuppressWarnings("rawtypes")
  private Map conf;
  private ConcurrentHashMap<String, String> workerThreadPids;
  private String supervisorId;
  private SupervisorData supervisorData;
  private MutableLong retryCount;
  private Object downloadLock;
  private StormClusterState stormClusterState;
  private Map<Integer, Integer> realToVirtualPort;


  public SyncProcessesEvent(SupervisorData supervisorData) throws Exception {
    this.supervisorData = supervisorData;
    this.conf = supervisorData.getConf();
    this.retryCount = new MutableLong(0);
    this.downloadLock = supervisorData.getDownloadLock();
    this.stormClusterState = supervisorData.getStormClusterState();
    this.realToVirtualPort =
        (Map<Integer, Integer>) conf.get(CoreConfig.STORM_REAL_VIRTUAL_PORTS);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {

    if (!supervisorData.getActive().get()) {
      return;
    }

    try {
      LocalState localState = supervisorData.getLocalState();
      Map<Integer, LocalAssignment> assignedExecutors =
          (Map<Integer, LocalAssignment>) localState
              .get(Common.LS_LOCAL_ASSIGNMENTS);
      if (assignedExecutors == null) {
        assignedExecutors = new HashMap<Integer, LocalAssignment>();
      }
      int now = CoreUtil.current_time_secs();
      Map<String, StateHeartbeat> allocated =
          SupervisorUtils.readAllocatedWorkers(supervisorData,
              assignedExecutors, now);
      Map<String, StateHeartbeat> keepers =
          filterKeepers(allocated, WorkerStatus.valid);

      Set<Integer> keepPorts = mkPorts(keepers);
      Map<Integer, LocalAssignment> reassignExecutors =
          CoreUtil.select_keys_pred(keepPorts, assignedExecutors);
      Map<Integer, String> newWorkerIds = new HashMap<Integer, String>();
      for (Integer port : reassignExecutors.keySet()) {
        String workerId = UUID.randomUUID().toString();
        newWorkerIds.put(port, workerId);
      }
      LOG.debug("Syncing processes");
      LOG.debug("Assigned executors: {} ", assignedExecutors.toString());
      LOG.debug("Allocated: {} ", allocated.toString());
      LOG.debug("keepPorts:" + keepPorts.toString());
      LOG.debug("newWorkerIds:" + newWorkerIds.toString());

      // 1.to kill are those in allocated that are dead or disallowed
      killDeadOrDisallowedWorkers(allocated, now);
      startNewWorkers(newWorkerIds, reassignExecutors, allocated, keepers);
      retryCount = new MutableLong(0);
    } catch (Exception e) {
      LOG.error("Failed Sync Process", CoreUtil.stringifyError(e));
      retryCount.increment();
      if (retryCount.get() >= 3) {
        throw new RuntimeException(e);
      }
    }
  }

  private Set<Integer> mkPorts(Map<String, StateHeartbeat> keepers) {
    Set<Integer> keepPorts = new HashSet<Integer>();
    for (Map.Entry<String, StateHeartbeat> f : keepers.entrySet()) {
      WorkerLocalHeartbeat hb = f.getValue().getHeartbeat();
      keepPorts.add(hb.getPort());
    }
    return keepPorts;
  }

  private Map<String, StateHeartbeat> filterKeepers(
      Map<String, StateHeartbeat> allocated, WorkerStatus filterStatus) {
    Map<String, StateHeartbeat> keepers = new HashMap<String, StateHeartbeat>();
    for (Map.Entry<String, StateHeartbeat> f : allocated.entrySet()) {
      WorkerStatus status = f.getValue().getState();
      if (status == filterStatus) {
        keepers.put(f.getKey(), f.getValue());
      }
    }
    return keepers;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#wait-for-workers-launch")
  public void waitForWorkersLaunch(Map conf,
      Map<Integer, String> portToWorkerIds) throws IOException,
      InterruptedException {
    int startTime = CoreUtil.current_time_secs();
    for (Map.Entry<Integer, String> entry : portToWorkerIds.entrySet()) {
      waitForWorkerLaunch(conf, entry.getKey(), entry.getValue(), startTime);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#wait-for-worker-launch")
  public void waitForWorkerLaunch(Map conf, Integer workerPort,
      String workerId, int startTime) throws IOException, InterruptedException {
    LocalState state = ConfigUtil.workerState(conf, workerId);
    int supervisorWorkerStartTimeOutSesc =
        CoreUtil.parseInt(
            conf.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS), 120);
    WorkerLocalHeartbeat hb = null;
    while (true) {
      hb = (WorkerLocalHeartbeat) state.get(Common.LS_WORKER_HEARTBEAT);
      if (hb == null
          && ((CoreUtil.current_time_secs() - startTime) < supervisorWorkerStartTimeOutSesc)) {
        LOG.info("Worker {}:{} still hasn't started", workerId, workerPort);
        Time.sleep(500);
      } else {
        // whb is valid or timeout
        break;
      }
    }

    if ((WorkerLocalHeartbeat) state.get(Common.LS_WORKER_HEARTBEAT) != null) {
      LOG.info("Successfully start worker {}:{} ", workerId, workerPort);
    }
  }

  private void killDeadOrDisallowedWorkers(
      Map<String, StateHeartbeat> allocated, int now) {
    if (allocated.size() == 0) {
      return;
    }
    for (Entry<String, StateHeartbeat> entry : allocated.entrySet()) {
      String id = entry.getKey();// workerid
      StateHeartbeat stateToHeartbeat = entry.getValue();
      WorkerStatus state = stateToHeartbeat.getState();
      WorkerLocalHeartbeat heartbeat = stateToHeartbeat.getHeartbeat();

      if (!state.equals(WorkerStatus.valid)) {
        Object objs[] =
            { id, now, state.toString(),
                heartbeat == null ? "null" : heartbeat.toString() };
        LOG.info(
            "Shutting down and clearing state for id {}. Current supervisor time: {}. State: {}, Heartbeat: {} ",
            objs);
        try {
          // 2. kill the ones that should be dead
          shutdownWorker(supervisorData, id);
        } catch (IOException e) {
          String errMsg =
              "Failed to shutdown worker workId:" + id + ",supervisorId: "
                  + supervisorId + ",workerThreadPids:" + workerThreadPids;
          LOG.error(errMsg, e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void startNewWorkers(Map<Integer, String> newWorkerIds,
      Map<Integer, LocalAssignment> reassignExecutors,
      Map<String, StateHeartbeat> allocated, Map<String, StateHeartbeat> keepers)
      throws Exception {
    if (newWorkerIds.size() == 0) {
      return;
    }
    for (String workerId : newWorkerIds.values()) {
      try {
        ConfigUtil.workerPidsRoot(conf, workerId);
      } catch (IOException e1) {
        LOG.error("Failed to create " + workerId + " localdir", e1);
        throw e1;
      }
    }
    LocalState localState = supervisorData.getLocalState();
    Map<String, Integer> newWorkers = new HashMap<String, Integer>();
    Map<String, Integer> tmp =
        (Map<String, Integer>) localState.get(Common.LS_APPROVED_WORKERS);
    if (tmp != null) {
      for (String keeperWorkerId : keepers.keySet()) {
        Integer keeperPort = tmp.get(keeperWorkerId);
        if (keeperPort != null) {
          newWorkers.put(keeperWorkerId, keeperPort);
        }
      }
    }
    for (Map.Entry<Integer, String> entry : newWorkerIds.entrySet()) {
      newWorkers.put(entry.getValue(), entry.getKey());
    }
    
    LocalStateUtil.lsApprovedWorkers(localState, newWorkers);
    // check storm toplolgy code dir exists before launching workers
    downloadCodeIfNotExisted(reassignExecutors);
    for (Map.Entry<Integer, LocalAssignment> entry : reassignExecutors
        .entrySet()) {
      int realPort = entry.getKey();
      int virtualPort = realPort;
      if (realToVirtualPort.containsKey(realPort)) {
        virtualPort = realToVirtualPort.get(realPort);
        LOG.info("RealPort-->VirtualPort: " + realPort + ":" + virtualPort);
      }
      LocalAssignment assignment = entry.getValue();
      String id = newWorkerIds.get(realPort);// workerid
      Object objs[] =
          { assignment.toString(), supervisorData.getSupervisorId(), realPort, id };
      LOG.info(
          "Launching worker with assignment {} for this supervisor {} on port {} with id {}",
          objs);
      SupervisorUtils.launchWorker(conf, supervisorData,
          assignment.get_topology_id(), realPort, id);
    }

    try {
      waitForWorkersLaunch(conf, newWorkerIds);
    } catch (IOException e) {
      LOG.error(e + " waitForWorkersLaunch failed");
    } catch (InterruptedException e) {
      LOG.error(e + " waitForWorkersLaunch failed");
    }
  }

  private void downloadCodeIfNotExisted(
      Map<Integer, LocalAssignment> reassignExecutors) throws Exception {
    Set<String> downloadedStormIds =
        SupervisorUtils.readDownloadedStormIds(conf);
    Map<String, Assignment> cachedAssignmentInfo =
        supervisorData.getAssignmentVersions();
    for (Map.Entry<Integer, LocalAssignment> assignment : reassignExecutors
        .entrySet()) {
      String stormId = assignment.getValue().get_topology_id();
      Assignment assignmentInfo = null;
      if (null != cachedAssignmentInfo
          && cachedAssignmentInfo.containsKey(stormId)) {
        assignmentInfo = cachedAssignmentInfo.get(stormId);
      } else {
        assignmentInfo =
            stormClusterState.assignmentInfoWithVersion(stormId, null);
      }

      String masterCodeDir = assignmentInfo.getMasterCodeDir();
      String stormRoot = ConfigUtil.supervisorStormdistRoot(conf, stormId);
      if (!(downloadedStormIds.contains(stormId)
          || CoreUtil.existsFile(stormRoot) || null == masterCodeDir)) {
        SupervisorUtils.downloadStormCode(conf, stormId, masterCodeDir,
            downloadLock);
      }
      // SupervisorUtils.readStormCodeLocations(assignmentInfo);
    }
  }
}
