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
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.supervisor.event.EventManager;
import org.apache.storm.guava.collect.Sets;
import org.apache.storm.localstate.LocalStateUtil;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.LocalAssignment;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.MutableLong;

@ClojureClass(className = "backtype.storm.daemon.supervisor#mk-synchronize-supervisor")
public class SynchronizeSupervisorEvent extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(SynchronizeSupervisorEvent.class);
  private String supervisorId;
  private EventManager processesEventManager;
  private StormClusterState stormClusterState;
  private LocalState localState;
  @SuppressWarnings("rawtypes")
  private Map conf;
  private SyncProcessesEvent syncProcesses;
  private SupervisorData supervisorData;
  private ISupervisor isupervisor;;
  private RunnableCallback syncCallback;
  private MutableLong retryCount;
  private Object downloadLock;

  public SynchronizeSupervisorEvent(SupervisorData supervisorData,
      SyncProcessesEvent syncProcesses, EventManager processesEventManager,
      EventManager eventManager) {
    this.conf = supervisorData.getConf();
    this.stormClusterState = supervisorData.getStormClusterState();
    this.isupervisor = supervisorData.getIsupervisor();
    this.localState = supervisorData.getLocalState();
    this.supervisorData = supervisorData;
    this.syncProcesses = syncProcesses;
    this.processesEventManager = processesEventManager;
    this.supervisorId = supervisorData.getSupervisorId();
    this.syncCallback = new SyncCallback(this, eventManager);
    this.retryCount = new MutableLong(0);
    this.downloadLock = supervisorData.getDownloadLock();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    LOG.debug("Synchronizing supervisor");

    if (!supervisorData.getActive().get()) {
      return;
    }

    try {
      Map<String, Assignment> assignmentsSnapshot =
          SupervisorUtils.assignmentsSnapshot(stormClusterState, syncCallback,
              supervisorData.getAssignmentVersions());
      Map<String, String> stormCodeMap =
          SupervisorUtils.readStormCodeLocations(assignmentsSnapshot);
      LOG.debug("Storm code map: " + stormCodeMap.toString());

      Set<String> downloadedStormIds =
          SupervisorUtils.readDownloadedStormIds(conf);
      LOG.debug("Downloaded storm ids: " + downloadedStormIds);

      Map<Integer, LocalAssignment> existingAssignment =
          (Map<Integer, LocalAssignment>) localState
              .get(Common.LS_LOCAL_ASSIGNMENTS);

      Map<Integer, LocalAssignment> allAssignment =
          SupervisorUtils.readAssignments(assignmentsSnapshot, supervisorId,
              existingAssignment, supervisorData.getSyncRetry());
      LOG.debug("All assignment: " + allAssignment.toString());

      Map<Integer, LocalAssignment> newAssignment =
          new HashMap<Integer, LocalAssignment>();
      for (Integer port : allAssignment.keySet()) {
        if (isupervisor.confirmAssigned(port)) {
          newAssignment.put(port, allAssignment.get(port));
        }
      }
      LOG.debug("New assignment: " + newAssignment.toString());

      Set<String> assignedStormIds =
          SupervisorUtils.assignedStormIdsFromPortAssignments(newAssignment);

      // download code first
      // This might take awhile
      // -- should this be done separately from usual monitoring?
      // should we only download when topology is assigned to this supervisor?
      for (Map.Entry<String, String> entry : stormCodeMap.entrySet()) {
        String stormId = entry.getKey();
        String masterCodeDir = entry.getValue();
        if (!downloadedStormIds.contains(stormId)
            && assignedStormIds.contains(stormId)) {
          LOG.info("Downloading code for storm id " + stormId + " from "
              + masterCodeDir);
          SupervisorUtils.downloadStormCode(conf, stormId, masterCodeDir,
              downloadLock);
          LOG.info("Finished downloading code for storm id " + stormId
              + " from " + masterCodeDir);
        }
      }

      LOG.debug("Writing new local assignment " + newAssignment.toString());
      Set<Integer> differencePorts = new HashSet<Integer>();
      if (existingAssignment != null) {
        differencePorts =
            Sets.difference(existingAssignment.keySet(),
                newAssignment.keySet());
      }
      for (Integer port : differencePorts) {
        isupervisor.killedWorker(port);
      }
      isupervisor.assigned(newAssignment.keySet());
      LocalStateUtil.lsLocalAssignments(localState, newAssignment);
      supervisorData.setAssignmentVersions(assignmentsSnapshot);
      supervisorData.setCurrAssignment(newAssignment);

      // remove any downloaded code that's no longer assigned or active
      // important that this happens after setting the local assignment so that
      // synchronize-supervisor doesn't try to launch workers for which the
      // resources don't exist

      for (String stormId : downloadedStormIds) {
        if (!assignedStormIds.contains(stormId)) {
          LOG.info("Removing code for storm id {} ", stormId);
          String path = null;
          try {
            path = ConfigUtil.supervisorStormdistRoot(conf, stormId);
            CoreUtil.rmr(path);
          } catch (Exception e) {
            String errMsg = "rmr the path:" + path + "failed\n";
            LOG.error(errMsg + CoreUtil.stringifyError(e));
          }
        }
      }

      processesEventManager.add(syncProcesses);
      retryCount = new MutableLong(0);
    } catch (Exception e) {
      LOG.error("Failed to Sync Supervisor", CoreUtil.stringifyError(e));
      retryCount.increment();
      if (retryCount.get() >= 3) {
        throw new RuntimeException(e);
      }
    }
  }
}
