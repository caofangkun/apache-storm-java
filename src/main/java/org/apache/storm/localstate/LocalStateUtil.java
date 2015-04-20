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
package org.apache.storm.localstate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeat;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.LSApprovedWorkers;
import backtype.storm.generated.LSSupervisorAssignments;
import backtype.storm.generated.LSSupervisorId;
import backtype.storm.generated.LSWorkerHeartbeat;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.utils.LocalState;

@ClojureClass(className = "backtype.storm.local-state")
public class LocalStateUtil {

  @ClojureClass(className = "backtype.storm.local-state#LS-WORKER-HEARTBEAT")
  public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
  @ClojureClass(className = "backtype.storm.local-state#LS-ID")
  public static final String LS_ID = "supervisor-id";
  @ClojureClass(className = "backtype.storm.local-state#LS-LOCAL-ASSIGNMENTS")
  public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
  @ClojureClass(className = "backtype.storm.local-state#LS-APPROVED-WORKERS")
  public static final String LS_APPROVED_WORKERS = "approved-workers";

  @ClojureClass(className = "backtype.storm.local-state#ls-supervisor-id!")
  public static void lsSupervisorId(LocalState localState, String id) {
    localState.put(LS_ID, new LSSupervisorId(id));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-supervisor-id")
  public static String getLsSupervisorId(LocalState localState) {
    LSSupervisorId superId = (LSSupervisorId) localState.get(LS_ID);
    return superId.get_supervisor_id();
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-approved-workers!")
  public static void lsApprovedWorkers(LocalState localState,
      Map<String, Integer> workers) {
    localState.put(LS_APPROVED_WORKERS, new LSApprovedWorkers(workers));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-approved-workers")
  public static Map<String, Integer> getLsApprovedWorkers(LocalState localState) {
    LSApprovedWorkers approvedWorkers =
        (LSApprovedWorkers) localState.get(LS_APPROVED_WORKERS);
    return approvedWorkers.get_approved_workers();
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-local-assignments!")
  public static void lsLocalAssignments(LocalState localState,
      Map<Integer, LocalAssignment> assignments) {
    localState.put(LS_LOCAL_ASSIGNMENTS, new LSSupervisorAssignments(
        assignments));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-local-assignments")
  public static Map<Integer, LocalAssignment> getLsLocalAssignments(
      LocalState localState) {
    LSSupervisorAssignments thriftLocalAssignments =
        (LSSupervisorAssignments) localState.get(LS_LOCAL_ASSIGNMENTS);
    return thriftLocalAssignments.get_assignments();
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-worker-heartbeat!")
  public static void lsWorkerHeartbeat(LocalState localState, int timeSecs,
      String stormId, List<ExecutorInfo> executors, int port) {
    localState.put(LS_WORKER_HEARTBEAT, new LSWorkerHeartbeat(timeSecs,
        stormId, executors, port));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-worker-heartbeat")
  public static WorkerLocalHeartbeat getLsWorkerHeartbeat(LocalState localState) {
    LSWorkerHeartbeat workerHb =
        (LSWorkerHeartbeat) localState.get(LS_WORKER_HEARTBEAT);
    Set<ExecutorInfo> executors = new HashSet<ExecutorInfo>();
    for (ExecutorInfo executor : workerHb.get_executors()) {
      executors.add(executor);
    }
    return new WorkerLocalHeartbeat(workerHb.get_time_secs(),
        workerHb.get_topology_id(), executors, workerHb.get_port(), "");
  }
}
