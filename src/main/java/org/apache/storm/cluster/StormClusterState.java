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

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.SupervisorInfo;
import org.apache.storm.daemon.worker.executor.heartbeat.ExecutorHeartbeat;
import org.apache.storm.daemon.worker.heartbeat.WorkerHeartbeats;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.generated.Credentials;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.NodeInfo;
import backtype.storm.generated.StormBase;
import backtype.storm.scheduler.WorkerSlot;


@ClojureClass(className = "backtype.storm.cluster#StormClusterState")
public interface StormClusterState extends Serializable {

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#assignments")
  public Set<String> assignments(RunnableCallback callback) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#assignment-info")
  public Assignment assignmentInfo(String stormId, RunnableCallback callback)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#assignment-info-with-version")
  public Assignment assignmentInfoWithVersion(String stormId,
      RunnableCallback callback) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#assignment-version")
  public int assignmentVersion(String stormId, RunnableCallback callback)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#active-storms")
  public Set<String> activeStorms() throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#storm-base")
  public StormBase stormBase(String stormId, RunnableCallback callback)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#get-worker-heartbeat")
  public WorkerHeartbeats getWorkerHeartbeat(String stormId, String node,
      Integer port) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#executor-beats")
  public Map<ExecutorInfo, ExecutorHeartbeat> executorBeats(String stormId,
      Map<ExecutorInfo, WorkerSlot> executorToNodePort) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#supervisors")
  public Set<String> supervisors(RunnableCallback callback) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#supervisor-info")
  public SupervisorInfo supervisorInfo(String supervisorId) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#setup-heartbeats!")
  public void setupHeartbeats(String stormId) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#teardown-heartbeats!")
  public void teardownHeartbeats(String stormId) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#teardown-topology-errors!")
  public void teardownTopologyErrors(String stormId) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#heartbeat-storms")
  public Set<String> heartbeatStorms() throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#error-topologies")
  public Set<String> errorTopologies() throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#worker-heartbeat!")
  public void workerHeartbeat(String stormId, String node, Integer port,
      WorkerHeartbeats info) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#remove-worker-heartbeat!")
  public void removeWorkerHeartbeat(String stormId, String node, Integer port)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#supervisor-heartbeat!")
  public void supervisorHeartbeat(String supervisorId, SupervisorInfo info)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#activate-storm!")
  public void activateStorm(String stormId, StormBase storm_base)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#update-storm!")
  public void updateStorm(String stormId, StormBase newElems) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#remove-storm-base!")
  public void removeStormBase(String storm_id) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#set-assignment!")
  public void setAssignment(String stormId, Assignment info) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#remove-storm!")
  public void removeStorm(String stormId) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#report-error")
  public void reportError(String stormId, String componentId, String node,
      int port, Throwable error) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#errors")
  public List<ErrorInfo> errors(String stormId, String componentId)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#last-error")
  public ErrorInfo lastError(String stormId, String componentId)
      throws Exception;

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#set-credentials!")
  public void setCredentials(String stormId, Credentials creds, Map topoConf)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#credentials")
  public Credentials credentials(String stormId, RunnableCallback callback)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#StormClusterState#disconnect")
  public void disconnect() throws Exception;

  public LeaderSelector getLeaderSelector(String path,
      LeaderSelectorListener listener) throws Exception;

  public void registerLeaderHost(NodeInfo bs) throws Exception;

  public NodeInfo getLeaderHost() throws Exception;

  public void registerUIServerInfo(NodeInfo bs) throws Exception;

  public NodeInfo getUIServerInfo() throws Exception;

  public boolean leaderExisted() throws Exception;

  public List<String> user_ids() throws Exception;

  public void removeAssignment(String stormId) throws Exception;
}
