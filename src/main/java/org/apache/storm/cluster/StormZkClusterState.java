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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.apache.storm.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.common.Assignment;
import backtype.storm.daemon.common.StormBase;
import backtype.storm.daemon.common.SupervisorInfo;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state")
public class StormZkClusterState implements StormClusterState {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory
      .getLogger(StormZkClusterState.class);
  private ClusterState clusterState;
  private ConcurrentHashMap<String, RunnableCallback> assignmentInfoCallback;
  private ConcurrentHashMap<String, RunnableCallback> assignmentInfoWithVersionCallback;
  private ConcurrentHashMap<String, RunnableCallback> assignmentVersionCallback;
  private AtomicReference<RunnableCallback> supervisorsCallback;
  private AtomicReference<RunnableCallback> assignmentsCallback;
  private ConcurrentHashMap<String, RunnableCallback> stormBaseCallback;
  private ConcurrentHashMap<String, RunnableCallback> credentialsCallback;

  private String stateId;
  private List<ACL> acls;
  private boolean solo;

  public StormZkClusterState(Object clusterStateSpec) throws Exception {
    this(clusterStateSpec, null);
  }

  @SuppressWarnings("rawtypes")
  public StormZkClusterState(Object clusterStateSpec, List<ACL> acls)
      throws Exception {
    // Watches should be used for optimization. When ZK is reconnecting, they're
    // not guaranteed to be called.

    this.acls = acls;
    if (clusterStateSpec instanceof ClusterState) {
      solo = false;
      clusterState = (ClusterState) clusterStateSpec;
    } else {
      solo = true;
      Map stormConf = (Map) clusterStateSpec;
      clusterState = new DistributedClusterState(stormConf);
    }
    assignmentInfoCallback = new ConcurrentHashMap<String, RunnableCallback>();
    assignmentInfoWithVersionCallback =
        new ConcurrentHashMap<String, RunnableCallback>();
    assignmentVersionCallback =
        new ConcurrentHashMap<String, RunnableCallback>();
    supervisorsCallback = new AtomicReference<RunnableCallback>(null);
    assignmentsCallback = new AtomicReference<RunnableCallback>(null);
    stormBaseCallback = new ConcurrentHashMap<String, RunnableCallback>();
    credentialsCallback = new ConcurrentHashMap<String, RunnableCallback>();
    stateId = clusterState.register(new ClusterStateCallback() {

      @Override
      public <T> Object execute(T... vals) {

        if (vals == null || vals.length != 2) {
          return null;
        }

        @SuppressWarnings("unused")
        EventType type = (EventType) vals[0];
        String path = (String) vals[1];

        List<String> toks = ServerUtils.tokenizePath(path);
        int size = toks.size();
        if (size < 1) {
          return null;
        }

        String args = null;
        String subtree = toks.get(0);
        RunnableCallback fn = null;
        if (subtree.equals(Cluster.ASSIGNMENTS_ROOT)) {
          if (size == 1) {
            // set null and get the old value
            fn = issueCallback(assignmentsCallback);
          } else {
            args = toks.get(1);
            fn = issueMapCallback(assignmentInfoCallback, args);
          }
        } else if (subtree.equals(Cluster.SUPERVISORS_ROOT)) {
          fn = issueCallback(supervisorsCallback);

        } else if (subtree.equals(Cluster.STORMS_ROOT) && size > 1) {
          args = toks.get(1);
          fn = issueMapCallback(stormBaseCallback, args);

        } else if (subtree.equals(Cluster.CREDENTIALS_ROOT) && size > 1) {
          args = toks.get(1);
          fn = issueMapCallback(credentialsCallback, args);

        } else {
          // this should never happen
          CoreUtil.exitProcess(30, "Unknown callback for subtree " + path);
        }
        if (fn != null) {
          fn.run();
        }
        return null;
      }
    });
    String[] pathlist =
        new String[] { Cluster.ASSIGNMENTS_SUBTREE, Cluster.STORMS_SUBTREE,
            Cluster.SUPERVISORS_SUBTREE, Cluster.WORKERBEATS_SUBTREE,
            Cluster.ERRORS_SUBTREE, Cluster.USER_SUBTREE };
    for (String path : pathlist) {
      clusterState.mkdirs(path, acls);
    }
  }

  @ClojureClass(className = "backtype.storm.cluster#issue-callback!")
  private RunnableCallback issueCallback(
      AtomicReference<RunnableCallback> cbAtom) {
    RunnableCallback cb = cbAtom.get();
    cbAtom.set(null);
    if (cb != null) {
      return cb;
    }
    return null;

  }

  @ClojureClass(className = "backtype.storm.cluster#issue-map-callback!")
  private RunnableCallback issueMapCallback(
      ConcurrentHashMap<String, RunnableCallback> cbAtom, String id) {
    RunnableCallback cb = cbAtom.get(id);
    cbAtom.remove(id);
    if (cb != null) {
      return cb;
    }
    return null;

  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#assignments")
  public Set<String> assignments(RunnableCallback callback) throws Exception {
    if (callback != null) {
      assignmentsCallback.set(callback);
    }
    Set<String> assignments =
        clusterState.getChildren(Cluster.ASSIGNMENTS_SUBTREE, callback != null);
    return assignments;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#assignment-info")
  public Assignment assignmentInfo(String stormId, RunnableCallback callback)
      throws Exception {
    if (callback != null) {
      assignmentInfoCallback.put(stormId, callback);
    }
    String assgnmentPath = Cluster.assignmentPath(stormId);
    byte[] znodeData = clusterState.getData(assgnmentPath, callback != null);
    return Cluster.maybeDeserialize(znodeData, Assignment.class);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#assignment-info-with-version")
  public Assignment assignmentInfoWithVersion(String stormId,
      RunnableCallback callback) throws Exception {
    if (callback != null) {
      assignmentInfoWithVersionCallback.put(stormId, callback);
    }
    String assgnmentPath = Cluster.assignmentPath(stormId);
    DataInfo dataInfo =
        clusterState.getDataWithVersion(assgnmentPath, callback != null);
    if (dataInfo == null) {
      return null;
    }
    Assignment data =
        Cluster.maybeDeserialize(dataInfo.getData(), Assignment.class);
    if (data == null) {
      return null;
    }
    data.setVersion(dataInfo.getVersion());
    return data;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#assignment-version")
  public int assignmentVersion(String stormId, RunnableCallback callback)
      throws Exception {
    if (callback != null) {
      assignmentVersionCallback.put(stormId, callback);
    }
    String assgnmentPath = Cluster.assignmentPath(stormId);
    return clusterState.getVersion(assgnmentPath, callback != null);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#active-storms")
  public Set<String> activeStorms() throws Exception {
    Set<String> active_storms =
        clusterState.getChildren(Cluster.STORMS_SUBTREE, false);
    return active_storms;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#heartbeat-storms")
  public Set<String> heartbeatStorms() throws Exception {
    Set<String> heartbeat_storms =
        clusterState.getChildren(Cluster.WORKERBEATS_SUBTREE, false);
    return heartbeat_storms;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#error-topologies")
  public Set<String> errorTopologies() throws Exception {
    Set<String> error_topologies =
        clusterState.getChildren(Cluster.ERRORS_SUBTREE, false);
    return error_topologies;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#get-worker-heartbeat")
  public WorkerHeartbeats getWorkerHeartbeat(String stormId, String node,
      Integer port) throws Exception {
    String workerbeatPath = Cluster.workerbeatPath(stormId, node, port);
    byte[] znodeData = clusterState.getData(workerbeatPath, false);
    return Cluster.maybeDeserialize(znodeData, WorkerHeartbeats.class);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#executor-beats")
  public Map<ExecutorInfo, ExecutorHeartbeat> executorBeats(String stormId,
      Map<ExecutorInfo, WorkerSlot> executorToNodePort) throws Exception {
    // need to take executor->node+port in explicitly so that we don't run into
    // a situation where a long dead worker with a skewed clock overrides all
    // the timestamps. By only checking heartbeats with an assigned node+port,
    // and only reading executors from that heartbeat that are actually
    // assigned, we avoid situations like that
    Map<ExecutorInfo, ExecutorHeartbeat> ret =
        new HashMap<ExecutorInfo, ExecutorHeartbeat>();
    Map<WorkerSlot, List<ExecutorInfo>> reverseMap =
        ServerUtils.reverse_map(executorToNodePort);
    for (Map.Entry<WorkerSlot, List<ExecutorInfo>> ews : reverseMap.entrySet()) {
      WorkerSlot nodeToPort = ews.getKey();
      List<ExecutorInfo> executors = ews.getValue();
      WorkerHeartbeats whbs =
          this.getWorkerHeartbeat(stormId, nodeToPort.getNodeId(),
              nodeToPort.getPort());
      Map<ExecutorInfo, ExecutorHeartbeat> cebs =
          convertExecutorBeats(executors, whbs);
      ret.putAll(cebs);
    }

    return ret;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#supervisors")
  public Set<String> supervisors(RunnableCallback callback) throws Exception {
    if (callback != null) {
      supervisorsCallback.set(callback);
    }
    Set<String> supervisors =
        clusterState.getChildren(Cluster.SUPERVISORS_SUBTREE, callback != null);
    return supervisors;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#supervisor-info")
  public SupervisorInfo supervisorInfo(String supervisorId) throws Exception {
    String supervisorPath = Cluster.supervisorPath(supervisorId);
    byte[] znodeData = clusterState.getData(supervisorPath, false);
    return Cluster.maybeDeserialize(znodeData, SupervisorInfo.class);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#worker-heartbeat!")
  public void workerHeartbeat(String stormId, String node, Integer port,
      WorkerHeartbeats info) throws Exception {
    String workerbeatPath = Cluster.workerbeatPath(stormId, node, port);
    byte[] data = Utils.serialize(info);
    clusterState.setData(workerbeatPath, data, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#remove-worker-heartbeat!")
  public void removeWorkerHeartbeat(String stormId, String node, Integer port)
      throws Exception {
    String taskbeatPath = Cluster.workerbeatPath(stormId, node, port);
    clusterState.deleteNode(taskbeatPath);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#setup-heartbeats!")
  public void setupHeartbeats(String topologyId) throws Exception {
    String taskbeatPath = Cluster.workerbeatStormRoot(topologyId);
    clusterState.mkdirs(taskbeatPath, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#teardown-heartbeats!")
  public void teardownHeartbeats(String topologyId) throws Exception {
    try {
      String heartbeats = Cluster.workerbeatStormRoot(topologyId);
      clusterState.deleteNode(heartbeats);
    } catch (Exception e) {
      throw new Exception("Could not teardown heartbeats for " + topologyId, e);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#teardown-topology-errors!")
  public void teardownTopologyErrors(String stormId) {
    try {
      String taskerrPath = Cluster.errorStormRoot(stormId);
      clusterState.deleteNode(taskerrPath);
    } catch (Exception e) {
      LOG.error("Could not teardown errors for " + stormId, e);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#supervisor-heartbeat!")
  public void supervisorHeartbeat(String supervisorId, SupervisorInfo info)
      throws Exception {
    String supervisorPath = Cluster.supervisorPath(supervisorId);
    byte[] infoData = Utils.serialize(info);
    clusterState.setEphemeralNode(supervisorPath, infoData, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#activate-storm!")
  public void activateStorm(String stormId, StormBase stormBase)
      throws Exception {
    String stormPath = Cluster.stormPath(stormId);
    byte[] stormBaseData = Utils.serialize(stormBase);
    clusterState.setData(stormPath, stormBaseData, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#update-storm!")
  public void updateStorm(String stormId, StormBase newElems) throws Exception {
    StormBase base = this.stormBase(stormId, null);
    if (base != null) {
      if (newElems.get_status() != null) {
        base.set_status(newElems.get_status());
      }

      if (newElems.get_num_workers() > 0) {
        base.set_num_workers(newElems.get_num_workers());
      }

      if (newElems.get_component_executors() != null) {
        Map<String, Integer> executors = base.get_component_executors();
        executors.putAll(newElems.get_component_executors());
        base.set_component_executors(executors);
      }
      LOG.debug("Update StormBase: " + base.toString());
      clusterState.setData(Cluster.stormPath(stormId), Utils.serialize(base),
          acls);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#storm-base")
  public StormBase stormBase(String stormId, RunnableCallback callback)
      throws Exception {
    if (callback != null) {
      stormBaseCallback.put(stormId, callback);
    }
    return Cluster.maybeDeserialize(
        clusterState.getData(Cluster.stormPath(stormId), callback != null),
        StormBase.class);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#remove-storm-base!")
  public void removeStormBase(String stormId) throws Exception {
    clusterState.deleteNode(Cluster.stormPath(stormId));
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#set-assignment!")
  public void setAssignment(String stormId, Assignment info) throws Exception {
    clusterState.setData(Cluster.assignmentPath(stormId),
        Utils.serialize(info), acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#remove-storm!")
  public void removeStorm(String stormId) throws Exception {
    removeAssignment(stormId);
    removeStormBase(stormId);
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#set-credentials!")
  public void setCredentials(String stormId, Credentials creds, Map topoConf)
      throws Exception {
    List<ACL> topoAcls = Cluster.mkTopoOnlyAcls(topoConf);
    String path = Cluster.credentialsPath(stormId);
    clusterState.setData(path, Utils.serialize(creds), topoAcls);
  }

  @SuppressWarnings("unchecked")
  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#credentials")
  public Credentials credentials(String stormId, RunnableCallback callback)
      throws Exception {
    if (callback != null) {
      credentialsCallback.put(stormId, callback);
    }
    return Cluster.maybeDeserialize(clusterState.getData(
        Cluster.credentialsPath(stormId), callback != null), Credentials.class);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#report-error")
  public void reportError(String topologyId, String componentId, String node,
      int port, Throwable error) throws Exception {
    String path = Cluster.errorPath(topologyId, componentId);
    ErrorInfo data =
        new ErrorInfo(CoreUtil.stringifyError(error),
            CoreUtil.current_time_secs());
    data.set_host(node);
    data.set_port(port);

    String errorPath =
        path + Cluster.ZK_SEPERATOR + CoreUtil.current_time_secs();
    clusterState.mkdirs(path, acls);
    clusterState.createSequential(errorPath, Utils.serialize(data), acls);

    List<String> children = new ArrayList<String>();
    for (String str : clusterState.getChildren(path, false)) {
      children.add(str);
    }
    Collections.sort(children);
    while (children.size() > 10) {
      clusterState.deleteNode(path + Cluster.ZK_SEPERATOR + children.remove(0));
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#errors")
  public List<ErrorInfo> errors(String stormId, String componentId)
      throws Exception {
    String path = Cluster.errorPath(stormId, componentId);
    clusterState.mkdirs(path, acls);
    Set<String> children = clusterState.getChildren(path, false);
    List<ErrorInfo> errors = new ArrayList<ErrorInfo>();
    for (String str : children) {
      byte[] v = clusterState.getData(path + "/" + str, false);
      if (v != null) {
        ErrorInfo error = Cluster.maybeDeserialize(v, ErrorInfo.class);
        errors.add(error);
      }
    }
    Collections.sort(errors, new Comparator<ErrorInfo>() {
      @Override
      public int compare(ErrorInfo o1, ErrorInfo o2) {
        if (o1.get_error_time_secs() > o2.get_error_time_secs()) {
          return 1;
        }
        if (o1.get_error_time_secs() < o2.get_error_time_secs()) {
          return -1;
        }
        return 0;
      }
    });
    return errors;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#last-error")
  public ErrorInfo lastError(String stormId, String componentId)
      throws Exception {
    ErrorInfo lastErrorInfo = new ErrorInfo();
    String path = Cluster.lastErrorPath(stormId, componentId);
    if (clusterState.existsNode(path, false)) {
      byte[] v = clusterState.getData(path, false);
      if (v != null) {
        lastErrorInfo = Cluster.maybeDeserialize(v, ErrorInfo.class);
      }
    }
    return lastErrorInfo;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-storm-cluster-state#disconnect")
  public void disconnect() {
    clusterState.unregister(stateId);
    if (solo == true) {
      clusterState.close();
    }
  }

  @Override
  public LeaderSelector getLeaderSelector(String path,
      LeaderSelectorListener listener) throws Exception {
    LeaderSelector selector = clusterState.mkLeaderSelector(path, listener);
    return selector;
  }

  @Override
  public void registerLeaderHost(NodeInfo serverInfo) throws Exception {
    if (serverInfo != null) {
      clusterState.setData(Cluster.MASTER_SUBTREE, Utils.serialize(serverInfo),
          acls);
    } else {
      LOG.error("register host to zk fail!");
      throw new Exception();
    }
  }

  @Override
  public NodeInfo getLeaderHost() throws Exception {
    byte[] zk_data = clusterState.getData(Cluster.MASTER_SUBTREE, false);
    if (zk_data == null) {
      return null;
    }
    return Utils.deserialize(zk_data, NodeInfo.class);
  }

  @Override
  public void registerUIServerInfo(NodeInfo serverInfo) throws Exception {
    if (serverInfo != null) {
      clusterState.setData(Cluster.UI_SUBTREE, Utils.serialize(serverInfo),
          acls);
    } else {
      LOG.error("register host to zk fail!");
      throw new Exception();
    }
  }

  @Override
  public NodeInfo getUIServerInfo() throws Exception {
    byte[] zk_data = clusterState.getData(Cluster.UI_SUBTREE, false);
    if (zk_data == null) {
      return null;
    }
    return Utils.deserialize(zk_data, NodeInfo.class);
  }

  @Override
  public boolean leaderExisted() throws Exception {
    boolean existed = clusterState.existsNode(Cluster.MASTER_SUBTREE, false);
    return existed;
  }

  @Override
  public List<String> user_ids() throws Exception {
    String userBeanRoot = Cluster.user_bean_root();
    Set<String> list = clusterState.getChildren(userBeanRoot, false);
    List<String> rtn = new ArrayList<String>();
    for (String str : list) {
      rtn.add(str);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.cluster#convert-executor-beats")
  private Map<ExecutorInfo, ExecutorHeartbeat> convertExecutorBeats(
      List<ExecutorInfo> executors, WorkerHeartbeats workerHb) {
    // ensures that we only return heartbeats for executors assigned to
    // this worker
    Map<ExecutorInfo, ExecutorHeartbeat> ret =
        new HashMap<ExecutorInfo, ExecutorHeartbeat>();
    if (workerHb != null) {
      Map<ExecutorInfo, StatsData> executorStats = workerHb.getExecutorStats();
      for (ExecutorInfo t : executors) {
        if (executorStats.containsKey(t)) {
          ExecutorHeartbeat eht =
              new ExecutorHeartbeat(workerHb.getTimeSecs(),
                  workerHb.getUptime(), executorStats.get(t));
          ret.put(t, eht);
        }
      }
    }
    return ret;
  }

  @Override
  public void removeAssignment(String stormId) throws Exception {
    String assignmentPath = Cluster.assignmentPath(stormId);
    clusterState.deleteNode(assignmentPath);
  }
}