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
package org.apache.storm.daemon.supervisor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.worker.executor.UptimeComputer;
import org.apache.storm.util.CoreUtil;

import backtype.storm.Config;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.generated.NodeInfo;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.LocalState;

@ClojureClass(className = "backtype.storm.daemon.supervisor#supervisor-data")
public class SupervisorData implements Serializable {
  private static final long serialVersionUID = 1L;
  @SuppressWarnings("rawtypes")
  private Map conf;
  private IContext sharedContext;
  private ISupervisor isupervisor;
  private AtomicBoolean active;
  private UptimeComputer uptime;
  private ConcurrentHashMap<String, String> workerThreadPids;
  private StormClusterState stormClusterState;
  private LocalState localState;
  private String supervisorId;
  private String assignmentId;
  private String myHostname;
  private Map<Integer, LocalAssignment> currAssignment;
  private volatile Map<String, Assignment> assignmentVersions;
  private AtomicInteger syncRetry;
  private Object downloadLock;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public SupervisorData(Map conf, IContext sharedContext,
      ISupervisor isupervisor) throws Exception {
    this.conf = conf;

    this.isupervisor = isupervisor;
    this.active = new AtomicBoolean(true);
    this.uptime = new UptimeComputer();
    this.workerThreadPids = new ConcurrentHashMap<String, String>();
    this.stormClusterState = new StormZkClusterState(conf);
    NodeInfo serverInfo = stormClusterState.getLeaderHost();
    if (serverInfo != null) {
      conf.put(Config.NIMBUS_HOST, serverInfo.get_node());
      conf.put(Config.NIMBUS_THRIFT_PORT, serverInfo.get_port().toArray()[0]);
    }
    this.localState = ConfigUtil.supervisorState(conf);
    this.supervisorId = isupervisor.getSupervisorId();
    this.assignmentId = isupervisor.getAssignmentId();
    this.sharedContext = sharedContext;
    this.myHostname =
        CoreUtil.parseString(conf.get(Config.STORM_LOCAL_HOSTNAME),
            CoreUtil.localHostname());
    conf.put(Config.STORM_LOCAL_HOSTNAME, myHostname);
    // used for reporting used ports when heartbeating
    this.currAssignment = new HashMap<Integer, LocalAssignment>();
    this.assignmentVersions = new HashMap<String, Assignment>();
    this.syncRetry = new AtomicInteger(0);
    this.downloadLock = new Object();
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  public IContext getSharedContext() {
    return sharedContext;
  }

  public ISupervisor getIsupervisor() {
    return isupervisor;
  }

  public AtomicBoolean getActive() {
    return active;
  }

  public UptimeComputer getUptime() {
    return uptime;
  }

  public StormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public LocalState getLocalState() {
    return localState;
  }

  public String getSupervisorId() {
    return supervisorId;
  }

  public String getAssignmentId() {
    return assignmentId;
  }

  public String getMyHostname() {
    return myHostname;
  }

  public void setConf(Map<Object, Object> conf) {
    this.conf = conf;
  }

  public void setSharedContext(IContext sharedContext) {
    this.sharedContext = sharedContext;
  }

  public void setIsupervisor(ISupervisor isupervisor) {
    this.isupervisor = isupervisor;
  }

  public void setActive(AtomicBoolean active) {
    this.active = active;
  }

  public void setUptime(UptimeComputer uptime) {
    this.uptime = uptime;
  }

  public void setStormClusterState(StormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public void setLocalState(LocalState localState) {
    this.localState = localState;
  }

  public void setSupervisorId(String supervisorId) {
    this.supervisorId = supervisorId;
  }

  public void setAssignmentId(String assignmentId) {
    this.assignmentId = assignmentId;
  }

  public void setMyHostname(String hostname) {
    this.myHostname = hostname;
  }

  public ConcurrentHashMap<String, String> getWorkerThreadPids() {
    return workerThreadPids;
  }

  public void setWorkerThreadPids(
      ConcurrentHashMap<String, String> workerThreadPids) {
    this.workerThreadPids = workerThreadPids;
  }

  public Map<Integer, LocalAssignment> getCurrAssignment() {
    return currAssignment;
  }

  public void setCurrAssignment(Map<Integer, LocalAssignment> currAssignment) {
    this.currAssignment = currAssignment;
  }

  public Map<String, Assignment> getAssignmentVersions() {
    return assignmentVersions;
  }

  public void setAssignmentVersions(Map<String, Assignment> assignmentVersions) {
    this.assignmentVersions = assignmentVersions;
  }

  public AtomicInteger getSyncRetry() {
    return syncRetry;
  }

  public void setSyncRetry(AtomicInteger syncRetry) {
    this.syncRetry = syncRetry;
  }

  public Object getDownloadLock() {
    return downloadLock;
  }

  public void setDownloadLock(Object downloadLock) {
    this.downloadLock = downloadLock;
  }

}
