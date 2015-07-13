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
package org.apache.storm.daemon.worker.threads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.WorkerUtils;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.MutableLong;

@ClojureClass(className = "backtype.storm.daemon.worker#mk-refresh-connections")
public class RefreshConnections extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(RefreshConnections.class);
  private WorkerData workerData;
  private AtomicBoolean active;
  private StormClusterState stormClusterState;
  private String stormId;
  private Set<Integer> outboundTasks;
  private HashMap<String, IConnection> cachedNodePortToSocket;
  private IContext context;
  private ReentrantReadWriteLock.WriteLock endpointSocketWriteLock;
  private MutableLong retryCount;
  private int frequence;
  private Exception exception = null;

  public RefreshConnections(WorkerData workerData) throws Exception {
    this.workerData = workerData;
    this.active = workerData.getStormActiveAtom();
    this.stormClusterState = workerData.getStormClusterState();
    this.stormId = workerData.getTopologyId();
    this.outboundTasks = WorkerUtils.workerOutboundTasks(workerData);
    this.cachedNodePortToSocket = workerData.getCachedNodeportToSocket();
    this.context = workerData.getContext();
    this.endpointSocketWriteLock =
        workerData.getEndpointSocketLock().writeLock();
    this.retryCount = new MutableLong(0);
    this.frequence =
        CoreUtil.parseInt(
            workerData.getStormConf().get(Config.TASK_REFRESH_POLL_SECS), 10);

  }

  @Override
  public void run() {
    try {
      Assignment assignment = null;
      int version = stormClusterState.assignmentVersion(stormId, this);
      Assignment oldAssignment =
          workerData.getAssignmentVersions().get(stormId);
      Integer oldVersion = null;
      if (oldAssignment != null) {
        oldVersion = oldAssignment.getVersion();
      }

      if (oldVersion != null && oldVersion.intValue() == version) {
        assignment = oldAssignment;
      } else {
        Assignment newAssignment =
            stormClusterState.assignmentInfoWithVersion(stormId, this);
        workerData.getAssignmentVersions().put(stormId, newAssignment);
        assignment = newAssignment;
      }

      Map<ExecutorInfo, WorkerSlot> executorInfoToNodePort =
          assignment.getExecutorToNodeport();

      Map<Integer, WorkerSlot> taskToNodePort =
          Common.toTaskToNodePort(executorInfoToNodePort);

      HashMap<Integer, String> myAssignment = new HashMap<Integer, String>();
      Map<Integer, WorkerSlot> neededAssignment =
          new ConcurrentHashMap<Integer, WorkerSlot>();
      for (Integer taskId : outboundTasks) {
        if (taskToNodePort.containsKey(taskId)) {
          myAssignment.put(taskId, taskToNodePort.get(taskId).toString());
          neededAssignment.put(taskId, taskToNodePort.get(taskId));
        }
      }

      // we dont need a connection for the local tasks anymore
      Set<Integer> taskIds = workerData.getTaskids();
      for (Integer taskId : taskIds) {
        neededAssignment.remove(taskId);
      }

      Set<WorkerSlot> neededConnections = new HashSet<WorkerSlot>();
      for (WorkerSlot ws : neededAssignment.values()) {
        neededConnections.add(ws);
      }

      Set<Integer> neededTasks = neededAssignment.keySet();

      Set<WorkerSlot> currentConnections = new HashSet<WorkerSlot>();
      Set<String> currentConnectionsStr = cachedNodePortToSocket.keySet();
      for (String nodeToPort : currentConnectionsStr) {
        String[] tmp = nodeToPort.split(":");
        currentConnections.add(new WorkerSlot(tmp[0], Integer.valueOf(tmp[1])));
      }
      Set<WorkerSlot> newConnections =
          CoreUtil.set_difference(neededConnections, currentConnections);

      Set<WorkerSlot> removeConnections =
          CoreUtil.set_difference(currentConnections, neededConnections);

      Map<String, String> nodeToHost = assignment.getNodeHost();

      for (WorkerSlot nodePort : newConnections) {
        String host = nodeToHost.get(nodePort.getNodeId());
        int port = nodePort.getPort();
        IConnection conn = context.connect(stormId, host, port);
        cachedNodePortToSocket.put(nodePort.toString(), conn);
        LOG.info("Add connection to host:{}, port:{} ", host, port);
      }

      endpointSocketWriteLock.lock();
      try {
        workerData.setCachedTaskToNodeport(myAssignment);
      } finally {
        endpointSocketWriteLock.unlock();
      }

      for (WorkerSlot endpoint : removeConnections) {
        String host = nodeToHost.get(endpoint.getNodeId());
        if (host == null) {
          host = endpoint.getNodeId();
        }
        int port = endpoint.getPort();
        IConnection removeConnection = cachedNodePortToSocket.get(endpoint);
        if (removeConnection != null) {
          removeConnection.close();
          LOG.info("Closed Connection to host:{}, port:{} ", host, port);
        }
        cachedNodePortToSocket.remove(endpoint);
        LOG.info("Removed connection to host:{}, port:{} ", host, port);
      }
      workerData.setCachedNodeportToSocket(cachedNodePortToSocket);

      Set<Integer> missingTasks = new HashSet<Integer>();
      for (Integer taskId : neededTasks) {
        if (!myAssignment.containsKey(taskId)) {
          missingTasks.add(taskId);
        }
      }
      if (!missingTasks.isEmpty()) {
        LOG.warn("Missing assignment for following tasks: "
            + missingTasks.toString());
      }
      retryCount = new MutableLong(0);
    } catch (Exception e) {
      LOG.error("Failed to refresh worker Connection",
          CoreUtil.stringifyError(e));
      retryCount.increment();
      if (retryCount.get() >= 3) {
        exception = new RuntimeException(e);
      }
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("Worker Local Heartbeat exception: {}",
          CoreUtil.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public Object getResult() {
    if (this.active.get()) {
      return frequence;
    }
    return -1;
  }
}
