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
package org.apache.storm.daemon.worker;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.ClusterState;
import org.apache.storm.cluster.DistributedClusterState;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.executor.ExecutorShutdown;
import org.apache.storm.daemon.worker.executor.UptimeComputer;
import org.apache.storm.daemon.worker.transfer.TransferFn;
import org.apache.storm.daemon.worker.transfer.TransferLocalFn;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyStatus;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

@ClojureClass(className = "backtype.storm.daemon.worker#worker-data")
public class WorkerData implements Serializable {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(WorkerData.class);
  @SuppressWarnings("rawtypes")
  private Map conf;
  private IConnection receiver;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private IContext mqContext;
  private final String stormId;
  private final String assignmentId;
  private final String hostName;
  private final Integer port;
  private final String workerId;
  private AtomicBoolean stormActiveAtom;
  private AtomicBoolean workerActiveFlag;
  private TopologyStatus topologyStatus;
  private ClusterState clusterState;
  private StormClusterState stormClusterState;
  private SortedSet<Integer> taskids;
  private volatile HashMap<WorkerSlot, IConnection> cachedNodeportToSocket;
  private volatile ConcurrentHashMap<Integer, WorkerSlot> cachedTaskToNodeport;
  private Set<ExecutorInfo> executors;
  private ConcurrentHashMap<Integer, DisruptorQueue> innerTaskTransfer;
  private Map<Integer, String> tasksToComponent;
  private Map<String, List<Integer>> componentToSortedTasks;
  private Map<String, Object> defaultSharedResources;
  private Map<String, Object> userSharedResources;
  private StormTopology topology;
  private StormTopology systemTopology;
  private DefaultKillFn suicideFn;
  private UptimeComputer uptime;
  private DisruptorQueue transferQueue;
  private List<ExecutorShutdown> shutdownExecutors;
  private String processId;
  private Map<String, Map<String, Fields>> componentToStreamToFields;
  private Map<ExecutorInfo, DisruptorQueue> executorReceiveQueueMap;
  private Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
  private Map<Integer, DisruptorQueue> receiveQueueMap;
  private ReentrantReadWriteLock endpointSocketLock;
  private Map<Integer, Integer> taskToShortExecutor;
  private TransferFn transferFn;
  private WorkerTopologyContext workerContext;
  private TransferLocalFn transferLocalFn;
  private Integer receiverThreadCount;
  private Map<String, Assignment> assignmentVersions;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public WorkerData(Map conf, IContext mqContext, String stormId,
      String assignmentId, int port, String workerId, String processId)
      throws Exception {
    this.conf = conf;
    this.stormConf = ConfigUtil.readSupervisorStormConf(conf, stormId);
    this.mqContext = mkMqContext(mqContext);
    this.receiver = this.mqContext.bind(stormId, port);
    this.stormId = stormId;
    this.assignmentId = assignmentId;
    this.hostName = System.getProperty("storm.local.hostname");
    this.port = port;
    this.workerId = workerId;
    this.processId = processId;
    this.clusterState = new DistributedClusterState(conf);
    this.stormClusterState = new StormZkClusterState(clusterState);
    this.workerActiveFlag = new AtomicBoolean(false);
    this.stormActiveAtom = new AtomicBoolean(false);
    this.topologyStatus = TopologyStatus.ACTIVE;
    this.innerTaskTransfer = new ConcurrentHashMap<Integer, DisruptorQueue>();
    this.executors =
        WorkerUtils.readWorkerExecutors(stormConf, stormClusterState, stormId,
            assignmentId, port);

    this.topology = ConfigUtil.readSupervisorTopology(conf, stormId);
    this.systemTopology = Common.systemTopology(stormConf, topology);
    // for optimized access when used in tasks later on
    this.tasksToComponent = Common.stormTaskInfo(topology, stormConf);
    this.componentToStreamToFields =
        WorkerUtils.componentToStreamToFields(systemTopology);
    this.componentToSortedTasks = mkComponentToSortedTasks(tasksToComponent);
    this.endpointSocketLock = new ReentrantReadWriteLock();
    this.cachedNodeportToSocket =
        new HashMap<WorkerSlot, IConnection>();
    this.cachedTaskToNodeport = new ConcurrentHashMap<Integer, WorkerSlot>();
    this.transferQueue = mkTransferQueue();
    this.executorReceiveQueueMap =
        WorkerUtils.mkReceiveQueueMap(stormConf, executors);
    this.shortExecutorReceiveQueueMap =
        WorkerUtils.mkShortExecutorReceiveQueueMap(executorReceiveQueueMap);
    this.taskToShortExecutor = mkTaskToShortExecutor(executors);
    this.receiveQueueMap = mkTaskReceiveQueueMap(executorReceiveQueueMap);
    this.taskids = new TreeSet(receiveQueueMap.keySet());
    this.suicideFn = mkSuicideFn();
    this.uptime = new UptimeComputer();
    this.defaultSharedResources = WorkerUtils.mkDefaultResources(conf);
    this.userSharedResources = WorkerUtils.mkUserResources(conf);
    this.transferLocalFn = new TransferLocalFn(this);
    this.receiverThreadCount =
        CoreUtil.parseInt(
            stormConf.get(Config.WORKER_RECEIVER_THREAD_COUNT), 1);
    this.transferFn = new TransferFn(this);
    this.assignmentVersions = new HashMap<String, Assignment>();

    LOG.info("Successfully create WorkerData");
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-suicide-fn")
  private DefaultKillFn mkSuicideFn() {
    return new DefaultKillFn(1, "Worker died");
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#transfer-queue")
  private DisruptorQueue mkTransferQueue() {
    int bufferSize =
        CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE), 2048);
    WaitStrategy waitStrategy =
        (WaitStrategy) Utils.newInstance((String) stormConf
            .get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
    return new DisruptorQueue("worker-transfer-queue",
        new MultiThreadedClaimStrategy(bufferSize), waitStrategy);
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#task->short-executor")
  private Map<Integer, Integer> mkTaskToShortExecutor(
      Set<ExecutorInfo> executors) {
    Map<Integer, Integer> results = new HashMap<Integer, Integer>();
    for (ExecutorInfo e : executors) {
      for (Integer t : Common.executorIdToTasks(e)) {
        results.put(t, e.get_task_start());
      }
    }
    return results;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#component->sorted-tasks")
  private Map<String, List<Integer>> mkComponentToSortedTasks(
      Map<Integer, String> tasksToComponent) {
    Map<String, List<Integer>> componentToSortedTasks =
        CoreUtil.reverse_map(tasksToComponent);
    for (Map.Entry<String, List<Integer>> entry : componentToSortedTasks
        .entrySet()) {
      List<Integer> tasks = entry.getValue();
      Collections.sort(tasks);
    }
    return componentToSortedTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#receive-queue-map")
  private Map<Integer, DisruptorQueue> mkTaskReceiveQueueMap(
      Map<ExecutorInfo, DisruptorQueue> executorReceiveQueueMap) {
    Map<Integer, DisruptorQueue> result =
        new HashMap<Integer, DisruptorQueue>();
    for (Map.Entry<ExecutorInfo, DisruptorQueue> eToD : executorReceiveQueueMap
        .entrySet()) {
      ExecutorInfo e = eToD.getKey();
      DisruptorQueue queue = eToD.getValue();
      for (Integer t : Common.executorIdToTasks(e)) {
        result.put(t, queue);
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#:mq-context")
  private IContext mkMqContext(IContext mqContext) {
    if (mqContext == null) {
      mqContext = TransportFactory.makeContext(stormConf);
    }
    return mqContext;
  }

  @SuppressWarnings("rawtypes")
  public WorkerData(Map conf, IContext context, String topologyId,
      String supervisorId, int port, String workerId) throws Exception {
    this(conf, context, topologyId, supervisorId, port, workerId, null);
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  public AtomicBoolean getStormActiveAtom() {
    return stormActiveAtom;
  }

  public void setStormActiveAtom(AtomicBoolean active) {
    this.stormActiveAtom = active;
  }

  public TopologyStatus getTopologyStatus() {
    return topologyStatus;
  }

  public void setTopologyStatus(TopologyStatus topologyStatus) {
    this.topologyStatus = topologyStatus;
  }

  @SuppressWarnings("unchecked")
  public Map<Object, Object> getStormConf() {
    return stormConf;
  }

  public IContext getContext() {
    return mqContext;
  }

  public String getTopologyId() {
    return stormId;
  }

  public String getAssignmentId() {
    return assignmentId;
  }

  public Integer getPort() {
    return port;
  }

  public String getWorkerId() {
    return workerId;
  }

  public ClusterState getZkClusterstate() {
    return clusterState;
  }

  public StormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public Set<Integer> getTaskids() {
    return taskids;
  }

  public HashMap<WorkerSlot, IConnection> getCachedNodeportToSocket() {
    return cachedNodeportToSocket;
  }

  public void setCachedNodeportToSocket(
      HashMap<WorkerSlot, IConnection> cachedNodeportToSocket) {
    this.cachedNodeportToSocket = cachedNodeportToSocket;
  }

  public ConcurrentHashMap<Integer, DisruptorQueue> getInnerTaskTransfer() {
    return innerTaskTransfer;
  }

  public Map<Integer, String> getTasksToComponent() {
    return tasksToComponent;
  }

  public StormTopology getTopology() {
    return topology;
  }

  public StormTopology getSystemTopology() {
    return systemTopology;
  }

  public DefaultKillFn getSuicideFn() {
    return suicideFn;
  }

  public DisruptorQueue getTransferQueue() {
    return transferQueue;
  }

  public Map<String, List<Integer>> getComponentToSortedTasks() {
    return componentToSortedTasks;
  }

  public Map<String, Object> getDefaultSharedResources() {
    return defaultSharedResources;
  }

  public Map<String, Object> getUserSharedResources() {
    return userSharedResources;
  }

  public List<ExecutorShutdown> getShutdownExecutors() {
    return shutdownExecutors;
  }

  public void setShutdownExecutors(List<ExecutorShutdown> shutdownExecutors) {
    this.shutdownExecutors = shutdownExecutors;
  }

  public Set<ExecutorInfo> getExecutors() {
    return executors;
  }

  public void setExecutors(Set<ExecutorInfo> executors) {
    this.executors = executors;
  }

  public AtomicBoolean getWorkerActiveFlag() {
    return workerActiveFlag;
  }

  public void setWorkerActiveFlag(AtomicBoolean workerActiveFlag) {
    this.workerActiveFlag = workerActiveFlag;
  }

  public String getProcessId() {
    return processId;
  }

  public void setProcessId(String processId) {
    this.processId = processId;
  }

  public Map<String, Map<String, Fields>> getComponentToStreamToFields() {
    return componentToStreamToFields;
  }

  public void setComponentToStreamToFields(
      Map<String, Map<String, Fields>> componentToStreamToFields) {
    this.componentToStreamToFields = componentToStreamToFields;
  }

  public Map<ExecutorInfo, DisruptorQueue> getExecutorReceiveQueueMap() {
    return executorReceiveQueueMap;
  }

  public void setExecutorReceiveQueueMap(
      Map<ExecutorInfo, DisruptorQueue> executorReceiveQueueMap) {
    this.executorReceiveQueueMap = executorReceiveQueueMap;
  }

  public ConcurrentHashMap<Integer, WorkerSlot> getCachedTaskToNodeport() {
    return cachedTaskToNodeport;
  }

  public void setCachedTaskToNodeport(
      ConcurrentHashMap<Integer, WorkerSlot> cachedTaskToNodeport) {
    this.cachedTaskToNodeport = cachedTaskToNodeport;
  }

  public Map<Integer, Integer> getTaskToShortExecutor() {
    return taskToShortExecutor;
  }

  public void setTaskToShortExecutor(Map<Integer, Integer> taskToShortExecutor) {
    this.taskToShortExecutor = taskToShortExecutor;
  }

  public Map<Integer, DisruptorQueue> getShortExecutorReceiveQueueMap() {
    return shortExecutorReceiveQueueMap;
  }

  public void setShortExecutorReceiveQueueMap(
      Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap) {
    this.shortExecutorReceiveQueueMap = shortExecutorReceiveQueueMap;
  }

  public TransferFn getTransferFn() {
    return transferFn;
  }

  public void setTransferFn(TransferFn transferFn) {
    this.transferFn = transferFn;
  }

  public TransferLocalFn getTransferLocalFn() {
    return transferLocalFn;
  }

  public void setTransferLocalFn(TransferLocalFn transferLocalFn) {
    this.transferLocalFn = transferLocalFn;
  }

  public UptimeComputer getUptime() {
    return uptime;
  }

  public void setUptime(UptimeComputer uptime) {
    this.uptime = uptime;
  }

  public Integer getReceiverThreadCount() {
    return receiverThreadCount;
  }

  public void setReceiverThreadCount(Integer receiverThreadCount) {
    this.receiverThreadCount = receiverThreadCount;
  }

  public ReentrantReadWriteLock getEndpointSocketLock() {
    return endpointSocketLock;
  }

  public void setEndpointSocketLock(ReentrantReadWriteLock endpointSocketLock) {
    this.endpointSocketLock = endpointSocketLock;
  }

  public Map<String, Assignment> getAssignmentVersions() {
    return assignmentVersions;
  }

  public void setAssignmentVersions(Map<String, Assignment> assignmentVersions) {
    this.assignmentVersions = assignmentVersions;
  }

  public WorkerTopologyContext getWorkerContext() {
    return workerContext;
  }

  public void setWorkerContext(WorkerTopologyContext workerContext) {
    this.workerContext = workerContext;
  }

  public String getHostName() {
    return hostName;
  }

  public IConnection getReceiver() {
    return receiver;
  }

  public void setReceiver(IConnection receiver) {
    this.receiver = receiver;
  }
}
