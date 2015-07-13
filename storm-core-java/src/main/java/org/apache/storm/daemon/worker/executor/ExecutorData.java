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
package org.apache.storm.daemon.worker.executor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.counter.Counters;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.error.ITaskReportErr;
import org.apache.storm.daemon.worker.executor.error.ReportErrorAndDie;
import org.apache.storm.daemon.worker.executor.error.ThrottledReportErrorFn;
import org.apache.storm.daemon.worker.executor.grouping.GroupingUtils;
import org.apache.storm.daemon.worker.executor.grouping.MkGrouper;
import org.apache.storm.daemon.worker.executor.task.Task;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.daemon.worker.stats.CommonStats;
import org.apache.storm.util.EvenSampler;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.messaging.IContext;
import backtype.storm.metric.api.IMetric;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.utils.DisruptorQueue;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-executor-data")
public class ExecutorData implements Serializable {
  private static final long serialVersionUID = 1L;
  private Integer taskId;
  private WorkerData worker;
  private ExecutorInfo executorInfo;
  private WorkerTopologyContext workerContext;
  private List<Integer> taskIds;
  private String componentId;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private DisruptorQueue receiveQueue;
  private ExecutorType executorType;
  private DisruptorQueue batchTransferQueue;
  private clojure.lang.Atom openOrPrepareWasCalled;
  private String stormId;
  private String hostName;
  @SuppressWarnings("rawtypes")
  private Map conf;
  @SuppressWarnings("rawtypes")
  private Map sharedExecutorData;
  private AtomicBoolean stormActiveAtom;
  private StormClusterState stormClusterState;
  private Map<Integer, String> taskToComponent;
  private Map<String, Map<String, MkGrouper>> streamToComponentToGrouper;
  private KryoTupleDeserializer deserializer;
  private IContext context;
  private ExecutorTransferFn transferFn;;
  private ITaskReportErr reportError;
  private ReportErrorAndDie reportErrorAndDie;
  private CommonStats stats;
  private Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry;
  private ExecutorStatus taskStatus;
  private Map<Integer, TaskData> taskDatas;
  private ScheduledExecutorService executorScheduler;
  private Counters counters;
  private EvenSampler sampler;

  @SuppressWarnings("rawtypes")
  public ExecutorData(WorkerData worker, ExecutorInfo executorInfo)
      throws Exception {
    this.taskId = executorInfo.get_task_start();
    this.worker = worker;
    this.workerContext = Common.workerContext(worker);
    this.executorInfo = executorInfo;
    this.taskIds = Common.executorIdToTasks(executorInfo);
    this.componentId = workerContext.getComponentId(taskIds.get(0));
    this.openOrPrepareWasCalled = new clojure.lang.Atom(false);
    this.stormConf =
        ExecutorUtils.normalizedComponentConf(worker.getStormConf(),
            workerContext, componentId);
    this.receiveQueue = worker.getExecutorReceiveQueueMap().get(executorInfo);
    this.stormId = worker.getTopologyId();
    this.hostName = worker.getHostName();
    this.conf = worker.getConf();
    this.sharedExecutorData = new HashMap();
    this.stormActiveAtom = worker.getStormActiveAtom();
    this.batchTransferQueue =
        ExecutorUtils.batchTransferToworker(stormConf, executorInfo);
    this.transferFn = new ExecutorTransferFn(batchTransferQueue);
    // :suicide-fn (:suicide-fn worker)
    this.stormClusterState = worker.getStormClusterState();
    this.executorType = ExecutorUtils.executorType(workerContext, componentId);
    // TODO: should refactor this to be part of the executor specific map (spout
    // or bolt with :common field)
    this.stats =
        ExecutorUtils.mkExecutorStats(executorType,
            ConfigUtil.samplingRate(stormConf));
    this.intervalToTaskToMetricRegistry =
        new HashMap<Integer, Map<Integer, Map<String, IMetric>>>();
    this.taskToComponent = worker.getTasksToComponent();
    this.streamToComponentToGrouper =
        GroupingUtils.outboundComponents(workerContext, componentId);
    this.reportError = new ThrottledReportErrorFn(this);
    // report error and halt worker
    this.reportErrorAndDie =
        new ReportErrorAndDie(reportError, worker.getSuicideFn());
    this.deserializer = new KryoTupleDeserializer(stormConf, workerContext);
    this.sampler = ConfigUtil.mkStatsSampler(stormConf);
    // TODO: add in the executor-specific stuff in a :specific... or make a
    // spout-data, bolt-data function?
    // ///////////////////////
    this.context = worker.getContext();
    this.taskDatas = mkTaskDatas(taskIds);
    this.taskStatus = new ExecutorStatus();
    this.executorScheduler = Executors.newScheduledThreadPool(8);
    this.counters = new Counters();
  }

  private Map<Integer, TaskData> mkTaskDatas(List<Integer> taskIds)
      throws Exception {
    Map<Integer, TaskData> taskDatas = new HashMap<Integer, TaskData>();
    for (Integer taskId : taskIds) {
      TaskData taskData = Task.mkTask(this, taskId);
      taskDatas.put(taskId, taskData);
    }
    return taskDatas;
  }

  public WorkerData getWorker() {
    return worker;
  }

  public void setWorker(WorkerData worker) {
    this.worker = worker;
  }

  public ExecutorInfo getExecutorInfo() {
    return executorInfo;
  }

  public void setExecutorInfo(ExecutorInfo executorInfo) {
    this.executorInfo = executorInfo;
  }

  public WorkerTopologyContext getWorkerContext() {
    return workerContext;
  }

  public void setWorkerContext(WorkerTopologyContext workerContext) {
    this.workerContext = workerContext;
  }

  public List<Integer> getTaskIds() {
    return taskIds;
  }

  public void setTaskIds(List<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  @SuppressWarnings("rawtypes")
  public Map getStormConf() {
    return stormConf;
  }

  @SuppressWarnings("rawtypes")
  public void setStormConf(Map stormConf) {
    this.stormConf = stormConf;
  }

  public ExecutorType getExecutorType() {
    return executorType;
  }

  public void setExecutorType(ExecutorType executorType) {
    this.executorType = executorType;
  }

  public DisruptorQueue getBatchTransferQueue() {
    return batchTransferQueue;
  }

  public void setBatchTransferQueue(DisruptorQueue batchTransferQueue) {
    this.batchTransferQueue = batchTransferQueue;
  }

  public clojure.lang.Atom getOpenOrPrepareWasCalled() {
    return openOrPrepareWasCalled;
  }

  public void setOpenOrPrepareWasCalled(clojure.lang.Atom openOrPrepareWasCalled) {
    this.openOrPrepareWasCalled = openOrPrepareWasCalled;
  }

  public String getStormId() {
    return stormId;
  }

  public void setStormId(String stormId) {
    this.stormId = stormId;
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public void setConf(Map conf) {
    this.conf = conf;
  }

  @SuppressWarnings("rawtypes")
  public Map getSharedExecutorData() {
    return sharedExecutorData;
  }

  @SuppressWarnings("rawtypes")
  public void setSharedExecutorData(Map sharedExecutorData) {
    this.sharedExecutorData = sharedExecutorData;
  }

  public AtomicBoolean getStormActiveAtom() {
    return stormActiveAtom;
  }

  public void setStormActiveAtom(AtomicBoolean stormActiveAtom) {
    this.stormActiveAtom = stormActiveAtom;
  }

  public StormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public void setStormClusterState(StormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public Map<Integer, String> getTaskToComponent() {
    return taskToComponent;
  }

  public void setTaskToComponent(Map<Integer, String> taskToComponent) {
    this.taskToComponent = taskToComponent;
  }

  public KryoTupleDeserializer getDeserializer() {
    return deserializer;
  }

  public void setDeserializer(KryoTupleDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  public IContext getContext() {
    return context;
  }

  public void setContext(IContext context) {
    this.context = context;
  }

  public ExecutorTransferFn getTransferFn() {
    return transferFn;
  }

  public void setTransferFn(ExecutorTransferFn transferFn) {
    this.transferFn = transferFn;
  }

  public Map<Integer, Map<Integer, Map<String, IMetric>>> getIntervalToTaskToMetricRegistry() {
    return intervalToTaskToMetricRegistry;
  }

  public void setIntervalToTaskToMetricRegistry(
      Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry) {
    this.intervalToTaskToMetricRegistry = intervalToTaskToMetricRegistry;
  }

  public Map<String, Map<String, MkGrouper>> getStreamToComponentToGrouper() {
    return streamToComponentToGrouper;
  }

  public void setStreamToComponentToGrouper(
      Map<String, Map<String, MkGrouper>> streamToComponentToGrouper) {
    this.streamToComponentToGrouper = streamToComponentToGrouper;
  }

  public ReportErrorAndDie getReportErrorAndDie() {
    return reportErrorAndDie;
  }

  public void setReportErrorAndDie(ReportErrorAndDie reportErrorAndDie) {
    this.reportErrorAndDie = reportErrorAndDie;
  }

  public DisruptorQueue getReceiveQueue() {
    return receiveQueue;
  }

  public void setReceiveQueue(DisruptorQueue receiveQueue) {
    this.receiveQueue = receiveQueue;
  }

  public ExecutorStatus getTaskStatus() {
    return taskStatus;
  }

  public void setTaskStatus(ExecutorStatus taskStatus) {
    this.taskStatus = taskStatus;
  }

  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public Map<Integer, TaskData> getTaskDatas() {
    return taskDatas;
  }

  public void setTaskDatas(Map<Integer, TaskData> taskDatas) {
    this.taskDatas = taskDatas;
  }

  public CommonStats getStats() {
    return stats;
  }

  public void setStats(CommonStats stats) {
    this.stats = stats;
  }

  public ScheduledExecutorService getExecutorScheduler() {
    return executorScheduler;
  }

  public void setExecutorScheduler(ScheduledExecutorService executorScheduler) {
    this.executorScheduler = executorScheduler;
  }

  public Counters getCounters() {
    return counters;
  }

  public void setCounters(Counters counters) {
    this.counters = counters;
  }

  public ITaskReportErr getReportError() {
    return reportError;
  }

  public void setReportError(ITaskReportErr reportError) {
    this.reportError = reportError;
  }

  public EvenSampler getSampler() {
    return sampler;
  }

  public void setSampler(EvenSampler sampler) {
    this.sampler = sampler;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }
}
