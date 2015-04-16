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
package org.apache.storm.daemon.worker.executor.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.stats.CommonStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;

@ClojureClass(className = "backtype.storm.daemon.task#mk-tasks-fn")
public class TasksFn {
  private Logger LOG = LoggerFactory.getLogger(TasksFn.class);
  private TaskData taskData;
  private ExecutorData executorData;
  private String componentId;
  private WorkerTopologyContext workerContext;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private Map<String, Map<String, MkGrouper>> streamToComponentToGrouper;
  private TopologyContext userContext;
  private CommonStats executorStats;
  private int taskId;
  private boolean isDebug = false;
  private EvenSampler evenSampler;

  public TasksFn(TaskData taskData) {
    this.taskData = taskData;
    this.taskId = taskData.getTaskId();
    this.executorData = taskData.getExecutorData();
    this.componentId = executorData.getComponentId();
    this.workerContext = executorData.getWorkerContext();
    this.stormConf = executorData.getStormConf();
    this.streamToComponentToGrouper =
        executorData.getStreamToComponentToGrouper();
    this.userContext = taskData.getUserContext();
    this.executorStats = executorData.getStats();
    this.evenSampler = ConfigUtils.mkStatsSampler(stormConf);
    this.isDebug =
        ServerUtils.parseBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
  }

  @ClojureClass(className = "backtype.storm.daemon.task#mk-tasks-fn#fn")
  public List<Integer> fn(Integer outTaskId, String stream, List<Object> values) {
    if (outTaskId == null) {
      return new ArrayList<Integer>();
    }
    if (isDebug) {
      LOG.info("Emitting direct: " + outTaskId + "; " + componentId + " "
          + stream + " " + values);
    }
    String targetComponent = workerContext.getComponentId(outTaskId);
    Map<String, MkGrouper> componentToGrouping =
        streamToComponentToGrouper.get(stream);
    if (null == componentToGrouping) {
      LOG.info("Empty grouping, streamId:{}", stream);
      return new ArrayList<Integer>();
    }
    MkGrouper grouping = componentToGrouping.get(targetComponent);
    if (grouping != null
        && !grouping.getGrouptype().equals(GroupingType.direct)) {
      throw new IllegalArgumentException(
          "Cannot emitDirect to a task expecting a regular grouping");
    }

    applyHooks(userContext,
        new EmitInfo(values, stream, taskId, Arrays.asList(outTaskId)));

    if (evenSampler.countCheck()) {
      Stats.emittedTuple(executorStats, stream);
      Stats.transferredTuples(executorStats, stream, 1);
    }

    return Lists.newArrayList(outTaskId);
  }

  @ClojureClass(className = "backtype.storm.daemon.task#mk-tasks-fn#fn")
  public List<Integer> fn(String stream, List<Object> values) {
    if (isDebug) {
      LOG.info("Emitting: " + componentId + " " + stream + " " + values);
    }
    List<Integer> outTasks = new ArrayList<Integer>();
    Map<String, MkGrouper> componentToGrouping =
        streamToComponentToGrouper.get(stream);
    if (componentToGrouping == null) {
      return outTasks;
    }
    for (MkGrouper grouper : componentToGrouping.values()) {
      if (grouper.getGrouptype().equals(GroupingType.direct)) {
        // TODO: this is wrong, need to check how the stream was declared
        throw new IllegalArgumentException(
            "Cannot do regular emit to direct stream");
      }
      outTasks.addAll(grouper.grouper(taskId, values));
    }

    applyHooks(userContext, new EmitInfo(values, stream, taskId, outTasks));

    if (evenSampler.countCheck()) {
      Stats.emittedTuple(executorStats, stream);
      Stats.transferredTuples(executorStats, stream, outTasks.size());
    }

    return outTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks")
  private void applyHooks(TopologyContext topologyContext, EmitInfo emitInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks == null || hooks.isEmpty()) {
      return;
    }
    for (ITaskHook hook : hooks) {
      hook.emit(emitInfo);
    }
  }

  public TaskData getTaskData() {
    return taskData;
  }

  public void setTaskData(TaskData taskData) {
    this.taskData = taskData;
  }

  public ExecutorData getExecutorData() {
    return executorData;
  }

  public void setExecutorData(ExecutorData executorData) {
    this.executorData = executorData;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  public WorkerTopologyContext getWorkerContext() {
    return workerContext;
  }

  public void setWorkerContext(WorkerTopologyContext workerContext) {
    this.workerContext = workerContext;
  }

  @SuppressWarnings("rawtypes")
  public Map getStormConf() {
    return stormConf;
  }

  @SuppressWarnings("rawtypes")
  public void setStormConf(Map stormConf) {
    this.stormConf = stormConf;
  }

  public Map<String, Map<String, MkGrouper>> getStreamToComponentToGrouper() {
    return streamToComponentToGrouper;
  }

  public void setStreamToComponentToGrouper(
      Map<String, Map<String, MkGrouper>> streamToComponentToGrouper) {
    this.streamToComponentToGrouper = streamToComponentToGrouper;
  }

  public TopologyContext getUserContext() {
    return userContext;
  }

  public void setUserContext(TopologyContext userContext) {
    this.userContext = userContext;
  }

  public CommonStats getExecutorStats() {
    return executorStats;
  }

  public void setExecutorStats(CommonStats executorStats) {
    this.executorStats = executorStats;
  }

  public boolean isDebug() {
    return isDebug;
  }

  public void setDebug(boolean isDebug) {
    this.isDebug = isDebug;
  }
}
