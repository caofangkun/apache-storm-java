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

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.ExecutorData;

import backtype.storm.task.TopologyContext;


@ClojureClass(className = "backtype.storm.daemon.task#mk-task-data")
public class TaskData {
  private ExecutorData executorData;
  private Integer taskId;
  private TopologyContext systemContext;
  private TopologyContext userContext;
  private String componentId;
  // private Map<String, IMetric> builtinMetrics;
  private TasksFn tasksFn;
  private Object object;

  public TaskData(ExecutorData executorData, Integer taskId) throws Exception {
    this.executorData = executorData;
    this.taskId = taskId;
    this.systemContext =
        TaskUtils.systemTopologyContext(executorData.getWorker(), executorData,
            taskId);
    this.userContext =
        TaskUtils.userTopologyContext(executorData.getWorker(), executorData,
            taskId);
    this.componentId = executorData.getComponentId();
    this.tasksFn = new TasksFn(this);
    // this.builtinMetrics =
    // BuiltinMetrics.mkData(executorData.getExecutorType());
    this.object =
        TaskUtils.getTaskObject(systemContext.getRawTopology(), componentId);
  }

  public ExecutorData getExecutorData() {
    return executorData;
  }

  public void setExecutorData(ExecutorData executorData) {
    this.executorData = executorData;
  }

  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public Object getObject() {
    return object;
  }

  public void setObject(Object object) {
    this.object = object;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  public TopologyContext getSystemContext() {
    return systemContext;
  }

  public void setSystemContext(TopologyContext systemContext) {
    this.systemContext = systemContext;
  }

  public TopologyContext getUserContext() {
    return userContext;
  }

  public void setUserContext(TopologyContext userContext) {
    this.userContext = userContext;
  }

  public TasksFn getTasksFn() {
    return tasksFn;
  }

  public void setTasksFn(TasksFn tasksFn) {
    this.tasksFn = tasksFn;
  }

  // public Map<String, IMetric> getBuiltinMetrics() {
  // return builtinMetrics;
  // }
  //
  // public void setBuiltinMetrics(Map<String, IMetric> builtinMetrics) {
  // this.builtinMetrics = builtinMetrics;
  // }

}
