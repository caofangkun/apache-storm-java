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
package org.apache.storm.daemon.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.ClojureClass;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.scheduler.WorkerSlot;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.common#Assignment")
public class Assignment implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String masterCodeDir;
  private final Map<String, String> nodeHost;
  private final Map<ExecutorInfo, Integer> executorToStartTimeSecs;
  private final Map<ExecutorInfo, WorkerSlot> executorToNodeport;
  private String user;
  private int version;

  /**
   * 
   * @param masterCodeDir
   * @param executorToNodeport
   * @param nodeHost node->host is here so that tasks know who to talk to just
   *          from assignment this avoid situation where node goes down and task
   *          doesn't know what to do information-wise
   * @param executorToStartTimeSecs
   */
  public Assignment(String masterCodeDir,
      Map<ExecutorInfo, WorkerSlot> executorToNodeport,
      Map<String, String> nodeHost,
      Map<ExecutorInfo, Integer> executorToStartTimeSecs) {
    this(masterCodeDir, executorToNodeport, nodeHost, executorToStartTimeSecs,
        "notknown", 0);
  }

  public Assignment(String masterCodeDir,
      Map<ExecutorInfo, WorkerSlot> executorToNodeport,
      Map<String, String> nodeHost,
      Map<ExecutorInfo, Integer> executorToStartTimeSecs, int version) {
    this(masterCodeDir, executorToNodeport, nodeHost, executorToStartTimeSecs,
        "notknown", version);
  }

  public Assignment(String masterCodeDir,
      Map<ExecutorInfo, WorkerSlot> executorToNodeport,
      Map<String, String> nodeHost,
      Map<ExecutorInfo, Integer> executorToStartTimeSecs, String user) {
    this(masterCodeDir, executorToNodeport, nodeHost, executorToStartTimeSecs,
        user, 0);
  }

  public Assignment(String masterCodeDir,
      Map<ExecutorInfo, WorkerSlot> executorToNodeport,
      Map<String, String> nodeHost,
      Map<ExecutorInfo, Integer> executorToStartTimeSecs, String user,
      int version) {
    this.executorToNodeport = executorToNodeport;
    this.nodeHost = nodeHost;
    this.executorToStartTimeSecs = executorToStartTimeSecs;
    this.masterCodeDir = masterCodeDir;
    this.user = user;
  }

  public Map<String, String> getNodeHost() {
    return nodeHost;
  }

  public Map<ExecutorInfo, Integer> getExecutorToStartTimeSecs() {
    return executorToStartTimeSecs;
  }

  public String getMasterCodeDir() {
    return masterCodeDir;
  }

  public Map<ExecutorInfo, WorkerSlot> getExecutorToNodeport() {
    return executorToNodeport;
  }

  /**
   * find taskToResource for every supervisorId (node)
   * 
   * @param supervisorId
   * @return Map<Integer, WorkerSlot>
   */
  public Map<ExecutorInfo, WorkerSlot> getTaskToPortbyNode(String supervisorId) {
    Map<ExecutorInfo, WorkerSlot> taskToPortbyNode =
        new HashMap<ExecutorInfo, WorkerSlot>();
    if (executorToNodeport == null) {
      return null;
    }
    for (Entry<ExecutorInfo, WorkerSlot> entry : executorToNodeport.entrySet()) {
      String node = entry.getValue().getNodeId();
      if (node.equals(supervisorId)) {
        taskToPortbyNode.put(entry.getKey(), entry.getValue());
      }
    }
    return taskToPortbyNode;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result
            + ((masterCodeDir == null) ? 0 : masterCodeDir.hashCode());
    result = prime * result + ((nodeHost == null) ? 0 : nodeHost.hashCode());
    result =
        prime
            * result
            + ((executorToStartTimeSecs == null) ? 0 : executorToStartTimeSecs
                .hashCode());
    result =
        prime
            * result
            + ((executorToNodeport == null) ? 0 : executorToNodeport.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Assignment)) {
      return false;
    }
    Assignment assignment = (Assignment) other;
    return (this.masterCodeDir.equals(assignment.masterCodeDir))
        && (this.nodeHost.equals(assignment.nodeHost))
        && this.executorToNodeport.equals(assignment.executorToNodeport)
        && this.executorToStartTimeSecs
            .equals(assignment.executorToStartTimeSecs);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
