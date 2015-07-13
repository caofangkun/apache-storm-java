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
package org.apache.storm.daemon.worker.heartbeat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.storm.ClojureClass;

import backtype.storm.generated.ExecutorInfo;

@ClojureClass(className = "backtype.storm.daemon.common#WorkerHeartbeat")
public class WorkerLocalHeartbeat implements Serializable {

  private static final long serialVersionUID = -914166726205534892L;
  private int timeSecs;
  private String stormId;
  private List<ExecutorInfo> executors;
  private Integer port;
  private String processId;

  public WorkerLocalHeartbeat(int timeSecs, String stormId,
      List<ExecutorInfo> executors, Integer port, String processId) {
    this.timeSecs = timeSecs;
    this.stormId = stormId;
    this.executors = new ArrayList<ExecutorInfo>(executors);
    this.port = port;
    this.processId = processId;

  }

  public int getTimeSecs() {
    return timeSecs;
  }

  public String getStormId() {
    return stormId;
  }

  public List<ExecutorInfo> getTaskIds() {
    return executors;
  }

  public Integer getPort() {
    return port;
  }

  public String toString() {
    return "topologyId:" + stormId + ", timeSecs:" + timeSecs + "("
        + timeStamp2Date(String.valueOf(timeSecs), "yyyy-MM-dd HH:mm:ss") + ")"
        + ", port:" + port + ", processid:" + processId + ", executors:"
        + executors.toString();
  }

  public static String timeStamp2Date(String timestampString, String formats) {
    Long timestamp = Long.parseLong(timestampString) * 1000;
    String date =
        new java.text.SimpleDateFormat(formats).format(new java.util.Date(
            timestamp));
    return date;
  }

  public String getProcessId() {
    return processId;
  }
}
