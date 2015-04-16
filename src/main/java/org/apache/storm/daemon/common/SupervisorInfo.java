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
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.ClojureClass;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.common#SupervisorInfo")
public class SupervisorInfo implements Serializable {
  private static final long serialVersionUID = 1L;
  private Integer timeSecs;
  private final String hostName;
  private String assignmentId;
  private Set<Integer> usedPorts;
  private Object meta;
  @SuppressWarnings("rawtypes")
  private Map schedulerMeta;
  private Integer uptimeSecs;
  @SuppressWarnings("rawtypes")
  private Map supervisorConf;

  @SuppressWarnings("rawtypes")
  public SupervisorInfo(int timeSecs, String hostName, String assignmentId,
      Set<Integer> usedPorts, Object meta, Map schedulerMeta, int uptimeSecs,
      Map supervisorConf) {
    this.timeSecs = timeSecs;
    this.hostName = hostName;
    this.assignmentId = assignmentId;
    this.usedPorts = usedPorts;
    this.meta = meta;
    this.schedulerMeta = schedulerMeta;
    this.uptimeSecs = uptimeSecs;
    this.supervisorConf = supervisorConf;
  }

  public String getHostName() {
    return hostName;
  }

  public int getTimeSecs() {
    return timeSecs;
  }

  public void setTimeSecs(int timeSecs) {
    this.timeSecs = timeSecs;
  }

  public Set<Integer> getUsedPorts() {
    return usedPorts;
  }

  public void setUsedPorts(Set<Integer> usedPorts) {
    this.usedPorts = usedPorts;
  }

  public int getUptimeSecs() {
    return uptimeSecs;
  }

  public void setUptimeSecs(int uptimeSecs) {
    this.uptimeSecs = uptimeSecs;
  }

  @Override
  public boolean equals(Object hb) {
    if (hb instanceof SupervisorInfo
        && ((SupervisorInfo) hb).timeSecs.equals(timeSecs)
        && ((SupervisorInfo) hb).hostName.equals(hostName)
        && ((SupervisorInfo) hb).usedPorts.equals(usedPorts)
        && ((SupervisorInfo) hb).uptimeSecs.equals(uptimeSecs)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return timeSecs.hashCode() + uptimeSecs.hashCode() + hostName.hashCode()
        + usedPorts.hashCode();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  /**
   * get Map<supervisorId, hostname>
   * 
   * @param stormClusterState
   * @param callback
   * @return
   */
  public static Map<String, String> getNodeHost(
      Map<String, SupervisorInfo> supInfos) {
    Map<String, String> rtn = new HashMap<String, String>();
    for (Entry<String, SupervisorInfo> entry : supInfos.entrySet()) {
      SupervisorInfo superinfo = entry.getValue();
      String supervisorid = entry.getKey();
      rtn.put(supervisorid, superinfo.getHostName());
    }
    return rtn;
  }

  public String getAssignmentId() {
    return assignmentId;
  }

  public void setAssignmentId(String assignmentId) {
    this.assignmentId = assignmentId;
  }

  @SuppressWarnings("rawtypes")
  public Map getSupervisorConf() {
    return supervisorConf;
  }

  @SuppressWarnings("rawtypes")
  public void setSupervisorConf(Map supervisorConf) {
    this.supervisorConf = supervisorConf;
  }

  public Object getMeta() {
    return meta;
  }

  public void setMeta(Object meta) {
    this.meta = meta;
  }

  @SuppressWarnings("rawtypes")
  public Map getSchedulerMeta() {
    return schedulerMeta;
  }

  @SuppressWarnings("rawtypes")
  public void setSchedulerMeta(Map schedulerMeta) {
    this.schedulerMeta = schedulerMeta;
  }
}