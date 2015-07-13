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

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.daemon.nimbus.transitions.StatusType;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class StormStatus implements Serializable {

  private static final long serialVersionUID = -1L;
  private StatusType type;
  private int killTimeSecs;
  private int delaySecs;
  private int numWorkers;
  private StormStatus oldStatus = null;
  // component->executors
  private Map<String, Integer> executorOverrides;

  public StormStatus(StatusType type, int delaySecs, StormStatus oldStatus,
      int numWorkers, Map<String, Integer> executorOverrides) {
    // rebalance-transition
    this.type = type;
    this.delaySecs = delaySecs;
    this.oldStatus = oldStatus;
    this.numWorkers = numWorkers;
    this.executorOverrides = executorOverrides;
  }

  public StormStatus(int killTimeSecs, StatusType type, StormStatus oldStatus) {
    this.type = type;
    this.killTimeSecs = killTimeSecs;
    this.oldStatus = oldStatus;
  }

  public StormStatus(int killTimeSecs, int numWorkers, StatusType type,
      StormStatus oldStatus) {
    this.type = type;
    this.killTimeSecs = killTimeSecs;
    this.numWorkers = numWorkers;
    this.oldStatus = oldStatus;
  }

  public StormStatus(int killTimeSecs, StatusType type) {
    // kill-transition
    this.type = type;
    this.killTimeSecs = killTimeSecs;
  }

  public StormStatus(StatusType type, int delaySecs, StormStatus oldStatus) {
    this.type = type;
    this.delaySecs = delaySecs;
    this.oldStatus = oldStatus;
  }

  public StormStatus(StatusType type) {
    this.type = type;
    this.killTimeSecs = -1;
    this.delaySecs = -1;
  }

  public StatusType getStatusType() {
    return type;
  }

  public void setStatusType(StatusType type) {
    this.type = type;
  }

  public Integer getKillTimeSecs() {
    return killTimeSecs;
  }

  public void setKillTimeSecs(int killTimeSecs) {
    this.killTimeSecs = killTimeSecs;
  }

  public Integer getDelaySecs() {
    return delaySecs;
  }

  public void setDelaySecs(int delaySecs) {
    this.delaySecs = delaySecs;
  }

  public StormStatus getOldStatus() {
    return oldStatus;
  }

  public void setOldStatus(StormStatus oldStatus) {
    this.oldStatus = oldStatus;
  }

  @Override
  public boolean equals(Object base) {
    if ((base instanceof StormStatus) == false) {
      return false;
    }

    StormStatus check = (StormStatus) base;
    if (check.getStatusType().equals(getStatusType())
        && check.getKillTimeSecs() == getKillTimeSecs()
        && check.getDelaySecs().equals(getDelaySecs())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getStatusType().hashCode() + this.getKillTimeSecs().hashCode()
        + this.getDelaySecs().hashCode();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public void setNumWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
  }

  public StatusType getType() {
    return type;
  }

  public void setType(StatusType type) {
    this.type = type;
  }

  public Map<String, Integer> getExecutorOverrides() {
    return executorOverrides;
  }

  public void setExecutorOverrides(Map<String, Integer> executorOverrides) {
    this.executorOverrides = executorOverrides;
  }

}
