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
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.ClojureClass;
import backtype.storm.generated.ExecutorInfo;

import com.tencent.jstorm.stats.StatsData;

@ClojureClass(className = "backtype.storm.daemon.worker#do-executor-heartbeats:zk-hb")
public class WorkerHeartbeats implements Serializable {
  private static final long serialVersionUID = -6369195955255963810L;
  private String stormId;
  private Integer timeSecs;
  private Integer uptime;
  private Map<ExecutorInfo, StatsData> executorStats; // taskid-->task-stats

  public WorkerHeartbeats(String stormId,
      Map<ExecutorInfo, StatsData> executorStats, int uptime, int timeSecs) {
    this.stormId = stormId;
    this.timeSecs = timeSecs;
    this.uptime = uptime;
    this.executorStats = executorStats;
  }

  public int getTimeSecs() {
    return timeSecs;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public void setTimeSecs(int timeSecs) {
    this.timeSecs = timeSecs;
  }

  public int getUptime() {
    return uptime;
  }

  public void setUptime(int uptimeSecs) {
    this.uptime = uptimeSecs;
  }

  public String getStormId() {
    return stormId;
  }

  public void setStormId(String stormId) {
    this.stormId = stormId;
  }

  /*
   * public Map<ExecutorInfo, Counters> getExecutorCounters() { return
   * executorCounters; }
   * 
   * public void setExecutorCounters(Map<ExecutorInfo, Counters>
   * executorCounters) { this.executorCounters = executorCounters; }
   */

  public Map<ExecutorInfo, StatsData> getExecutorStats() {
    return executorStats;
  }

  public void setExecutorStats(Map<ExecutorInfo, StatsData> stats) {
    this.executorStats = stats;
  }

  @Override
  public boolean equals(Object hb) {
    if (hb instanceof WorkerHeartbeats
        && ((WorkerHeartbeats) hb).timeSecs.equals(timeSecs)
        && ((WorkerHeartbeats) hb).uptime.equals(uptime)
        && ((WorkerHeartbeats) hb).executorStats.equals(executorStats)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return timeSecs.hashCode() + uptime.hashCode() + executorStats.hashCode();
  }
}
