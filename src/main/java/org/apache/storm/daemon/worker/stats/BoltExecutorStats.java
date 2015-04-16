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
package org.apache.storm.daemon.worker.stats;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.rolling.RollingWindowSet;

@ClojureClass(className = "backtype.storm.stats#BoltExecutorStats")
public class BoltExecutorStats extends CommonStats {
  private static final long serialVersionUID = 1L;
  private RollingWindowSet acked;
  private RollingWindowSet failed;
  private RollingWindowSet process_latencies;
  private RollingWindowSet executed;
  private RollingWindowSet execute_latencies;
  protected Map<StatsFields, RollingWindowSet> boltFieldsMap =
      new HashMap<StatsFields, RollingWindowSet>();

  @ClojureClass(className = "backtype.storm.stats#mk-bolt-stats")
  public BoltExecutorStats(Integer samplerate) {
    super(samplerate);
    this.acked =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.failed =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.process_latencies =
        Stats.keyedAvgRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.executed =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.execute_latencies =
        Stats.keyedAvgRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    boltFieldsMap.put(StatsFields.acked, acked);
    boltFieldsMap.put(StatsFields.failed, failed);
    boltFieldsMap.put(StatsFields.process_latencies, process_latencies);
    boltFieldsMap.put(StatsFields.executed, executed);
    boltFieldsMap.put(StatsFields.execute_latencies, execute_latencies);
  }

  public RollingWindowSet getAcked() {
    return acked;
  }

  public void setAcked(RollingWindowSet acked) {
    this.acked = acked;
  }

  public RollingWindowSet getFailed() {
    return failed;
  }

  public void setFailed(RollingWindowSet failed) {
    this.failed = failed;
  }

  public RollingWindowSet getProcess_latencies() {
    return process_latencies;
  }

  public void setProcess_latencies(RollingWindowSet process_latencies) {
    this.process_latencies = process_latencies;
  }

  public RollingWindowSet getExecuted() {
    return executed;
  }

  public void setExecuted(RollingWindowSet executed) {
    this.executed = executed;
  }

  public RollingWindowSet getExecute_latencies() {
    return execute_latencies;
  }

  public void setExecute_latencies(RollingWindowSet execute_latencies) {
    this.execute_latencies = execute_latencies;
  }

  public Map<StatsFields, RollingWindowSet> getBoltFieldsMap() {
    return boltFieldsMap;
  }

  public void setBoltFieldsMap(Map<StatsFields, RollingWindowSet> boltFieldsMap) {
    this.boltFieldsMap = boltFieldsMap;
  }
}
