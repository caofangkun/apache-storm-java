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

@ClojureClass(className = "backtype.storm.stats#SpoutExecutorStats")
public class SpoutExecutorStats extends CommonStats {
  private static final long serialVersionUID = 1L;
  private RollingWindowSet acked;
  private RollingWindowSet failed;
  private RollingWindowSet complete_latencies;
  protected Map<StatsFields, RollingWindowSet> spoutFieldsMap =
      new HashMap<StatsFields, RollingWindowSet>();

  @ClojureClass(className = "backtype.storm.stats#mk-spout-stats")
  public SpoutExecutorStats(Integer rate) {
    super(rate);
    this.acked =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.failed =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.complete_latencies =
        Stats.keyedAvgRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    spoutFieldsMap.put(StatsFields.acked, acked);
    spoutFieldsMap.put(StatsFields.failed, failed);
    spoutFieldsMap.put(StatsFields.complete_latencies, complete_latencies);
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

  public RollingWindowSet getComplete_latencies() {
    return complete_latencies;
  }

  public void setComplete_latencies(RollingWindowSet complete_latencies) {
    this.complete_latencies = complete_latencies;
  }

  public Map<StatsFields, RollingWindowSet> getSpoutFieldsMap() {
    return spoutFieldsMap;
  }

  public void setSpoutFieldsMap(
      Map<StatsFields, RollingWindowSet> spoutFieldsMap) {
    this.spoutFieldsMap = spoutFieldsMap;
  }
}
