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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.rolling.RollingWindowSet;

@ClojureClass(className = "backtype.storm.stats#CommonStats")
public class CommonStats implements Serializable {
  private static final long serialVersionUID = 1L;
  private RollingWindowSet emitted;
  private RollingWindowSet transferred;
  private Integer rate;
  protected Map<StatsFields, RollingWindowSet> commonFieldsMap =
      new HashMap<StatsFields, RollingWindowSet>();

  @ClojureClass(className = "backtype.storm.stats#mk-common-stats")
  public CommonStats(Integer rate) {
    this.emitted =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.transferred =
        Stats.keyedCounterRollingWindowSet(Stats.NUM_STAT_BUCKETS,
            Stats.STAT_BUCKETS);
    this.rate = rate;
    commonFieldsMap.put(StatsFields.emitted, emitted);
    commonFieldsMap.put(StatsFields.transferred, transferred);
  }

  public RollingWindowSet getEmitted() {
    return emitted;
  }

  public void setEmitted(RollingWindowSet emitted) {
    this.emitted = emitted;
  }

  public RollingWindowSet getTransferred() {
    return transferred;
  }

  public void setTransferred(RollingWindowSet transferred) {
    this.transferred = transferred;
  }

  public Integer getRate() {
    return rate;
  }

  public void setRate(Integer rate) {
    this.rate = rate;
  }

  public Map<StatsFields, RollingWindowSet> getCommonFieldsMap() {
    return commonFieldsMap;
  }

  public void setCommonFieldsMap(
      Map<StatsFields, RollingWindowSet> commonFieldsMap) {
    this.commonFieldsMap = commonFieldsMap;
  }

}
