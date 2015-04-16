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
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.daemon.worker.executor.ExecutorType;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SpoutStats;

public class StatsData implements Serializable {

  private static final long serialVersionUID = 1L;

  protected Map<StatsFields, Map<Integer, Object>> commonFieldsMap;
  protected Map<StatsFields, Map<Integer, Object>> boltFieldsMap;
  protected Map<StatsFields, Map<Integer, Object>> spoutFieldsMap;
  protected int rate = Stats.NUM_STAT_BUCKETS;
  private ExecutorType type;

  public StatsData(ExecutorType type) {
    this.type = type;
    commonFieldsMap = new HashMap<StatsFields, Map<Integer, Object>>();
    boltFieldsMap = new HashMap<StatsFields, Map<Integer, Object>>();
    spoutFieldsMap = new HashMap<StatsFields, Map<Integer, Object>>();

    // <window, Map<Stream, counter>>
    HashMap<Integer, Object> emitted = new HashMap<Integer, Object>();
    HashMap<Integer, Object> transferred = new HashMap<Integer, Object>();
    HashMap<Integer, Object> acked = new HashMap<Integer, Object>();
    HashMap<Integer, Object> failed = new HashMap<Integer, Object>();
    HashMap<Integer, Object> process_latencies = new HashMap<Integer, Object>();
    HashMap<Integer, Object> executed = new HashMap<Integer, Object>();
    HashMap<Integer, Object> execute_latencies = new HashMap<Integer, Object>();
    HashMap<Integer, Object> complete_ms_avg = new HashMap<Integer, Object>();

    // Common
    commonFieldsMap.put(StatsFields.emitted, emitted);
    commonFieldsMap.put(StatsFields.transferred, transferred);

    // Bolt
    boltFieldsMap.put(StatsFields.acked, acked);
    boltFieldsMap.put(StatsFields.failed, failed);
    boltFieldsMap.put(StatsFields.process_latencies, process_latencies);
    boltFieldsMap.put(StatsFields.executed, executed);
    boltFieldsMap.put(StatsFields.execute_latencies, execute_latencies);

    // Spout
    spoutFieldsMap.put(StatsFields.acked, acked);
    spoutFieldsMap.put(StatsFields.failed, failed);
    spoutFieldsMap.put(StatsFields.complete_latencies, complete_ms_avg);

  }

  public void putCommon(StatsFields type, Map<Integer, Object> value) {
    commonFieldsMap.put(type, value);
  }

  public Map<Integer, Object> getCommon(StatsFields type) {
    return commonFieldsMap.get(type);
  }

  public void putSpout(StatsFields type, Map<Integer, Object> value) {
    spoutFieldsMap.put(type, value);
  }

  public Map<Integer, Object> getSpout(StatsFields type) {
    return spoutFieldsMap.get(type);
  }

  public void putBolt(StatsFields type, Map<Integer, Object> value) {
    boltFieldsMap.put(type, value);
  }

  public Map<Integer, Object> getBolt(StatsFields type) {
    return boltFieldsMap.get(type);
  }

  public int getRate() {
    return rate;
  }

  public void setRate(int rate) {
    this.rate = rate;
  }

  @Override
  public boolean equals(Object assignment) {
    if ((assignment instanceof StatsData) == false) {
      return false;
    }

    StatsData otherData = (StatsData) assignment;

    for (Entry<StatsFields, Map<Integer, Object>> entry : commonFieldsMap
        .entrySet()) {
      StatsFields type = entry.getKey();
      Map<Integer, Object> value = entry.getValue();
      Map<Integer, Object> otherValue = otherData.getCommon(type);

      if (value.equals(otherValue) == false) {
        return false;
      }
    }

    for (Entry<StatsFields, Map<Integer, Object>> entry : spoutFieldsMap
        .entrySet()) {
      StatsFields type = entry.getKey();
      Map<Integer, Object> value = entry.getValue();
      Map<Integer, Object> otherValue = otherData.getSpout(type);
      if (value.equals(otherValue) == false) {
        return false;
      }
    }

    for (Entry<StatsFields, Map<Integer, Object>> entry : boltFieldsMap
        .entrySet()) {
      StatsFields type = entry.getKey();
      Map<Integer, Object> value = entry.getValue();
      Map<Integer, Object> otherValue = otherData.getBolt(type);

      if (value.equals(otherValue) == false) {
        return false;
      }
    }

    return true;
  }

  // @Override
  // public int hashCode() {
  // int ret = 0;
  // for (Entry<StaticsType, Map<Integer, Object>> entry :
  // staticsMap.entrySet()) {
  // StaticsType type = entry.getKey();
  // Map<Integer, Object> value = entry.getValue();
  //
  // ret += value.hashCode();
  // }
  // return ret;
  // }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  @SuppressWarnings("unchecked")
  public <K, V> Map<String, Map<K, V>> convertKey(Map<Integer, Object> statics,
      K keySample, V valueSample) {

    Map<String, Map<K, V>> ret = new HashMap<String, Map<K, V>>();

    for (Entry<Integer, Object> times : statics.entrySet()) {

      Map<K, V> val = (Map<K, V>) times.getValue();

      Integer window = times.getKey();

      String key = Stats.parseTimeKey(window);

      ret.put(key, val);
    }

    return ret;
  }

  // Common
  public Map<String, Map<String, Long>> get_emitted() {
    Map<Integer, Object> statics = commonFieldsMap.get(StatsFields.emitted);
    return convertKey(statics, String.valueOf(""), Long.valueOf(0));
  }

  public Map<String, Map<String, Long>> get_transferred() {
    Map<Integer, Object> statics = commonFieldsMap.get(StatsFields.transferred);
    return convertKey(statics, String.valueOf(""), Long.valueOf(0));
  }

  // Spout
  public Map<String, Map<String, Long>> get_spout_acked() {
    Map<Integer, Object> statics = spoutFieldsMap.get(StatsFields.acked);
    return convertKey(statics, String.valueOf(""), Long.valueOf(0));
  }

  public Map<String, Map<String, Long>> get_spout_failed() {
    Map<Integer, Object> statics = spoutFieldsMap.get(StatsFields.failed);

    return convertKey(statics, String.valueOf(""), Long.valueOf(0));
  }

  public Map<String, Map<String, Double>> get_complete_latencies() {
    Map<Integer, Object> statics =
        spoutFieldsMap.get(StatsFields.complete_latencies);
    return convertKey(statics, String.valueOf(""), Double.valueOf(0));
  }

  // Bolt
  public Map<String, Map<GlobalStreamId, Long>> get_acked() {
    Map<Integer, Object> statics = boltFieldsMap.get(StatsFields.acked);
    GlobalStreamId streamIdSample = new GlobalStreamId("", "");
    return convertKey(statics, streamIdSample, Long.valueOf(0));
  }

  public Map<String, Map<GlobalStreamId, Long>> get_failed() {
    Map<Integer, Object> statics = boltFieldsMap.get(StatsFields.failed);
    GlobalStreamId streamIdSample = new GlobalStreamId("", "");
    return convertKey(statics, streamIdSample, Long.valueOf(0));
  }

  public Map<String, Map<GlobalStreamId, Double>> get_process_ms_avg() {
    Map<Integer, Object> statics =
        boltFieldsMap.get(StatsFields.process_latencies);
    GlobalStreamId streamIdSample = new GlobalStreamId("", "");
    return convertKey(statics, streamIdSample, Double.valueOf(0));
  }

  public Map<String, Map<GlobalStreamId, Long>> get_executed() {
    Map<Integer, Object> statics = boltFieldsMap.get(StatsFields.executed);
    GlobalStreamId streamIdSample = new GlobalStreamId("", "");
    return convertKey(statics, streamIdSample, Long.valueOf(0));
  }

  public Map<String, Map<GlobalStreamId, Double>> get_execute_ms_avg() {
    Map<Integer, Object> statics =
        boltFieldsMap.get(StatsFields.execute_latencies);

    GlobalStreamId streamIdSample = new GlobalStreamId("", "");
    return convertKey(statics, streamIdSample, Double.valueOf(0));
  }

  public ExecutorStats getExecutorStats() {
    ExecutorStats taskStats = new ExecutorStats();

    taskStats.set_emitted(get_emitted());
    taskStats.set_transferred(get_transferred());

    ExecutorSpecificStats specific = new ExecutorSpecificStats();
    if (this.type.equals(ExecutorType.bolt)) {
      BoltStats bolt = new BoltStats();
      bolt.set_acked(get_acked());
      bolt.set_failed(get_failed());
      bolt.set_process_ms_avg(get_process_ms_avg());
      bolt.set_executed(get_executed());
      bolt.set_execute_ms_avg(get_execute_ms_avg());
      specific.set_bolt(bolt);
    }
    if (this.type.equals(ExecutorType.spout)) {
      SpoutStats spout = new SpoutStats();
      spout.set_acked(get_spout_acked());
      spout.set_failed(get_spout_failed());
      spout.set_complete_ms_avg(get_complete_latencies());
      specific.set_spout(spout);
    }
    taskStats.set_specific(specific);
    return taskStats;
  }

  public ExecutorType getType() {
    return type;
  }

  public void setType(ExecutorType type) {
    this.type = type;
  }
}
