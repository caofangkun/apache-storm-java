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

import java.util.Map.Entry;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.ExecutorType;
import org.apache.storm.daemon.worker.stats.incval.CounterExtractor;
import org.apache.storm.daemon.worker.stats.incval.IncrValMerger;
import org.apache.storm.daemon.worker.stats.incval.IncrValUpdater;
import org.apache.storm.daemon.worker.stats.keyAvg.KeyedAvgExtractor;
import org.apache.storm.daemon.worker.stats.keyAvg.KeyedAvgMerger;
import org.apache.storm.daemon.worker.stats.keyAvg.KeyedAvgUpdater;
import org.apache.storm.daemon.worker.stats.rolling.RollingWindowSet;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SpoutStats;

@ClojureClass(className = "backtype.storm.stats")
public class Stats {

  @ClojureClass(className = "backtype.storm.stats##NUM-STAT-BUCKETS")
  public static final Integer NUM_STAT_BUCKETS = 20;

  // 10 minutes, 3 hours, 1 day
  @ClojureClass(className = "backtype.storm.stats##STAT-BUCKETS")
  public static Integer[] STAT_BUCKETS = { 30, 540, 4320 };

  @ClojureClass(className = "backtype.storm.stats#keyed-counter-rolling-window-set")
  public static RollingWindowSet keyedCounterRollingWindowSet(int num_buckets,
      Integer[] bucket_sizes) {

    RunnableCallback updater = new IncrValUpdater();
    RunnableCallback merger = new IncrValMerger();
    RunnableCallback extractor = new CounterExtractor();

    return RollingWindowSet.rolling_window_set(updater, merger, extractor,
        num_buckets, bucket_sizes);
  }

  @ClojureClass(className = "backtype.storm.stats#keyed-avg-rolling-window-set")
  public static RollingWindowSet keyedAvgRollingWindowSet(int num_buckets,
      Integer[] bucket_sizes) {
    RunnableCallback updater = new KeyedAvgUpdater();

    RunnableCallback merger = new KeyedAvgMerger();

    RunnableCallback extractor = new KeyedAvgExtractor();

    return RollingWindowSet.rolling_window_set(updater, merger, extractor,
        num_buckets, bucket_sizes);
  }

  @ClojureClass(className = "backtype.storm.stats#render-stats!")
  public static StatsData renderStats(Object stats) {
    if (stats instanceof SpoutExecutorStats) {
      return valueSpoutStats((SpoutExecutorStats) stats);
    }
    if (stats instanceof BoltExecutorStats) {
      return valueBoltStats((BoltExecutorStats) stats);
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.stats#value-spout-stats!")
  public static StatsData valueSpoutStats(SpoutExecutorStats stats) {
    cleanupSpoutStats(stats);
    StatsData ret = new StatsData(ExecutorType.spout);
    for (Entry<StatsFields, RollingWindowSet> entry : stats
        .getCommonFieldsMap().entrySet()) {
      ret.putCommon(entry.getKey(), entry.getValue().value_rolling_window_set());
    }
    for (Entry<StatsFields, RollingWindowSet> entry : stats.getSpoutFieldsMap()
        .entrySet()) {
      ret.putSpout(entry.getKey(), entry.getValue().value_rolling_window_set());
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.stats#value-bolt-stats!")
  public static StatsData valueBoltStats(BoltExecutorStats stats) {
    cleanupBoltStats(stats);
    StatsData ret = new StatsData(ExecutorType.bolt);
    for (Entry<StatsFields, RollingWindowSet> entry : stats
        .getCommonFieldsMap().entrySet()) {
      ret.putCommon(entry.getKey(), entry.getValue().value_rolling_window_set());
    }
    for (Entry<StatsFields, RollingWindowSet> entry : stats.getBoltFieldsMap()
        .entrySet()) {
      ret.putBolt(entry.getKey(), entry.getValue().value_rolling_window_set());
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-bolt-stats!")
  private static void cleanupBoltStats(BoltExecutorStats stats) {
    cleanupCommonStats((CommonStats) stats);
    for (StatsFields f : StatsFields.values()) {
      if (f.getFlag() == 1 || f.getFlag() == 3) {
        RollingWindowSet rws = stats.getBoltFieldsMap().get(f);
        cleanupStat(rws);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-spout-stats!")
  private static void cleanupSpoutStats(SpoutExecutorStats stats) {
    cleanupCommonStats((CommonStats) stats);
    for (StatsFields f : StatsFields.values()) {
      if (f.getFlag() == 2 || f.getFlag() == 3) {
        RollingWindowSet rws = stats.getSpoutFieldsMap().get(f);
        cleanupStat(rws);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-common-stats!")
  private static void cleanupCommonStats(CommonStats stats) {
    for (StatsFields f : StatsFields.values()) {
      if (f.getFlag() == 0) {
        RollingWindowSet rws = stats.getCommonFieldsMap().get(f);
        cleanupStat(rws);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-stat!")
  private static void cleanupStat(RollingWindowSet rws) {
    rws.cleanup_rolling_window_set();
  }

  @ClojureClass(className = "backtype.storm.stats#stats-rate")
  private static double statsRate(CommonStats stats) {
    return stats.getRate();
  }

  @ClojureClass(className = "backtype.storm.stats#update-executor-stat!#common")
  public static void update_executor_common_stat(CommonStats stats,
      StatsFields type, Object... args) {
    RollingWindowSet rws = stats.getCommonFieldsMap().get(type);
    if (rws != null) {
      rws.update_rolling_window_set(args);
    }
  }

  @ClojureClass(className = "backtype.storm.stats#update-executor-stat!#bolt")
  public static void update_executor_bolt_stat(BoltExecutorStats stats,
      StatsFields type, Object... args) {
    RollingWindowSet rws = stats.getBoltFieldsMap().get(type);
    if (rws != null) {
      rws.update_rolling_window_set(args);
    }
  }

  @ClojureClass(className = "backtype.storm.stats#update-executor-stat!#spout")
  public static void update_executor_spout_stat(SpoutExecutorStats stats,
      StatsFields type, Object... args) {
    RollingWindowSet rws = stats.getSpoutFieldsMap().get(type);
    if (rws != null) {
      rws.update_rolling_window_set(args);
    }
  }

  @ClojureClass(className = "backtype.storm.stats#emitted-tuple!")
  public static void emittedTuple(CommonStats stats, String stream) {
    update_executor_common_stat(stats, StatsFields.emitted, stream,
        statsRate(stats));
  }

  @ClojureClass(className = "backtype.storm.stats#transferred-tuples!")
  public static void transferredTuples(CommonStats stats, String stream, int amt) {
    update_executor_common_stat(stats, StatsFields.transferred, stream,
        (statsRate(stats) * amt));
  }

  @ClojureClass(className = "backtype.storm.stats#bolt-execute-tuple!")
  public static void boltExecuteTuple(BoltExecutorStats stats,
      String component, String stream, Long latencyMs) {
    GlobalStreamId key = new GlobalStreamId(component, stream);
    update_executor_bolt_stat(stats, StatsFields.executed, key,
        statsRate(stats));
    update_executor_bolt_stat(stats, StatsFields.execute_latencies, key,
        latencyMs);
  }

  @ClojureClass(className = "backtype.storm.stats#bolt-acked-tuple!")
  public static void boltAckedTuple(BoltExecutorStats stats, String component,
      String stream, Long latencyMs) {
    GlobalStreamId key = new GlobalStreamId(component, stream);
    update_executor_bolt_stat(stats, StatsFields.acked, key, statsRate(stats));
    update_executor_bolt_stat(stats, StatsFields.process_latencies, key,
        latencyMs);
  }

  @ClojureClass(className = "backtype.storm.stats#bolt-failed-tuple!")
  public static void boltFaileduple(BoltExecutorStats stats, String component,
      String stream, Long latencyMs) {
    GlobalStreamId key = new GlobalStreamId(component, stream);
    update_executor_bolt_stat(stats, StatsFields.failed, key, statsRate(stats));
  }

  @ClojureClass(className = "backtype.storm.stats#spout-acked-tuple!")
  public static void spoutAckedTuple(SpoutExecutorStats stats, String stream,
      Long latencyMs) {
    update_executor_spout_stat(stats, StatsFields.acked, stream,
        statsRate(stats));
    update_executor_spout_stat(stats, StatsFields.complete_latencies, stream,
        latencyMs);
  }

  @ClojureClass(className = "backtype.storm.stats#spout-failed-tuple!")
  public static void spoutFailedTuple(SpoutExecutorStats stats, String stream,
      Long latencyMs) {
    update_executor_spout_stat(stats, StatsFields.failed, stream,
        statsRate(stats));
  }

  public static String parseTimeKey(Integer key) {
    return String.valueOf(key);
  }

  @ClojureClass(className = "backtype.storm.stats#to-global-stream-id")
  public static GlobalStreamId toGlobalStreamId(String componentId,
      String streamId) {
    return new GlobalStreamId(componentId, streamId);
  }

  @ClojureClass(className = "backtype.storm.stats#thriftify-specific-stats#bolt")
  private static ExecutorSpecificStats thriftifySpecificStatsBolt(
      StatsData stats) {
    BoltStats boltStats =
        new BoltStats(stats.get_acked(), stats.get_failed(),
            stats.get_process_ms_avg(), stats.get_executed(),
            stats.get_execute_ms_avg());

    return ExecutorSpecificStats.bolt(boltStats);
  }

  @ClojureClass(className = "backtype.storm.stats#thriftify-specific-stats#spout")
  private static ExecutorSpecificStats thriftifySpecificStatsSpout(
      StatsData stats) {
    SpoutStats spoutStats =
        new SpoutStats(stats.get_spout_acked(), stats.get_spout_failed(),
            stats.get_complete_latencies());
    return ExecutorSpecificStats.spout(spoutStats);
  }

  @ClojureClass(className = "backtype.storm.stats#thriftify-executor-stats")
  public static ExecutorStats thriftifyExecutorStats(StatsData stats) {
    ExecutorSpecificStats specificStats = new ExecutorSpecificStats();
    if (stats.getType().equals(ExecutorType.bolt)) {
      specificStats = thriftifySpecificStatsBolt(stats);
    } else if (stats.getType().equals(ExecutorType.spout)) {
      specificStats = thriftifySpecificStatsSpout(stats);
    }
    return new ExecutorStats(stats.get_emitted(), stats.get_transferred(),
        specificStats, stats.getRate());
  }
}