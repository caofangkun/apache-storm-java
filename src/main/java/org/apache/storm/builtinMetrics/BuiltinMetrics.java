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
package org.apache.storm.builtinMetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.ExecutorType;
import org.apache.storm.stats.CommonStats;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IStatefulObject;
import backtype.storm.metric.api.StateMetric;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

public class BuiltinMetrics {

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#make-data")
  public static Map<String, IMetric> mkData(ExecutorType type) {
    if (type.equals(ExecutorType.spout)) {
      return new BuiltinSpoutMetrics().getSpoutMetrics();
    }
    if (type.equals(ExecutorType.bolt)) {
      return new BuiltinBoltMetrics().getBoltMetrics();
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#register-all")
  public static void registerAll(Map<String, IMetric> builtinMetrics,
      Map stormConf, TopologyContext topologyContext) {
    if (builtinMetrics == null || builtinMetrics.isEmpty()) {
      return;
    }

    for (Map.Entry<String, IMetric> builtinMetric : builtinMetrics.entrySet()) {
      String kw = builtinMetric.getKey();
      IMetric imetric = builtinMetric.getValue();
      int timeBucketSizeInSecs =
          Utils.getInt(
              stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS),
              60);
      topologyContext.registerMetric("__" + kw, imetric, timeBucketSizeInSecs);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#register-iconnection-server-metric")
  public static void registerIconnectionServerMetric(Object server,
      Map stormConf, TopologyContext topologyContext) {

    if (server instanceof IStatefulObject) {
      int timeBucketSizeInSecs =
          Utils.getInt(
              stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS),
              60);
      topologyContext.registerMetric("__recv-iconnection", new StateMetric(
          (IStatefulObject) server), timeBucketSizeInSecs);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#register-iconnection-client-metric")
  public static void registerIconnectionClientMetric(
      final ConcurrentHashMap<WorkerSlot, IConnection> nodePortToSocketRef,
      Map stormConf, TopologyContext topologyContext) {
    int timeBucketSizeInSecs =
        Utils
            .getInt(
                stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS),
                60);
    topologyContext.registerMetric("__send-iconnection", new IMetric() {

      @Override
      public Object getValueAndReset() {
        Map<WorkerSlot, Object> ret = new HashMap<WorkerSlot, Object>();

        for (Map.Entry<WorkerSlot, IConnection> nodePortToSocket : nodePortToSocketRef
            .entrySet()) {
          WorkerSlot nodePort = nodePortToSocket.getKey();
          IConnection connection = nodePortToSocket.getValue();
          if (connection instanceof IStatefulObject) {
            IStatefulObject sObj = (IStatefulObject) connection;
            ret.put(nodePort, sObj.getState());
          }
        }
        return ret;
      }

    }, timeBucketSizeInSecs);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#register-queue-metrics")
  public static void registerQueueMetrics(List<DisruptorQueue> queues,
      Map stormConf, TopologyContext topologyContext) {
    int timeBucketSizeInSecs =
        Utils
            .getInt(
                stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS),
                60);
    for (DisruptorQueue queue : queues) {
      String queueName = "__" + queue.getName();
      topologyContext.registerMetric(queueName, new StateMetric(queue),
          timeBucketSizeInSecs);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#spout-acked-tuple!")
  public static void spoutAckedTuple(BuiltinSpoutMetrics m, CommonStats stats,
      String stream, int latencyMs) {
    m.getAckCount().scope(stream).incrBy(stats.getRate().intValue());
    m.getCompleteLatency().scope(stream).update(latencyMs);
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#spout-failed-tuple!")
  public static void spoutFailedTuple(BuiltinSpoutMetrics m, CommonStats stats,
      String stream, int latencyMs) {
    m.getFailCount().scope(stream).incrBy(stats.getRate().intValue());
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#bolt-execute-tuple!")
  public static void boltExecuteTuple(BuiltinBoltMetrics m, CommonStats stats,
      String compId, String stream, int latencyMs) {
    String scope = compId + ":" + stream;
    m.getEmitCount().scope(scope).incrBy(stats.getRate().intValue());
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#bolt-acked-tuple!")
  public static void boltAckedTuple(BuiltinBoltMetrics m, CommonStats stats,
      String compId, String stream, int latencyMs) {
    String scope = compId + ":" + stream;
    m.getAckCount().scope(scope).incrBy(stats.getRate().intValue());
    m.getProcessLatency().scope(scope).update(latencyMs);
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#bolt-failed-tuple!")
  public static void boltFailedTuple(BuiltinBoltMetrics m, CommonStats stats,
      String compId, String stream) {
    String scope = compId + ":" + stream;
    m.getFailCount().scope(scope).incrBy(stats.getRate().intValue());
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#emitted-tuple!")
  public static void emittedTuple(BuiltinBoltMetrics m, CommonStats stats,
      String stream) {
    m.getEmitCount().scope(stream).incrBy(stats.getRate().intValue());
  }

  @ClojureClass(className = "backtype.storm.daemon.builtin-metrics#transferred-tuple!")
  public static void transferredTuple(BuiltinBoltMetrics m, CommonStats stats,
      String stream, int numOutTasks) {
    m.getTransferCount().scope(stream)
        .incrBy(stats.getRate().intValue() * numOutTasks);
  }
}
