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
package org.apache.storm.daemon.worker.executor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.util.MapUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.daemon.worker.executor.task.TaskUtils;
import org.apache.storm.daemon.worker.stats.BoltExecutorStats;
import org.apache.storm.daemon.worker.stats.CommonStats;
import org.apache.storm.daemon.worker.stats.SpoutExecutorStats;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.ReflectionUtils;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.metric.api.IMetricsConsumer.DataPoint;
import backtype.storm.metric.api.IMetricsConsumer.TaskInfo;
import backtype.storm.spout.ISpoutWaitStrategy;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ExecutorUtils {

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.executor#init-spout-wait-strategy")
  public static ISpoutWaitStrategy initSpoutWaitStrategy(Map stormConf)
      throws ClassNotFoundException {
    // default : backtype.storm.spout.SleepSpoutWaitStrategy
    ISpoutWaitStrategy spoutWaitStrategy =
        ReflectionUtils.newInstance((String) stormConf
            .get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
    spoutWaitStrategy.prepare(stormConf);
    return spoutWaitStrategy;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.executor#executor-max-spout-pending")
  public static Integer executorMaxSpoutPending(Map stormConf, int numTasks) {
    Integer p =
        CoreUtil.parseInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING),
            null);
    if (p != null) {
      return p * numTasks;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static DisruptorQueue batchTransferToworker(Map stormConf,
      ExecutorInfo executorInfo) {
    Integer bufferSize =
        CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE), 1024);
    WaitStrategy waitStrategy =
        (WaitStrategy) Utils.newInstance((String) stormConf
            .get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
    DisruptorQueue disruptorQueue =
        new DisruptorQueue("executor-" + executorInfo.get_task_start()
            + "-send-queue", new SingleThreadedClaimStrategy(bufferSize),
            waitStrategy);
    return disruptorQueue;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#executor-type")
  public static ExecutorType executorType(WorkerTopologyContext context,
      String componentId) {
    StormTopology topology = context.getRawTopology();
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    if (bolts.containsKey(componentId)) {
      return ExecutorType.bolt;
    }
    if (spouts.containsKey(componentId)) {
      return ExecutorType.spout;
    } else {
      throw new RuntimeException("Could not find " + componentId
          + " in topology " + topology);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-executor-stats")
  public static CommonStats mkExecutorStats(ExecutorType executorType,
      Integer samplerate) {
    if (executorType.equals(ExecutorType.spout)) {
      return new SpoutExecutorStats(samplerate);

    } else if (executorType.equals(ExecutorType.bolt)) {
      return new BoltExecutorStats(samplerate);
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.executor#normalized-component-conf")
  public static Map normalizedComponentConf(Map stormConf,
      WorkerTopologyContext generalContext, String componentId) {
    List<Object> to_remove = ConfigUtil.All_CONFIGS();
    to_remove.remove(Config.TOPOLOGY_DEBUG);
    to_remove.remove(Config.TOPOLOGY_MAX_SPOUT_PENDING);
    to_remove.remove(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
    to_remove.remove(Config.TOPOLOGY_TRANSACTIONAL_ID);
    to_remove.remove(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    to_remove.remove(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS);
    to_remove.remove(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY);

    Map specConf = new HashMap();
    String jsonConf =
        generalContext.getComponentCommon(componentId).get_json_conf();
    if (jsonConf != null) {
      specConf = (Map) CoreUtil.from_json(jsonConf);
    }

    for (Object p : to_remove) {
      specConf.remove(p);
    }

    return MapUtils.merge(stormConf, specConf);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#metrics-tick")
  public static void metricsTick(ExecutorData executorData, TaskData taskData,
      TupleImpl tuple) {
    // TODO
    Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry =
        executorData.getIntervalToTaskToMetricRegistry();
    WorkerTopologyContext workerContext = executorData.getWorkerContext();
    int interval = tuple.getInteger(0);
    int taskId = taskData.getTaskId();
    Map<String, IMetric> nameToImetrics =
        intervalToTaskToMetricRegistry.get(interval).get(taskId);
    long now = System.currentTimeMillis() / 1000;
    TaskInfo taskInfo = null;
    try {
      taskInfo =
          new IMetricsConsumer.TaskInfo(CoreUtil.localHostname(),
              workerContext.getThisWorkerPort(), executorData.getComponentId(),
              taskId, now, interval);
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    List<DataPoint> dataPoints = new ArrayList<DataPoint>();
    for (Map.Entry<String, IMetric> nameTometric : nameToImetrics.entrySet()) {
      String name = nameTometric.getKey();
      IMetric imetirc = nameTometric.getValue();
      Object value = imetirc.getValueAndReset();
      if (value != null) {
        DataPoint dataPoint = new IMetricsConsumer.DataPoint(name, value);
        dataPoints.add(dataPoint);
      }
    }

    try {
      if (!dataPoints.isEmpty()) {
        TaskUtils.sendUnanchored(taskData, Constants.METRICS_STREAM_ID,
            Lists.newArrayList((Object) taskInfo));
      }
    } catch (InsufficientCapacityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
