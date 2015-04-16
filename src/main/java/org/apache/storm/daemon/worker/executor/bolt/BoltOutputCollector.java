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
package org.apache.storm.daemon.worker.executor.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.Config;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.counter.TaskCounter;
import com.tencent.jstorm.daemon.acker.AckerBolt;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.executor.error.ITaskReportErr;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.daemon.task.TasksFn;
import com.tencent.jstorm.stats.BoltExecutorStats;
import com.tencent.jstorm.stats.CommonStats;
import com.tencent.jstorm.stats.Stats;
import com.tencent.jstorm.tuple.TuplePair;
import com.tencent.jstorm.utils.ServerUtils;

/**
 * 
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#IOutputCollector")
public class BoltOutputCollector implements IOutputCollector {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoltOutputCollector.class);
  private ITaskReportErr reportError;
  private ExecutorTransferFn transferFn;
  private TasksFn tasksFn;
  private WorkerTopologyContext topologyContext;
  private Integer taskId;
  private Random rand;
  private ConcurrentLinkedQueue<TuplePair> overflowBuffer;
  private CommonStats executorStats;
  private Boolean isDebug = false;
  private TaskData taskData;
  private Counters counters;

  @SuppressWarnings("rawtypes")
  public BoltOutputCollector(TaskData taskData, ITaskReportErr reportError,
      Map stormConf, ExecutorTransferFn transferFn,
      WorkerTopologyContext topologyContext, Integer taskId,
      RotatingMap<Tuple, Long> tuple_start_times, CommonStats executorStats,
      ConcurrentLinkedQueue<TuplePair> overflowBuffer) {
    this.taskData = taskData;
    this.tasksFn = taskData.getTasksFn();
    this.reportError = reportError;
    this.transferFn = transferFn;
    this.topologyContext = topologyContext;
    this.taskId = taskId;
    this.counters = taskData.getExecutorData().getCounters();
    this.rand = new Random(Utils.secureRandomLong());
    this.overflowBuffer = overflowBuffer;
    this.executorStats = executorStats;
    this.isDebug =
        ServerUtils.parseBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors,
      List<Object> tuple) {
    if (isDebug) {
      LOG.info("bolt emit: streamId:" + streamId + " anchors:" + anchors
          + " messageId:" + tuple.toString());
    }
    List<Integer> ret = boltEmit(streamId, anchors, tuple, null);
    // counters.incrCounter(TaskCounter.BOLT_EMIT_CNT, 1);
    counters.tpsCounter(TaskCounter.BOLT_EMIT_TPS, String.valueOf(taskId));
    return ret;
  }

  @Override
  public void emitDirect(int taskId, String streamId,
      Collection<Tuple> anchors, List<Object> tuple) {
    if (isDebug) {
      LOG.info("bolt emitDirect: streamId:" + streamId + " anchors:" + anchors
          + " messageId:" + tuple.toString());
    }
    boltEmit(streamId, anchors, tuple, taskId);
    // counters.incrCounter(TaskCounter.BOLT_EMIT_DIRECT_CNT, 1);
    counters.tpsCounter(TaskCounter.BOLT_EMIT_DIRECT_TPS,
        String.valueOf(taskId));
  }

  @Override
  public void ack(Tuple input) {

    TupleImpl tuple = (TupleImpl) input;
    Long ackVal = tuple.getAckVal();
    Map<Long, Long> rootToIds = tuple.getMessageId().getAnchorsToIds();
    for (Map.Entry<Long, Long> entry : rootToIds.entrySet()) {
      Long root = entry.getKey();
      Long id = entry.getValue();
      try {
        TaskUtils.sendUnanchored(taskData, AckerBolt.ACKER_ACK_STREAM_ID,
            Lists.newArrayList((Object) root, ServerUtils.bit_xor(id, ackVal)),
            overflowBuffer);
      } catch (InsufficientCapacityException e) {
        // TODO Auto-generated catch block
      }
    }

    Long delta = tupleTimeDelta((TupleImpl) tuple);

    TaskUtils.applyHooks(taskData.getUserContext(), new BoltAckInfo(tuple,
        taskId, delta));

    if (delta != null) {
      Stats.boltAckedTuple((BoltExecutorStats) executorStats,
          tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
    }
  }

  @Override
  public void fail(Tuple tuple) {
    Set<Long> roots = tuple.getMessageId().getAnchors();
    for (Long root : roots) {
      try {
        TaskUtils.sendUnanchored(taskData, AckerBolt.ACKER_FAIL_STREAM_ID,
            Lists.newArrayList((Object) root), overflowBuffer);
      } catch (InsufficientCapacityException e) {
        // TODO Auto-generated catch block
      }
    }

    Long delta = tupleTimeDelta((TupleImpl) tuple);

    TaskUtils.applyHooks(taskData.getUserContext(), new BoltFailInfo(tuple,
        taskId, delta));

    if (delta != null) {
      Stats.boltFaileduple((BoltExecutorStats) executorStats,
          tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
    }
  }

  @Override
  public void reportError(Throwable error) {
    reportError.report(error);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#bolt-emit")
  private List<Integer> boltEmit(String stream, Collection<Tuple> anchors,
      List<Object> values, Integer task) {
    List<Integer> outTasks = new ArrayList<Integer>();
    if (task != null) {
      outTasks = tasksFn.fn(task, stream, values);
    } else {
      outTasks = tasksFn.fn(stream, values);
    }

    for (Integer t : outTasks) {
      Map<Long, Long> anchorsToIds = new HashMap<Long, Long>();
      if (anchors != null) {
        for (Tuple anchor : anchors) {
          TupleImpl a = (TupleImpl) anchor;
          Set<Long> rootIds = a.getMessageId().getAnchorsToIds().keySet();
          if (rootIds != null && rootIds.size() > 0) {
            Long edgeId = MessageId.generateId(rand);
            a.updateAckVal(edgeId);
            for (Long rootId : rootIds) {
              putXor(anchorsToIds, rootId, edgeId);
            }
          }
        }
      }
      TupleImpl tupleExt =
          new TupleImpl(topologyContext, values, taskId, stream,
              MessageId.makeId(anchorsToIds));
      try {
        transferFn.transfer(t, tupleExt, overflowBuffer);
      } catch (InsufficientCapacityException e) {
        LOG.error("bolt emit", e);
      }
    }

    return outTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#tuple-time-delta!")
  public static Long tupleTimeDelta(TupleImpl tuple) {
    Long ms = tuple.getProcessSampleStartTime();
    if (ms != null) {
      return ServerUtils.time_delta_ms(ms);
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#put-xor!")
  public static void putXor(Map<Long, Long> pending, Long key, Long id) {
    // synchronized (pending) {
    Long curr = pending.get(key);
    if (curr == null) {
      curr = Long.valueOf(0);
    }
    pending.put(key, ServerUtils.bit_xor(curr, id));
    // }
  }

}
