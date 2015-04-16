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
package org.apache.storm.daemon.worker.executor.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.MutableLong;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.counter.TaskCounter;
import com.tencent.jstorm.daemon.acker.AckerBolt;
import com.tencent.jstorm.daemon.common.Common;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.executor.error.ITaskReportErr;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.daemon.task.TasksFn;
import com.tencent.jstorm.tuple.TuplePair;
import com.tencent.jstorm.utils.EvenSampler;
import com.tencent.jstorm.utils.ServerUtils;

public class SpoutCollector implements ISpoutOutputCollector {
  private static Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);

  private ExecutorTransferFn executorTransferFn;
  private RotatingMap<Long, TupleInfo> pending;
  private WorkerTopologyContext workerContext;

  private ITaskReportErr report_error;

  private boolean isDebug = false;
  private boolean isHasAckers = false;
  private Random rand;

  private ExecutorData executorData;
  private Integer taskId;
  private MutableLong emittedCount;
  private ConcurrentLinkedQueue<TuplePair> overflowBuffer;
  private TaskData taskData;
  private TasksFn tasksFn;
  private EvenSampler sampler;
  private Counters counters;

  public SpoutCollector(ExecutorData executorData, Integer taskId,
      TaskData taskData, MutableLong emittedCount,
      RotatingMap<Long, TupleInfo> pending,
      ConcurrentLinkedQueue<TuplePair> overflowBuffer) {
    this.executorData = executorData;
    this.taskData = taskData;
    this.taskId = taskId;
    this.tasksFn = taskData.getTasksFn();
    @SuppressWarnings("rawtypes")
    Map stormConf = executorData.getStormConf();
    this.counters = executorData.getCounters();
    this.emittedCount = emittedCount;
    this.executorTransferFn = executorData.getTransferFn();
    this.pending = pending;
    this.workerContext = executorData.getWorkerContext();
    this.isHasAckers = Common.hasAcker(stormConf);
    this.rand = new Random(Utils.secureRandomLong());
    this.report_error = executorData.getReportError();
    this.isDebug =
        ServerUtils.parseBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
    this.overflowBuffer = overflowBuffer;
    this.sampler = executorData.getSampler();
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple,
      Object messageId) {
    if (isDebug) {
      LOG.info("spout emit streamId:" + streamId + " tuple:" + tuple.toString()
          + " messageId:" + messageId);
    }
    List<Integer> ret = sendSpoutMsg(streamId, tuple, messageId, null);
    // counters.incrCounter(TaskCounter.SPOUT_EMIT_CNT, 1);
    counters.tpsCounter(TaskCounter.SPOUT_EMIT_TPS, String.valueOf(taskId));
    return ret;
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple,
      Object messageId) {
    if (isDebug) {
      LOG.info("spout emitDirect: taskId:" + taskId + " streamId:" + streamId
          + " tuple:" + tuple.toString() + " messageId:" + messageId);
    }
    sendSpoutMsg(streamId, tuple, messageId, taskId);
    // counters.incrCounter(TaskCounter.SPOUT_EMIT_DIRECT_CNT, 1);
    counters.tpsCounter(TaskCounter.SPOUT_EMIT_DIRECT_TPS);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout#send-spout-msg")
  private List<Integer> sendSpoutMsg(String outStreamId, List<Object> values,
      Object messageId, Integer outTaskId) {
    emittedCount.increment();
    List<Integer> outTasks = null;
    if (outTaskId != null) {
      outTasks = tasksFn.fn(outTaskId, outStreamId, values);
    } else {
      outTasks = tasksFn.fn(outStreamId, values);
    }

    if (outTasks.size() == 0) {
      // don't need send tuple to other task
      return outTasks;
    }

    boolean isRooted = (messageId != null) && isHasAckers;
    Long rootId = null;
    if (isRooted) {
      rootId = MessageId.generateId(rand);
    }
    List<Long> outIds = new ArrayList<Long>();
    for (Integer outTask : outTasks) {
      MessageId tupleId;
      if (isRooted) {
        long id = MessageId.generateId(rand);
        outIds.add(id);
        tupleId = MessageId.makeRootId(rootId, id);
      } else {
        tupleId = MessageId.makeUnanchored();
      }

      TupleImpl outTuple =
          new TupleImpl(workerContext, values, taskId, outStreamId, tupleId);
      try {
        executorTransferFn.transfer(outTask, outTuple, overflowBuffer);
      } catch (InsufficientCapacityException e) {
        LOG.error("transfer error:{}", e.getMessage());
      }
    }

    if (isRooted) {
      TupleInfo info = new TupleInfo();
      info.setTaskId(taskId);
      info.setMessageId(messageId);
      info.setStream(outStreamId);
      info.setValues(values);
      if (sampler.countCheck()) {
        info.setTimestamp(System.currentTimeMillis());
      }
      pending.put(rootId, info);

      List<Object> ackerTuple =
          Lists.newArrayList((Object) rootId, ServerUtils.bit_xor_vals(outIds),
              taskId);
      try {
        TaskUtils.sendUnanchored(taskData, AckerBolt.ACKER_INIT_STREAM_ID,
            ackerTuple);
      } catch (InsufficientCapacityException e) {
        // TODO Auto-generated catch block
      }
    } else if (messageId != null) {
      TupleInfo info = new TupleInfo();
      info.setStream(outStreamId);
      info.setValues(values);
      Long timeDelta = null;
      if (sampler.countCheck()) {
        timeDelta = 0L;
      }
      AckSpoutMsg ack =
          new AckSpoutMsg(executorData, taskData, messageId, info, timeDelta, 0L);
      ack.run();
    }
    return outTasks;
  }

  @Override
  public void reportError(Throwable error) {
    report_error.report(error);
  }

}
