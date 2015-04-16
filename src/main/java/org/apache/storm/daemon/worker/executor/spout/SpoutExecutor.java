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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.ICredentialsListener;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.ISpoutWaitStrategy;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.MutableLong;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Time;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.counter.TaskCounter;
import com.tencent.jstorm.daemon.acker.AckerBolt;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorStatus;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.tuple.TuplePair;
import com.tencent.jstorm.utils.ServerUtils;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */

@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout")
public class SpoutExecutor extends RunnableCallback implements
    EventHandler<Object> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(SpoutExecutor.class);
  private ExecutorData executorData;
  private AtomicBoolean lastActive;
  private AtomicBoolean isActive;
  private RotatingMap<Long, TupleInfo> pending;
  private DisruptorQueue receiveQueue;
  private KryoTupleDeserializer deserializer;
  private ArrayList<ISpout> spouts;
  private Map<Integer, TaskData> taskDatas;
  private Boolean isDebug;
  private Integer maxSpoutPending;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private ConcurrentLinkedQueue<TuplePair> overflowBuffer;
  private String componentId;
  private MutableLong emittedCount;
  private MutableLong emptyEmitStreak;
  private ISpoutWaitStrategy spoutWaitStrategy;
  private ExecutorTransferFn executorTransferFn;
  protected ExecutorStatus taskStatus;
  private Exception exception = null;
  private Counters counters;

  public SpoutExecutor(ExecutorData executorData,
      Map<Integer, TaskData> taskDatas) throws Exception {
    this.executorData = executorData;
    this.componentId = executorData.getComponentId();
    this.taskDatas = taskDatas;
    this.taskStatus = executorData.getTaskStatus();
    this.spouts = new ArrayList<ISpout>();
    for (TaskData taskData : taskDatas.values()) {
      this.spouts.add((ISpout) taskData.getObject());
    }
    this.stormConf = executorData.getStormConf();
    this.lastActive = new AtomicBoolean(false);
    this.pending =
        new RotatingMap<Long, TupleInfo>(AckerBolt.TIMEOUT_BUCKET_NUM,
            new SpoutExpiredCallback<Long, TupleInfo>(executorData, taskDatas));
    this.receiveQueue = executorData.getReceiveQueue();
    this.emittedCount = new MutableLong(0);
    this.emptyEmitStreak = new MutableLong(0);
    this.deserializer = executorData.getDeserializer();
    this.executorTransferFn = executorData.getTransferFn();
    this.isDebug =
        ServerUtils.parseBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
    this.maxSpoutPending =
        ExecutorUtils.executorMaxSpoutPending(stormConf, taskDatas.size());
    this.isActive = executorData.getStormActiveAtom();
    this.counters = executorData.getCounters();
    // the overflow buffer is used to ensure that spouts never block when
    // emitting
    // this ensures that the spout can always clear the incoming buffer
    // (acks
    // and fails), which
    // prevents deadlock from occuring across the topology (e.g. Spout ->
    // Bolt
    // -> Acker -> Spout, and all
    // buffers filled up)
    // when the overflow buffer is full, spouts stop calling nextTuple until
    // it's able to clear the overflow buffer
    // this limits the size of the overflow buffer to however many tuples a
    // spout emits in one call of nextTuple,
    // preventing memory issues
    this.overflowBuffer = new ConcurrentLinkedQueue<TuplePair>();
    try {
      this.spoutWaitStrategy = ExecutorUtils.initSpoutWaitStrategy(stormConf);
      // If topology was started in inactive state,
      // don't call (.open spout)
      // until it's activated first.
      while (!isActive.get()) {
        Thread.sleep(100);
      }
    } catch (Exception e) {
      LOG.error(ServerUtils.stringifyError(e));
      exception = new RuntimeException(e);
    }
    LOG.info("Opening spout " + componentId + ":"
        + taskDatas.keySet().toString());
    for (Map.Entry<Integer, TaskData> tt : taskDatas.entrySet()) {
      Integer taskId = tt.getKey();
      TaskData taskData = tt.getValue();
      ISpout spoutObj = (ISpout) taskData.getObject();

      // BuiltinMetrics.registerAll(taskData.getBuiltinMetrics(), stormConf,
      // taskData.getUserContext());
      // List<DisruptorQueue> queues = new ArrayList<DisruptorQueue>();
      // queues.add(executorData.getBatchTransferQueue());
      // queues.add(executorData.getReceiveQueue());
      // BuiltinMetrics.registerQueueMetrics(queues, stormConf,
      // taskData.getUserContext());

      spoutObj.open(stormConf, taskData.getUserContext(),
          new SpoutOutputCollector(new SpoutCollector(executorData, taskId,
              taskData, emittedCount, pending, overflowBuffer)));
    }
    this.executorData.getOpenOrPrepareWasCalled().reset(true);
    LOG.info("Opened spout " + componentId + ":"
        + taskDatas.keySet().toString());
    this.receiveQueue.consumerStarted();
  }

  @Override
  public void run() {
    try {
      while (!executorData.getTaskStatus().isShutdown()) {
        // This design requires that spouts be non-blocking
        receiveQueue.consumeBatch(this);

        // try to clear the overflow-buffer
        try {
          while (!overflowBuffer.isEmpty()) {
            TuplePair tuplePair = overflowBuffer.peek();
            executorTransferFn.transfer(tuplePair.getOutTask(),
                (TupleImpl) tuplePair.getOutTuple(), false, null);
            overflowBuffer.poll();
          }
        } catch (InsufficientCapacityException e) {
          // TODO do something here
          counters.tpsCounter(TaskCounter.SPOUT_TRANSFER_FAILED, componentId);
        }

        Long currCount = emittedCount.get();

        if (overflowBuffer.isEmpty()
            && (maxSpoutPending == null || pending.size() < maxSpoutPending)) {

          if (isActive.get()) {
            if (!lastActive.get()) {
              lastActive.set(true);
              LOG.info("Activating spout " + componentId + ":"
                  + taskDatas.keySet().toString());
              for (ISpout spout : spouts) {
                spout.activate();
              }
            }
            for (ISpout spout : spouts) {
              spout.nextTuple();
            }
          } else {
            if (lastActive.get()) {
              lastActive.set(false);
              LOG.info("Deactivating spout " + componentId + ":"
                  + taskDatas.keySet().toString());
              for (ISpout spout : spouts) {
                spout.deactivate();
              }
            }
            // TODO: log that it's getting throttled
            LOG.warn("Storm not active yet. Sleep 100 ms ...");
            try {
              Time.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }

        if (currCount.longValue() == emittedCount.get() && isActive.get()) {
          emptyEmitStreak.increment();
          spoutWaitStrategy.emptyEmit(emptyEmitStreak.get());
        } else {
          emptyEmitStreak.set(0);
        }
      }
    } catch (Throwable e) {
      exception = new Exception(e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-task-receiver")
  public void onEvent(Object tupleBatch, long sequenceId, boolean endOfBatch)
      throws Exception {
    if (tupleBatch == null) {
      return;
    }
    @SuppressWarnings("unchecked")
    ArrayList<TuplePair> tuples = (ArrayList<TuplePair>) tupleBatch;
    for (TuplePair tuple : tuples) {
      Integer taskId = tuple.getOutTask();
      Object msg = tuple.getOutTuple();

      TupleImpl outTuple = null;
      if (msg instanceof Tuple) {
        outTuple = (TupleImpl) msg;
      } else {
        try {
          outTuple = (TupleImpl) deserializer.deserialize((byte[]) msg);
        } catch (Throwable t) {
          LOG.error("Deserialize failed.{}", ServerUtils.stringifyError(t));
          return;
        }
      }

      if (isDebug) {
        LOG.info("Processing received message FOR " + taskId + " TUPLE: "
            + outTuple);
      }

      if (taskId != null) {
        tupleActionFn(taskId, outTuple);
      } else {
        // null task ids are broadcast tuples
        for (Integer t : executorData.getTaskIds()) {
          tupleActionFn(t, outTuple);
        }
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout#tuple-action-fn")
  private void tupleActionFn(Integer taskId, TupleImpl tuple) {
    String streamId = tuple.getSourceStreamId();
    if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      pending.rotate();
    } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
      ExecutorUtils.metricsTick(executorData, taskDatas.get(taskId), tuple);
    } else if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
      TaskData taskData = taskDatas.get(taskId);
      ISpout spoutObject = (ISpout) taskData.getObject();
      if (spoutObject instanceof ICredentialsListener) {
        ((ICredentialsListener) spoutObject)
            .setCredentials((Map<String, String>) tuple.getValue(0));
      }
    } else {
      Long id = tuple.getLong(0);
      Object obj = pending.remove(id);
      if (obj == null) {
        LOG.warn("Pending map no entry:" + id + ", pending size:"
            + pending.size());
        return;
      }
      TupleInfo tupleInfo = (TupleInfo) obj;
      Object spoutId = tupleInfo.getMessageId();
      Integer storedTaskId = tupleInfo.getTaskId();

      if (spoutId != null) {
        if (storedTaskId != taskId) {
          throw new RuntimeException("Fatal error, mismatched task ids: "
              + taskId + " " + storedTaskId);
        }

        Long startTimeMs = tupleInfo.getTimestamp();
        Long timeDelta = null;
        if (startTimeMs != null) {
          timeDelta = ServerUtils.time_delta_ms(startTimeMs);
        }
        if (streamId.equals(AckerBolt.ACKER_ACK_STREAM_ID)) {
          // ack-spout-msg [executor-data task-data msg-id tuple-info
          // time-delta]
          AckSpoutMsg asm =
              new AckSpoutMsg(executorData, taskDatas.get(taskId), spoutId,
                  tupleInfo, timeDelta, id);
          asm.run();

        } else if (streamId.equals(AckerBolt.ACKER_FAIL_STREAM_ID)) {
          FailSpoutMsg fsm =
              new FailSpoutMsg(executorData, taskDatas.get(taskId), spoutId,
                  tupleInfo, timeDelta, "FAIL-STREAM", id);
          fsm.run();
        } else {
          // TODO: on failure, emit tuple to failure stream
          LOG.warn("streamId : {} , not legal", streamId);
        }
      } else {
        LOG.warn("spoutId is null");
      }
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("SpoutExecutor Exception {}",
          ServerUtils.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public void shutdown() {
    executorData.getTaskStatus().setStatus(ExecutorStatus.SHUTDOWN);
  }

  @Override
  public Object getResult() {
    return executorData.getTaskStatus().isShutdown() ? -1 : 0;
  }
}
