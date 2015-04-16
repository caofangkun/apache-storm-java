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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.ExecutorData;
import org.apache.storm.daemon.worker.executor.ExecutorStatus;
import org.apache.storm.daemon.worker.executor.ExecutorUtils;
import org.apache.storm.daemon.worker.executor.error.ITaskReportErr;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.daemon.worker.executor.task.TaskUtils;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;
import org.apache.storm.daemon.worker.stats.BoltExecutorStats;
import org.apache.storm.daemon.worker.stats.CommonStats;
import org.apache.storm.daemon.worker.stats.Stats;
import org.apache.storm.util.EvenSampler;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;

/**
 * 
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */

@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt")
public class BoltExecutor extends RunnableCallback implements
    EventHandler<Object> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BoltExecutor.class);
  private ExecutorData executorData;
  private Map<Integer, TaskData> taskDatas;
  private String componentId;
  private ConcurrentLinkedQueue<TuplePair> overflowBuffer;
  private ITaskReportErr reportError;
  private DisruptorQueue receiveQueue;
  private KryoTupleDeserializer deserializer;
  private CommonStats executorStats;
  private EvenSampler executeSampler;
  private EvenSampler sampler;
  private Exception exception = null;
  private Counters counters;

  public BoltExecutor(ExecutorData executorData,
      Map<Integer, TaskData> taskDatas) throws Exception {
    this.executorData = executorData;
    this.taskDatas = taskDatas;
    @SuppressWarnings("rawtypes")
    Map stormConf = executorData.getStormConf();
    this.executorStats = executorData.getStats();
    this.reportError = executorData.getReportError();
    this.receiveQueue = executorData.getReceiveQueue();
    this.deserializer = executorData.getDeserializer();
    this.componentId = executorData.getComponentId();
    this.executeSampler = ConfigUtils.mkStatsSampler(stormConf);
    this.sampler = executorData.getSampler();
    // the overflow buffer is used to ensure that bolts do not block when
    // emitting this ensures that the bolt can always clear the incoming
    // messages, which prevents deadlock from occurs across the topology
    // (e.g. Spout -> BoltA -> Splitter -> BoltB -> BoltA, and all buffers
    // filled up)
    // the overflow buffer is might gradually fill degrading the performance
    // gradually eventually running out of memory, but at least prevent
    // live-locks/deadlocks.
    boolean boltOutgoingOverflowBufferEnable =
        ServerUtils.parseBoolean(stormConf
            .get(Config.TOPOLOGY_BOLTS_OUTGOING_OVERFLOW_BUFFER_ENABLE), false);
    if (boltOutgoingOverflowBufferEnable) {
      this.overflowBuffer = new ConcurrentLinkedQueue<TuplePair>();
    } else {
      this.overflowBuffer = null;
    }
    this.counters = executorData.getCounters();
    try {
      // If topology was started in inactive state, don't call prepare
      // bolt until it's activated first.
      Boolean isActive = executorData.getStormActiveAtom().get();
      while (!isActive) {
        LOG.warn("Storm not active yet. Sleep 100 ms ...");
        Thread.sleep(100);
      }
      LOG.info("Preparing bolt {}:{}", componentId, taskDatas.keySet()
          .toString());
      for (Map.Entry<Integer, TaskData> tt : taskDatas.entrySet()) {
        Integer taskId = tt.getKey();
        TaskData taskData = tt.getValue();
        IBolt boltObj = (IBolt) taskData.getObject();
        TopologyContext userContext = taskData.getUserContext();
        // TODO
        // BuiltinMetrics.registerAll(taskData.getBuiltinMetrics(), stormConf,
        // taskData.getUserContext());
        // if (boltObj instanceof ICredentialsListener) {
        // //TODO
        // Map<String,String> initialCredentials = null;
        // ((ICredentialsListener) boltObj).setCredentials(initialCredentials);
        // }

        // if (componentId.equals(Constants.SYSTEM_COMPONENT_ID)) {
        // List<DisruptorQueue> queues = new ArrayList<DisruptorQueue>();
        // queues.add(executorData.getBatchTransferQueue());
        // queues.add(executorData.getReceiveQueue());
        // queues.add(executorData.getWorker().getTransferQueue());
        // BuiltinMetrics.registerQueueMetrics(queues, stormConf,userContext);
        // BuiltinMetrics.registerIconnectionClientMetric(executorData.getWorker().getCachedNodeportToSocket(),
        // stormConf, userContext);
        // BuiltinMetrics.registerIconnectionServerMetric(executorData.getWorker().getReceiver(),
        // stormConf, userContext);
        // } else {
        // List<DisruptorQueue> queues = new ArrayList<DisruptorQueue>();
        // queues.add(executorData.getBatchTransferQueue());
        // queues.add(executorData.getReceiveQueue());
        // BuiltinMetrics.registerQueueMetrics(queues, stormConf,userContext);
        // }

        boltObj.prepare(
            stormConf,
            taskData.getUserContext(),
            new OutputCollector(new BoltOutputCollector(taskData, reportError,
                stormConf, executorData.getTransferFn(), executorData
                    .getWorkerContext(), taskId, null, executorData.getStats(),
                overflowBuffer)));
      }
    } catch (Exception e) {
      LOG.error("Bolt Executor init error {}", ServerUtils.stringifyError(e));
      exception = new RuntimeException(e);
    }

    this.executorData.getOpenOrPrepareWasCalled().reset(true);
    LOG.info("Prepared bolt {}:{}", componentId, taskDatas.keySet().toString());
    this.receiveQueue.consumerStarted();
  }

  @Override
  public void run() {
    try {
      while (!executorData.getTaskStatus().isShutdown()) {
        receiveQueue.consumeBatchWhenAvailable(this);

        // try to clear the overflow-buffer
        try {
          while (overflowBuffer != null && !overflowBuffer.isEmpty()) {
            TuplePair tuplePair = overflowBuffer.peek();
            executorData.getTransferFn().transfer(tuplePair.getOutTask(),
                (TupleImpl) tuplePair.getOutTuple(), false, null);
            overflowBuffer.poll();
          }
        } catch (InsufficientCapacityException e) {
          // TODO do something here
          counters.tpsCounter(TaskCounter.BOLT_TRANSFER_FAILED, componentId);
        }
      }

    } catch (Throwable e) {
      exception = new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object tupleBatch, long sequence, boolean endOfBatch)
      throws Exception {
    if (tupleBatch == null) {
      return;
    }
    ArrayList<TuplePair> tuples = (ArrayList<TuplePair>) tupleBatch;
    for (TuplePair tuple : tuples) {
      Integer outTask = tuple.getOutTask();
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
      if (outTask != null) {
        tupleActionFn(outTask, outTuple);
      } else {
        // null task ids are broadcast tuples
        for (Integer t : executorData.getTaskIds()) {
          tupleActionFn(t, outTuple);
        }
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#tuple-action-fn")
  private void tupleActionFn(Integer taskId, TupleImpl tuple) {
    // synchronization needs to be done with a key provided by this bolt,
    // otherwise:
    // spout 1 sends synchronization (s1), dies, same spout restarts somewhere
    // else, sends synchronization (s2) and incremental update. s2 and update
    // finish before s1 -> lose the incremental update
    // TODO: for state sync, need to first send sync messages in a loop and
    // receive tuples until synchronization buffer other tuples until fully
    // synchronized, then process all of those tuples then go into normal loop
    // spill to disk?
    // could be receiving incremental updates while waiting for sync or even a
    // partial sync because of another failed task
    // should remember sync requests and include a random sync id in the
    // request. drop anything not related to active sync requests or just
    // timeout the sync messages that are coming in until full sync is hit from
    // that task need to drop incremental updates from tasks where waiting for
    // sync. otherwise, buffer the incremental updates
    // TODO: for state sync, need to check if tuple comes from state spout. if
    // so, update state
    // TODO: how to handle incremental updates as well as synchronizations at
    // same time
    // TODO: need to version tuples somehow

    // LOG.debug("Received tuple {} at task {}", tuple.toString(), taskId);
    // need to do it this way to avoid reflection
    String streamId = tuple.getSourceStreamId();
    if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
      TaskData taskData = taskDatas.get(taskId);
      IBolt boltObj = (IBolt) taskData.getObject();
      if (boltObj instanceof ICredentialsListener) {
        ((ICredentialsListener) boltObj)
            .setCredentials((Map<String, String>) tuple.getValue(0));
      }
    } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
      ExecutorUtils.metricsTick(executorData, taskDatas.get(taskId), tuple);
    } else {
      Long now = null;
      boolean isSampler = sampler.countCheck();
      boolean isExecuteSampler = executeSampler.countCheck();
      if (isSampler || isExecuteSampler) {
        now = System.currentTimeMillis();
      }

      if (isSampler) {
        tuple.setProcessSampleStartTime(now);
      }

      if (isExecuteSampler) {
        tuple.setExecuteSampleStartTime(now);
      }

      TaskData taskData = taskDatas.get(taskId);
      IBolt boltObj = (IBolt) taskData.getObject();
      try {
        boltObj.execute(tuple);
      } catch (Exception e) {
        LOG.error("Bolt occur an exception:", e);
      }

      Long delta = BoltUtils.tupleExecuteTimeDelta(tuple);
      TaskUtils.applyHooks(taskData.getUserContext(), new BoltExecuteInfo(
          tuple, taskId, delta));

      if (delta != null) {
        Stats.boltExecuteTuple((BoltExecutorStats) executorStats,
            tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
      }
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("BoltExecutor Exception {}",
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
