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

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor")
public class Executor implements Serializable {
  private static final long serialVersionUID = 1L;
  private final static Logger LOG = LoggerFactory.getLogger(Executor.class);
  private WorkerData workerData;
  private ExecutorData executorData;
  private Map<Integer, TaskData> taskDatas;
  @SuppressWarnings("rawtypes")
  private Map stormConf;

  public Executor(WorkerData workerData, ExecutorInfo executorId)
      throws Exception {
    this.workerData = workerData;
    this.executorData = new ExecutorData(workerData, executorId);
    this.taskDatas = executorData.getTaskDatas();
    this.stormConf = executorData.getStormConf();
    LOG.info("Loading task " + executorData.getComponentId() + ":"
        + executorData.getTaskId());
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads")
  public RunnableCallback mkThreads(ExecutorData executorData,
      Map<Integer, TaskData> taskDatas) throws Exception {
    ExecutorType executorType = executorData.getExecutorType();
    if (executorType.equals(ExecutorType.bolt)) {
      return new BoltExecutor(executorData, taskDatas);
    } else if (executorType.equals(ExecutorType.spout)) {
      return new SpoutExecutor(executorData, taskDatas);
    }
    return null;
  }

  public ExecutorShutdown execute() throws Exception {
    // starting the batch-transfer->worker ensures that anything publishing to
    // that queue doesn't block (because it's a single threaded queue and the
    // caching/consumer started trick isn't thread-safe)
    BatchTransferToWorkerHandler batchTransferToWorker =
        new BatchTransferToWorkerHandler(workerData, executorData);
    AsyncLoopThread systemThreads =
        new AsyncLoopThread(batchTransferToWorker, false, new DefaultKillFn(1,
            "Async loop died!"), Thread.MAX_PRIORITY, true);

    RunnableCallback baseExecutor = mkThreads(executorData, taskDatas);
    AsyncLoopThread executor_threads =
        new AsyncLoopThread(baseExecutor, false,
            executorData.getReportErrorAndDie(), Thread.MAX_PRIORITY, true,
            executorData.getComponentId());

    AsyncLoopThread[] all_threads = { executor_threads, systemThreads };

    setupTicks(workerData, executorData);

    LOG.info("Finished loading executor componentId:{}, executorId:{}",
        executorData.getComponentId(), executorData.getExecutorInfo()
            .toString());
    return new ExecutorShutdown(executorData, workerData.getAssignmentId(),
        workerData.getPort(), all_threads);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#setup-ticks!")
  private void setupTicks(WorkerData workerData, ExecutorData executorData) {
    Integer tickTimeSecs =
        ServerUtils.parseInt(
            stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS), null);
    if (tickTimeSecs != null) {
      boolean isSystemId = Utils.isSystemId(executorData.getComponentId());
      boolean isEnableMessageTimeOuts =
          ServerUtils.parseBoolean(
              stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS), true);
      boolean isSpout =
          executorData.getExecutorType().equals(ExecutorType.spout);
      if (isSystemId || (!isEnableMessageTimeOuts && isSpout)) {
        LOG.info("Timeouts disabled for executor {} : {}",
            executorData.getComponentId(), executorData.getExecutorInfo());
      } else {
        final ScheduledExecutorService scheduExec =
            executorData.getExecutorScheduler();
        RunnableCallback ticksRunnable =
            new TicksRunable(executorData, tickTimeSecs);
        scheduExec.scheduleAtFixedRate(ticksRunnable, 0, tickTimeSecs,
            TimeUnit.SECONDS);
      }
    }
  }

  public static ExecutorShutdown mkExecutorShutdownDameon(
      WorkerData workerData, ExecutorInfo executorId) throws Exception {
    Executor executor = new Executor(workerData, executorId);
    return executor.execute();
  }
}
