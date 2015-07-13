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

import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.counter.Counters;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.daemon.worker.stats.Stats;
import org.apache.storm.daemon.worker.stats.StatsData;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-executor#shutdown")
public class ExecutorShutdown implements ShutdownableDameon, RunningExecutor {
  private static final Logger LOG = LoggerFactory
      .getLogger(ExecutorShutdown.class);
  private ExecutorInfo executorInfo;
  public static final byte QUIT_MSG = (byte) 0xff;
  private Map<Integer, TaskData> taskDatas;
  private AsyncLoopThread[] all_threads;
  private StormClusterState stormClusterState;
  private AsyncLoopThread heartbeat_thread;
  private ExecutorData executorData;

  public ExecutorShutdown(ExecutorData executorData, String node, int port,
      AsyncLoopThread[] all_threads) {
    this.executorData = executorData;
    this.taskDatas = executorData.getTaskDatas();
    this.executorInfo = executorData.getExecutorInfo();
    this.all_threads = all_threads;
    this.stormClusterState = executorData.getStormClusterState();
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down executor {}:{}", executorData.getComponentId(),
        executorData.getExecutorInfo().toString());
    executorData.getTaskStatus().setStatus(ExecutorStatus.SHUTDOWN);

    LOG.info("Close receive queue.");
    executorData.getReceiveQueue().haltWithInterrupt();
    LOG.info("Close batch transfer queue.");
    executorData.getBatchTransferQueue().haltWithInterrupt();

    LOG.info("Close executor scheduler.");
    executorData.getExecutorScheduler().shutdown();
    // all thread will check the taskStatus
    // once it has been set SHUTDOWN, it will quit
    // waiting 100ms for executor thread shutting it's own
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }

    LOG.info("Close executor threads.");
    for (AsyncLoopThread thr : all_threads) {
      try {
        thr.interrupt();
        thr.join();
      } catch (InterruptedException e) {
        LOG.error("AsyncLoopThread shutdown error:", e.getMessage());
      }
    }

    // userContext hooks cleanup
    LOG.info("Cleanup hooks.");
    for (TaskData taskData : taskDatas.values()) {
      TopologyContext userContext = taskData.getUserContext();
      for (ITaskHook hook : userContext.getHooks()) {
        hook.cleanup();
      }
    }

    try {
      LOG.info("Close zookeeper connection.");
      stormClusterState.disconnect();
    } catch (Exception e) {
      LOG.info("Disconnect zkCluster error ", e);
    }

    LOG.info("Close component.");
    if ((Boolean) executorData.getOpenOrPrepareWasCalled().deref()) {
      for (TaskData t : taskDatas.values()) {
        Object obj = t.getObject();
        closeComponent(obj);
      }
    }
    LOG.info("Shut down executor componentId:" + executorData.getComponentId()
        + ", executorType: " + executorData.getExecutorType().toString()
        + ", executorId:" + executorData.getExecutorInfo().toString());
  }

  public void join() throws InterruptedException {
    for (AsyncLoopThread t : all_threads) {
      t.join();
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#close-component")
  private void closeComponent(Object _task_obj) {
    if (_task_obj instanceof IBolt) {
      ((IBolt) _task_obj).cleanup();
    }
    if (_task_obj instanceof ISpout) {
      ((ISpout) _task_obj).close();
    }
  }

  @Override
  public boolean waiting() {
    return heartbeat_thread.isSleeping();
  }

  @Override
  public void run() {
    shutdown();
  }

  @Override
  public ExecutorInfo get_executor_id() {
    return executorInfo;
  }

  public Counters render_counters() {
    return executorData.getCounters();
  }

  @Override
  public StatsData render_stats() {
    return Stats.renderStats(executorData.getStats());
  }
}
