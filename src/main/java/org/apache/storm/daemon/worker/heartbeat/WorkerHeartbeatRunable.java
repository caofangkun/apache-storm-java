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
package org.apache.storm.daemon.worker.heartbeat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.ExecutorShutdown;
import org.apache.storm.daemon.worker.executor.UptimeComputer;
import org.apache.storm.daemon.worker.stats.StatsData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.utils.MutableLong;

public class WorkerHeartbeatRunable extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(WorkerHeartbeatRunable.class);
  private StormClusterState stormClusterState;
  private AtomicBoolean active;
  private String stormId;
  private UptimeComputer uptime;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private int frequence;
  private Exception exception = null;
  private String node;
  private int port;
  private WorkerData workerData;
  private MutableLong retryCount;

  public WorkerHeartbeatRunable(WorkerData workerData) {
    this.workerData = workerData;
    this.stormClusterState = workerData.getStormClusterState();
    this.stormId = workerData.getTopologyId();
    this.node = workerData.getAssignmentId();
    this.port = workerData.getPort();
    this.stormConf = workerData.getStormConf();
    this.frequence =
        CoreUtil.parseInt(stormConf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS),
            3);
    this.uptime = workerData.getUptime();
    this.active = workerData.getStormActiveAtom();
    this.retryCount = new MutableLong(0);
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#do-executor-heartbeats")
  private void doExecutorHeartbeats() {
    List<ExecutorShutdown> executors = workerData.getShutdownExecutors();
    // stats is how we know what executors are assigned to this worker
    Map<ExecutorInfo, StatsData> stats = new HashMap<ExecutorInfo, StatsData>();
    if (executors == null) {
      LOG.warn("ExecutorShutdowns is null");
      return;
    }
    for (ExecutorShutdown executor : executors) {
      stats.put(executor.get_executor_id(), executor.render_stats());
    }
    Integer currtime = CoreUtil.current_time_secs();
    WorkerHeartbeats zkHb =
        new WorkerHeartbeats(stormId, stats, uptime.uptime(), currtime);
    try {
      // do the zookeeper heartbeat
      stormClusterState.workerHeartbeat(stormId, node, port, zkHb);
      retryCount = new MutableLong(0);
    } catch (Exception e) {
      LOG.error("Failed to update heartbeat to ZK ", e);
      retryCount.increment();
      if (retryCount.get() >= 3) {
        exception = new RuntimeException(e);
      }
      return;
    }
    LOG.debug("WorkerHeartbeat:" + zkHb.toString());
  }

  @Override
  public void run() {
    doExecutorHeartbeats();
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("Worker Local Heartbeat exception: {}",
          CoreUtil.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public Object getResult() {
    if (this.active.get()) {
      return frequence;
    }
    return -1;
  }
}
