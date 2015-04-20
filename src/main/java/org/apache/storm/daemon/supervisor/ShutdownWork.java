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
package org.apache.storm.daemon.supervisor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.supervisor.psim.ProcessSimulator;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeat;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;

@ClojureClass(className = "backtype.storm.daemon.supervisor#shutdown-worker")
public class ShutdownWork extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(ShutdownWork.class);

  @SuppressWarnings("unchecked")
  public void shutdownWorker(SupervisorData supervisor, String workerId)
      throws IOException {
    LOG.info("Shutting down supervisorId:{}, workerId:{}",
        supervisor.getSupervisorId(), workerId);

    String workerPidPath =
        ConfigUtil.workerPidsRoot(supervisor.getConf(), workerId);

    Set<String> pids = CoreUtil.readDirContents(workerPidPath);
    String threadPid = supervisor.getWorkerThreadPids().get(workerId);
    LocalState ls = ConfigUtil.workerState(supervisor.getConf(), workerId);
    WorkerLocalHeartbeat workerLocalHeartBeat =
        (WorkerLocalHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);

    try {

      if (workerLocalHeartBeat != null
          && workerLocalHeartBeat.getProcessId() != null) {
        CoreUtil.killProcessWithSigTerm(Integer.parseInt(workerLocalHeartBeat
            .getProcessId()));
        CoreUtil.sleepSecs(3);
      }

      if (threadPid != null) {
        ProcessSimulator.killProcess(threadPid);
      }

      for (String pid : pids) {
        CoreUtil.killProcessWithSigTerm(Integer.parseInt(pid));
      }

      if (!pids.isEmpty()) {
        int sleepSecs =
            CoreUtil.parseInt(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS, 1);
        LOG.info("Sleep " + sleepSecs
            + " seconds for execution of cleanup threads on worker.");
        CoreUtil.sleepSecs(sleepSecs);
      }

      for (String pid : pids) {
        CoreUtil.forceKillProcess(Integer.parseInt(pid));
        CoreUtil.rmpath(ConfigUtil.workerPidPath(supervisor.getConf(),
            workerId, pid));

      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (workerLocalHeartBeat != null
        && workerLocalHeartBeat.getProcessId() != null) {
      CoreUtil.forceKillProcess(Integer.parseInt(workerLocalHeartBeat
          .getProcessId()));
    }

    tryCleanupWorkerDir(supervisor.getConf(), workerId);
    LOG.info("Shut down supervisorId:{}, workerId:{}",
        supervisor.getSupervisorId(), workerId);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.supervisor#try-cleanup-worker")
  public void tryCleanupWorkerDir(Map conf, String workerId) {
    try {
      CoreUtil.rmr(ConfigUtil.workerHeartbeatsRoot(conf, workerId));
      // this avoids a race condition with worker or subprocess writing pid
      // around same time
      CoreUtil.rmr(ConfigUtil.workerPidsRoot(conf, workerId));
      CoreUtil.rmr(ConfigUtil.workerRoot(conf, workerId));
    } catch (FileNotFoundException e) {
      LOG.warn("File not found for: {}", CoreUtil.stringifyError(e));
    } catch (Exception e) {
      LOG.warn(e + "Failed to cleanup worker " + workerId
          + ". Will retry later");
    }
  }
}
